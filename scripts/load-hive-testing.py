#!/usr/bin/env python3

import argparse
import time
import datetime
import os

from threading import Thread
from subprocess import PIPE, Popen

HELP = '''
Examples SELECT:
    python3 load-hive-testing.py --hive_server hive-server \
    --parallel_threads 3 \
    --request_count 3 \
    --database default \
    --table table \
    --select  
    
Examples AGGREGATE:
python3 load-hive-testing.py --hive_server hive-server \
        --parallel_threads 3 \
        --request_count 3 \
        --database default \
        --table table \
        --aggregate   

'''
RESULT_FILE_PATH = './result_hive_testing_{}.txt'.format(datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))

def get_datetime():
    return datetime.datetime.today().strftime("%Y-%m-%d_%H:%M:%S.%f")[:-3]


class HiveException(Exception):
    pass


class HiveTester:

    def __init__(self, test_args):
        self.args = test_args
        self.time_dict = dict()

    def queries_in_thread(self, thread_name):
        try:
            print('{} - {} start work'.format(get_datetime(), thread_name))

            for i in range(self.args.request_count):
                start_time = time.time()
                print('{} - {} query'.format(get_datetime(), (i + 1)))

                if self.args.aggregate:
                    script = 'USE {}; SELECT AVG(count) FROM {}'.format(self.args.database, self.args.table)
                else:
                    script = 'USE {}; SELECT * FROM {} LIMIT 100'.format(self.args.database, self.args.table)

                print('{} - [{}] Hive_sql is: {}'.format(get_datetime(), thread_name, script))
                hive_line = 'jdbc:hive2://{}:10000/;principal=hive/_HOST@LC.CLUSTER'.format(self.args.hive_server)
                select = Popen(["beeline", "-u", hive_line, "-e", script], stdin=PIPE, bufsize=-1)
                select.communicate()
                if select.returncode != 0:
                    print('{} - [{}] Error query {}'.format(get_datetime(), thread_name, script))
                    raise HiveException('Failed HIVE')
                delta = time.time() - start_time
                print('{} - [{}] Finish query'.format(get_datetime(), thread_name))
                self.time_dict['{}_{}'.format(thread_name, i + 1)] = delta

            print(self.time_dict)
            print('{} - Tread #{} finish work'.format(get_datetime(), thread_name))
        except HiveException as err:
            print(repr(err))
            self.time_dict[thread_name] = -1

    def start_threads(self):
        print('{} - Start {} threads'.format(get_datetime(), self.args.parallel_threads))

        threads = [Thread(target=self.queries_in_thread, args=('Thread #{}'.format(j + 1),)) for j in
                   range(args.parallel_threads)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        return self.time_dict


def get_args():
    parser = argparse.ArgumentParser(description='', epilog=HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--hive_server', help='Hive url')
    parser.add_argument('--parallel_threads', default=3, type=int, help='Count threads')
    parser.add_argument('--request_count', default=3, type=int, help='Count request')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--table', help='Table name')
    parser.add_argument('--select', default=False, action='store_true', help='Preserve intermediate files and logs')
    parser.add_argument('--aggregate', default=False, action='store_true', help='Preserve intermediate files and logs')
    parser.add_argument('--debug', default=False, action='store_true', help='Preserve intermediate files and logs')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    tester = HiveTester(args)
    res_dic = tester.start_threads()

    result_file = os.path.normpath(RESULT_FILE_PATH)
    writer = open(result_file, 'w')

    sum_times = 0

    for thread_key in res_dic:
        thread_time = res_dic[thread_key]
        sum_times = sum_times + thread_time
        if thread_time < 0:
            writer.write('{}: {} s \n'.format(thread_key, 'FAILED!!!!'))
        else:
            writer.write('{}: {} s \n'.format(thread_key, thread_time))

    writer.write('AVG TIME IS: {}'.format(sum_times / len(res_dic)))
    writer.close()
