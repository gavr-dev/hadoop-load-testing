#!/usr/bin/env python3

import argparse
import time
import datetime
import os
from threading import Thread

HELP = '''
Examples SELECT:
python3 load-spark-testing.py --nameservice ns \
--parallel_threads 3 \
--request_count 3 \
--path /user/hdfs/file.parquet  \
--select

Examples AGGREGATE:
python3 load-spark-testing.py --nameservice ns \
--parallel_threads 3 \
--request_count 3 \
--path /user/hdfs/file.parquet  \
--aggregate
'''
RESULT_FILE_PATH = './result_spark_testing_{}.txt'.format(datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))

TMP_COMMAND_FILE = './spark_commands.txt'

def get_datetime():
    return datetime.datetime.today().strftime("%Y-%m-%d_%H:%M:%S.%f")[:-3]

class SparkException(Exception):
    pass


def remove_tmp_spark_file():
    tmp_command_file = os.path.normpath(TMP_COMMAND_FILE)
    if os.path.exists(tmp_command_file):
        os.remove(tmp_command_file)


class SparkTester:

    def __init__(self, test_args):
        self.args = test_args
        self.time_dict = dict()

    def create_tmp_spark_file(self):
        tmp_command_file = os.path.normpath(TMP_COMMAND_FILE)
        if os.path.exists(tmp_command_file):
            remove_tmp_spark_file()

        spark_file_writer = open(tmp_command_file, 'w')
        spark_file_writer.write('import org.apache.spark.sql.hive.HiveContext\n')
        spark_file_writer.write('val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)\n')
        if self.args.aggregate:
            spark_file_writer.write('sqlContext.read.parquet("hdfs://{}/{}").agg(sum("count")).show(100)\n'.format(self.args.nameservice, self.args.path))
        else:
            spark_file_writer.write('sqlContext.read.parquet("hdfs://{}/{}").show(10)\n'.format(self.args.nameservice, self.args.path))

    def queries_in_thread(self, thread_name):
        try:
            print('{} - {} start work'.format(get_datetime(), thread_name))
            for i in range(self.args.request_count):
                start_time = time.time()
                print('{} - {} query'.format(get_datetime(), (i + 1)))
                result = os.system(
                    'spark-shell < {} --master yarn --conf spark.kerberos.access.hadoopFileSystems=hdfs://{}/'.format(TMP_COMMAND_FILE, self.args.nameservice))
                print('{} - Exit code is: '.format(get_datetime(), result))
                if result != 0:
                    print("Failed spark task")
                    raise SparkException('Failed SPARK')
                delta = time.time() - start_time
                self.time_dict['{}_{}'.format(thread_name, i + 1)] = delta

            print(self.time_dict)
            print('{} - Tread #{} finish work'.format(get_datetime(), thread_name))
        except SparkException as err:
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
    parser.add_argument('--nameservice', default='ns', help='HDFS nameservice')
    parser.add_argument('--parallel_threads', default=3, type=int, help='Count threads')
    parser.add_argument('--request_count', default=3, type=int, help='Count request')
    parser.add_argument('--path', help='Parquet path')
    parser.add_argument('--select', default=False, action='store_true', help='Preserve intermediate files and logs')
    parser.add_argument('--aggregate', default=False, action='store_true', help='Preserve intermediate files and logs')
    parser.add_argument('--debug', default=False, action='store_true', help='Preserve intermediate files and logs')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    tester = SparkTester(args)
    tester.create_tmp_spark_file()
    res_dic = tester.start_threads()

    result_file = os.path.normpath(RESULT_FILE_PATH)
    writer = open(result_file, 'w')

    sum_times = 0

    for thread_key in res_dic:
        thread_time = res_dic[thread_key]
        sum_times = sum_times + thread_time
        if thread_time < 0:
            writer.write('{} - {}: {} s \n'.format(get_datetime(), thread_key, 'FAILED!!!!'))
        else:
            writer.write('{} - {}: {} s \n'.format(get_datetime(), thread_key, thread_time))

    writer.write('{} - AVG TIME IS: {}'.format(get_datetime(), (sum_times / len(res_dic))))
    writer.close()
    remove_tmp_spark_file()
