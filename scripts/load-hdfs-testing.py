#!/usr/bin/env python3

import argparse
import time
import datetime
import os
from threading import Thread
import shutil

HELP = '''
Examples SELECT:
python3 load-hdfs-testing.py \
--path hdfs://ns/user/hdfs/file \
--parallel_threads 3 \
--request_count 3 \
--download_folder /opt/nt/tmp
'''
RESULT_FILE_PATH = './result_hdfs_testing_{}.txt'.format(datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))


def get_datetime():
    return datetime.datetime.today().strftime("%Y-%m-%d_%H:%M:%S.%f")[:-3]


class HdfsException(Exception):
    pass


class HdfsTester:

    def __init__(self, test_args, is_gw):
        self.args = test_args
        self.time_dict = dict()
        self.is_gw = is_gw

    def getting_in_thread(self, thread_name):
        try:
            print('{} - {} start work'.format(get_datetime(), thread_name))
            self.time_dict[thread_name] = 0
            for i in range(self.args.request_count):
                print('{} - {} query'.format(get_datetime(), i + 1))
                local_folder = '{}/{}_{}'.format(self.args.download_folder, thread_name, i)
                local_folder_path = os.path.normpath(local_folder)

                if os.path.exists(local_folder_path):
                    shutil.rmtree(local_folder_path)

                os.mkdir(local_folder_path)

                start_time = time.time()
                print('{} - Start getting files'.format(get_datetime()))
                result = os.system('hadoop fs -copyToLocal {} {}'.format(args.path, local_folder))
                print('{} - Exit code is: {}'.format(get_datetime(), result))

                if result != 0:
                    print("{} - Failed hdfs task".format(get_datetime()))
                    raise HdfsException('Failed HDFS')
                end_time = time.time()
                print('{} - Tread {}, requet {}: {}'.format(get_datetime(), thread_name, i, end_time - start_time))

                if os.path.exists(local_folder_path):
                    shutil.rmtree(local_folder_path)

                self.time_dict[thread_name] = self.time_dict[thread_name] + (end_time - start_time)
            print('{} - Tread #{} finish work'.format(get_datetime(), thread_name))
        except HdfsException as err:
            print(repr(err))
            self.time_dict[thread_name] = -1

    def start_threads(self):
        print('{} - Start {} threads'.format(get_datetime(), self.args.parallel_threads))

        threads = [Thread(target=self.getting_in_thread, args=('Thread_{}'.format(j + 1),)) for j in
                   range(args.parallel_threads)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        print(self.time_dict)
        return self.time_dict


def get_args():
    parser = argparse.ArgumentParser(description='', epilog=HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--path', help='HDFS path')
    parser.add_argument('--parallel_threads', default=3, type=int, help='Count threads')
    parser.add_argument('--request_count', default=3, type=int, help='Count request')
    parser.add_argument('--download_folder', help='local download folder')
    parser.add_argument('--debug', default=False, action='store_true', help='Preserve intermediate files and logs')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    print('{} - ==SC ({})=='.format(get_datetime(), args.path))
    tester = HdfsTester(args, False)
    res_dic = tester.start_threads()

    result_file = os.path.normpath(RESULT_FILE_PATH)
    writer = open(result_file, 'w')

    writer.write('{} - ==SC ({})==\n'.format(get_datetime(), args.path))
    sum_times = 0
    max_time = 0
    for thread_key in res_dic:
        thread_time = res_dic[thread_key]
        sum_times = sum_times + thread_time
        if thread_time < 0:
            writer.write('{}: {} s \n'.format(thread_key, 'FAILED!!!!'))
        else:
            if thread_time > max_time:
                max_time = thread_time
            writer.write('{}: {} s \n'.format(thread_key, thread_time))

    awg_dda = sum_times / len(res_dic)
    print('{} - Result: AVG {} sec, MAX {} sec'.format(get_datetime(), awg_dda, max_time))
    writer.write('{} - Result: AVG {} sec, MAX {} sec \n'.format(get_datetime(), awg_dda, max_time))
    writer.close()
