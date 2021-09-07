#!/usr/bin/env python3

import pyarrow.csv as pyarrow_csv
import pyarrow.parquet as pyarrow_parquet

from subprocess import PIPE, Popen

import argparse
import os
import sys
import time
import string
import random
import datetime


FIRST_LINE_CSV = 'one_str,two_str,three_str,four_int,five_int,six_bool,seven_bool,eight_date,nine_date,ten_double,eleven_double,twelve_double\n'

LINES_IN_MB = 16000
BLOCK_SIZE = 128
PATH_TMP_FILE_DATA_FOR_PARQUET = '/tmp/file-generator.csv'

HELP = '''
Examples:

For generation files use:
python files-generator.py --hdfs_upload_folder '/user/hdfs' \\
--local_tmp_folder /tmp \\
--folder_name small_files \\
--database default \\
--sub_folder_count 1 \\
--files_count 1 \\
--file_size 2 \\
--nameservice ns \\
--rm_old

For generation parquet files add --parquet:
python files-generator.py --hdfs_upload_folder '/user/hdfs' \\
--hive_server "hive-server.lc.cluster" \\
--local_tmp_folder ./ \\
--folder_name small_files \\
--database default \\
--sub_folder_count 1 \\
--files_count 5 \\
--file_size 10 \\
--nameservice ns \\
--parquet \\
--rm_old
'''.strip()


class HadoopException(Exception):
    pass


class CheckFolderHadoopException(Exception):
    pass


def get_args():
    parser = argparse.ArgumentParser(description='', epilog=HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--hdfs_upload_folder', default='/user/hdfs', help='HDFS folder for upload file')
    parser.add_argument('--nameservice', default='ns', help='HDFS nameservice')
    parser.add_argument('--hive_server', default='hive-server.lc.cluster', help='Hive server')
    parser.add_argument('--database', default='default', help='Hive database')
    parser.add_argument('--local_tmp_folder', default='/tmp', help='Local work folder')
    parser.add_argument('--folder_name', default='files_nt', help='Local folder name for new files')
    parser.add_argument('--sub_folder_count', default=1, type=int, help='Count folders for files')
    parser.add_argument('--files_count', default=1, type=int, help='Count files in folder')
    parser.add_argument('--file_size_mb', default=120, type=int, help='Files size in mb')
    parser.add_argument('--parquet', default=False, action='store_true', help='Flag parquet file generation')
    parser.add_argument('--debug', default=False, action='store_true', help='Preserve intermediate files and logs')
    parser.add_argument('--rm_old', default=False, action='store_true', help='Delete old files and hive tables')
    return parser.parse_args()


def create_csv(csv_input, count):
    start_time_creation_csv = time.time()
    file = open(csv_input, "w")
    file.write(FIRST_LINE_CSV)
    thousand = 0
    for i in range(count):
        data = get_random_data()
        file.write(data)
        thousand = thousand - 1
        if thousand <= 0:
            print('{}/{}'.format(i, count))
            thousand = 1000000
    file.close()
    print('Creation csv file time is {} sec'.format(time.time() - start_time_creation_csv))


def get_random_data():
    # return DATA_CSV
    return '{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(
        get_random_string(20),
        "men",
        get_random_string(10),
        random.randint(0, 10000000),
        12345,
        random.randint(0, 1) == 0,
        random.randint(0, 1) == 0,
        datetime.datetime.fromtimestamp(time.time()),
        datetime.datetime.fromtimestamp(time.time()),
        random.randint(0, 10000000),
        random.uniform(1.0, 6.0),
        1.3
    )


def get_random_string(length):
    letters = string.ascii_letters
    str_result = ''
    for i in range(length):
        str_result = str_result + random.choice(letters)
    return str_result


def write_csv_to_parquet(csv_input, parquet_output):
    table = pyarrow_csv.read_csv(csv_input)
    pyarrow_parquet.write_table(table, parquet_output, BLOCK_SIZE * LINES_IN_MB)


def get_usual_file_name(parent_folder, i):
    base_name_folder = os.path.basename(parent_folder)
    return 'file-{}-{}'.format(base_name_folder, i)


def remove_csv_file():
    if os.path.exists(PATH_TMP_FILE_DATA_FOR_PARQUET):
        print('Delete old tmp csv file')
        os.remove(PATH_TMP_FILE_DATA_FOR_PARQUET)


def create_files(file_size_mb, local_folders_list, files_count, is_parquet):
    if args.parquet:
        remove_csv_file()
        print('Creation CSV file')
        create_csv(PATH_TMP_FILE_DATA_FOR_PARQUET, args.file_size_mb * LINES_IN_MB)

    whole_count_in_dir_files = files_count // len(local_folders_list)
    remainder_count_in_dir_files = files_count % len(local_folders_list)
    if whole_count_in_dir_files == 0:
        whole_count_in_dir_files = 1
        remainder_count_in_dir_files = 0

    for folder in local_folders_list:
        for i in range(whole_count_in_dir_files + remainder_count_in_dir_files):
            if files_count == 0:
                break
            start_time_creation = time.time()
            if is_parquet:
                file_path = create_parquet_file(folder, i)
            else:
                file_path = create_usual_file(file_size_mb, folder, i)

            files_count = files_count - 1
            print('Time of file creation {}: {} sec'.format(file_path, time.time() - start_time_creation))

        remainder_count_in_dir_files = 0

    if args.parquet:
        remove_csv_file()


def create_usual_file(file_size_mb, folder, i):
    file_name = get_usual_file_name(folder, i)
    file_path = os.path.join(folder, file_name)
    if os.path.exists(file_path):
        sys.stderr.write('Output location "{}" already exists. Rename or delete before running again.\n'
                         .format(args.parquet_output))
        sys.exit(1)
    file_for_write = open(file_path, 'w')
    str = '.'
    for remainder_count_in_dir_files in range(1024 * 1024):
        str = str + '.'
    for j in range(file_size_mb):
        file_for_write.write(str)
    file_for_write.close()
    return file_path


def create_parquet_file(folder, i):
    file_name = get_usual_file_name(folder, i) + '.parquet'
    file_path = os.path.join(folder, file_name)
    if os.path.exists(file_path):
        sys.stderr.write(
            'Output location "{}" already exists. Rename or delete before running again.\n'.format(
                args.parquet_output))
        sys.exit(1)
    try:
        write_csv_to_parquet(PATH_TMP_FILE_DATA_FOR_PARQUET, file_path)
        return file_path
    except Exception as ex:
        sys.stderr.write('''Fatal writing CSV to Parquet:\n{}'''.format(ex))
        sys.exit(2)


def upload_folder_to_hadoop(local_folder, hdfs_folder):
    put = Popen(["hadoop", "fs", "-put", local_folder, hdfs_folder], stdin=PIPE,
                bufsize=-1)
    put.communicate()
    if put.returncode != 0:
        raise HadoopException('Failed upload files to hdfs')

    du = Popen(["hadoop", "fs", "-du", "-s", "-h", hdfs_folder], stdin=PIPE, bufsize=-1)
    du.communicate()
    if du.returncode != 0:
        raise CheckFolderHadoopException('Failed getting folder size')


def delete_old_hadoop_folder(hdfs_folder, work_folder_name):
    delete = Popen(["hadoop", "fs", "-rmr", os.path.join(hdfs_folder, work_folder_name + '*')], stdin=PIPE,
                   bufsize=-1)
    delete.communicate()
    if delete.returncode != 0:
        print('Failed delete old files in hdfs. Is it exists?')


def get_tables_names_like(hive_server_url, name_for_pattern, database):
    show_tables_like_script = "\"USE {}; SHOW TABLES '*{}*'\"".format(database, name_for_pattern)
    print('Hive_sql: ' + show_tables_like_script)

    show = Popen(["beeline", "-u", hive_server_url, "-e", show_tables_like_script], stdout=PIPE, stdin=PIPE, bufsize=-1)
    out, err = show.communicate()
    out_lines = str(out).split(' ')
    return filter(lambda x: x.startswith(name_for_pattern), out_lines)


def delete_old_hive_tables_like(hive_server_url, name_for_pattern, database):
    old_tables_list = get_tables_names_like(hive_server_url, name_for_pattern, database)
    for table in old_tables_list:
        delete_old_hive_table(hive_server_url, table, database)


def delete_old_hive_table(hive_server_url, table_name, database):
    drop_hive_script = "\"USE {}; DROP TABLE IF EXISTS {}\"".format(database, table_name)
    print('Hive_sql: ' + drop_hive_script)

    drop = Popen(["beeline", "-u", hive_server_url, "-e", drop_hive_script], stdin=PIPE,
                 bufsize=-1)
    drop.communicate()
    if drop.returncode != 0:
        print('Error drop table {}'.format(table_name))


def get_name_prom_path(folder):
    split = os.path.split(folder)
    return split[len(split) - 1]


def create_hive_table(work_folder_name, hive_server_url, folders_list, hdfs_folder, database):
    print('Will be created {} folders'.format(len(folders_list)))
    for folder in folders_list:
        subfolder = get_name_prom_path(folder)
        table_name = get_table_name(work_folder_name, subfolder)

        location = os.path.join(hdfs_folder, subfolder)
        create_hive_script = "\"USE {}; CREATE EXTERNAL TABLE {}(one_str STRING, two_str STRING, four_int BIGINT, five_int BIGINT) " \
                             "STORED AS PARQUET " \
                             "LOCATION '{}' " \
                             "TBLPROPERTIES ('parquet.compress'='SNAPPY');\"".format(database, table_name, location)

        print('Hive_sql: ' + create_hive_script)
        create = Popen(["beeline", "-u", hive_server_url, "-e", create_hive_script], stdin=PIPE, bufsize=-1)
        create.communicate()
        if create.returncode != 0:
            raise HadoopException('Failed create hive table')
        print("successful table creation {}".format(table_name))


def get_table_name(work_folder_name, subdir):
    name = work_folder_name.replace('/', '__').replace('-', '_') + '__' + subdir
    return name


if __name__ == "__main__":
    args = get_args()
    print('Create local work folder')
    work_folder_name = '{}_{}'.format(args.folder_name, datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
    work_folder_path = os.path.join(args.local_tmp_folder, work_folder_name)
    os.mkdir(work_folder_path)

    print('Create local subfolders')
    folders_list = list()
    for i in range(args.sub_folder_count):
        path_join = os.path.join(work_folder_path, str(i + 1))
        folders_list.append(path_join)
        os.mkdir(path_join)

    print('Creation files')

    create_files(args.file_size_mb, folders_list, args.files_count, args.parquet)

    print('Upload files to HDFS')
    hive_line = 'jdbc:hive2://{}:10000/;principal=hive/_HOST@LC.CLUSTER'.format(args.hive_server)
    try:
        sc_upload_folder = 'hdfs://{}{}/'.format(args.nameservice, args.hdfs_upload_folder)
        sc_folder = sc_upload_folder + work_folder_name

        if args.rm_old:
            if args.parquet:
                delete_old_hive_tables_like(hive_line, args.folder_name, args.database)
            delete_old_hadoop_folder(sc_upload_folder, args.folder_name)

        upload_folder_to_hadoop(work_folder_path, sc_folder)
        if args.parquet:
            create_hive_table(work_folder_name, hive_line, folders_list, sc_folder, args.database)
    except HadoopException as err:
        print(repr(err))
        sys.exit(2)
    except CheckFolderHadoopException as err:
        print(repr(err))
