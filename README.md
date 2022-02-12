## Command
### files-generator
```shell
python files-generator.py --nameservice ns --parquet \
 --hdfs_upload_folder '/user/spark' \
 --folder_name small_files --sub_folder_count 1 --files_count 2 --file_size 2 \
 --hive_server "hive-server.lc.cluster" --database default \
 --local_tmp_folder /tmp
```

### hdfs testing
```shell
python load-hdfs-testing.py --path hdfs://ns/user/spark/small_files_2021-09-23_11-39-15 --parallel_threads 2 --request_count 2 --download_folder /tmp
```

### hive testing
```shell
python load-hive-testing.py --hive_server hive-server.lc.cluster --parallel_threads 1 --request_count 1 --database default --table small_files_2021-09-23_11_39_15__1 --select
```

### spark testing
```shell
python load-spark-testing.py --nameservice ns --parallel_threads 1 --request_count 2 --path /user/spark/small_files_2021-09-23_11-39-15/1/file-1-0.parquet --select
```