## Command
### files-generator
```shell
python3 files-generator.py --nameservice ns --parquet \
 --hdfs_upload_folder '/user/spark' \
 --folder_name small_files --sub_folder_count 1 --files_count 2 --file_size 2 \
 --hive_server "hive-server.lc.cluster" --database default \
 --local_tmp_folder /tmp
```
