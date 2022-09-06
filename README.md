<h1 align="center">Hadoop Load Testing Utils</h1>
<p align="center">
<img src="https://img.shields.io/github/last-commit/gavr-dev/hadoop-load-testing"/>
<a href="https://github.com/gavr-dev/hadoop-load-testing/tags" alt="Tag"><img src="https://img.shields.io/github/v/tag/gavr-dev/hadoop-load-testing"/></a>
<a href="https://github.com/gavr-dev/hadoop-load-testing/blob/main/LICENSE" alt="GPLv3 licensed"><img src="https://img.shields.io/badge/license-GPLv3-blue"/></a>
</p>

The repository contains a set of utilities for preparing data (regular and parquet files) and generating load on Apache Hadoop.

## HDFS file generator
```shell
python files-generator.py \
  --nameservice '' \
  --parquet \
  --hdfs_upload_folder '' \
  --folder_name '' \
  --sub_folder_count 10 \
  --files_count 500 \
  --file_size 5 \
  --hive_server "" \
  --database default \
  --local_tmp_folder /tmp
```

## HDFS testing
```shell
python load-hdfs-testing.py \
  --path '' \
  --parallel_threads 4 \
  --request_count 10 \
  --download_folder /tmp
```

## Hive testing
```shell
python load-hive-testing.py \
  --hive_server '' \
  --parallel_threads 4 \
  --request_count 10 \
  --database '' \
  --table '' \
  --select
```

## Spark testing
```shell
python load-spark-testing.py \
  --nameservice '' \
  --parallel_threads 4 \
  --request_count 10 \
  --path '' \
  --select
```