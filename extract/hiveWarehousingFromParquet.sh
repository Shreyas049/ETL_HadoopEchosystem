#!/bin/bash
cd $HIVE_HOME

# External table with complete data
hive -e "create database if not exists miniproject;use miniproject;
  drop table if exists ext_sales_complete;
  create external table if not exists ext_sales_complete (year int, month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) stored as parquet;"

hdfs dfs -cp -p -f /miniproject/raw/persist/* /user/hive/warehouse/miniproject.db/ext_sales_complete/

# Partition table with year and month as partitions
hive -e "use miniproject;
  set hive.exec.dynamic.partition.mode=nonstrict;
  drop table if exists sales_partition_year_month;
  create external table if not exists sales_partition_year_month (day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) PARTITIONED BY (year int, month int);
  insert overwrite table sales_partition_year_month partition(year, month) select day, date_block_num , shop_id, item_id, item_price, item_cnt_day, year, month from ext_sales_complete;

  set hive.enforce.bucketing = true;
  drop table if exists sales_bucketted_on_month;
  create table sales_bucketted_on_month (year int, month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) clustered by (month) into 11 buckets;
  insert overwrite table sales_bucketted_on_month select * from ext_sales_complete;"
  # Created bucketing table
