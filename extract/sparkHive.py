import os
from os.path import abspath
from pyspark.sql import SparkSession

os.environ['SPARK_HOME'] = '/home/theshree/DBDA_HOME/spark-3.2.0-bin-hadoop3.2'

master = 'local'
appName = 'PySpark_Adding Data to Hive Warehouse'

warehouse_location = abspath('hdfs://localhost:9000/user/hive/warehouse')
metastore_location = abspath('/home/theshree/DBDA_HOME/apache-hive-2.3.9-bin')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config('spark.sql.warehouse.dir', warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config('hive.metastore.warehouse.dir', metastore_location) \
    .enableHiveSupport() \
    .getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

spark.sql("CREATE DATABASE IF NOT EXISTS miniproject;")
spark.sql("SHOW DATABASES;").show()
spark.sql("SHOW TABLES;").show()

# # Creating external table and putting all data in it
# spark.sql("create external table if not exists ext_sales "
#           "(year int, month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) "
#           "stored as parquet "
#           "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
#           "location '/user/hive/warehouse/miniproject.db/';")
# spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
# # Inserting data into ext_sales
# # spark.sql("dfs -put /miniproject/raw/persist/part-00000-408d1074-de44-4849-8f70-0c121db5491c-c000.snappy.parquet "
# #           "/user/hive/warehouse/miniproject.db/ext_sales/")
# os.system("hdfs dfs -put /miniproject/raw/persist/part-00000-408d1074-de44-4849-8f70-0c121db5491c-c000.snappy.parquet /user/hive/warehouse/miniproject.db/ext_sales/")
# # Creating external table partitioned by 'year'
# spark.sql("create table if not exists ext_sales_partitioned "
#           "(month int, day int, date_block_num int, shop_id int, item_id int,item_price float, item_cnt_day int) "
#           "PARTITIONED BY (year int) stored as parquet "
#           "location '/user/hive/warehouse/miniproject.db/';")
# # Inserting data into 'ext_sales_partitioned'
# spark.sql("insert overwrite table ext_sales_partition partition(year) "
#           "select month, day, date_block_num ,item_id ,item_price ,item_cnt_day, year from ext_sales;")
#
# spark.sql("SHOW TABLES").show()
