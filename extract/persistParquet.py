from pyspark import SparkContext, SparkConf, SQLContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, window, count
from pyspark.sql.types import *

master = 'local'
appName = 'PySpark_DataConvertParquet'
config = SparkConf().setAppName(appName).setMaster(master)
ss = SparkSession.builder.appName('MySparkStreamingSession')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .master('local')\
    .getOrCreate()

print(ss.sparkContext.getConf().get("spark.serializer"))
print('Done')

if ss:
    print(ss.sparkContext.appName)
else:
    print('Could not initialise pyspark session')

salesSchema = StructType([StructField('year', IntegerType(), True),
                          StructField('month', IntegerType(), True),
                          StructField('day', IntegerType(), True),
                          StructField('date_block_num', IntegerType(), True),
                          StructField('shop_id', IntegerType(), True),
                          StructField('item_id', IntegerType(), True),
                          StructField('item_price', FloatType(), True),
                          StructField('item_cnt_day', IntegerType(), True)])

df = ss.read.schema(salesSchema)\
    .option('header', True) \
    .option('delimiter', ',') \
    .csv('hdfs://localhost:9000/miniproject/raw/dq_good/part-r-00000')

df.printSchema()
df.show(truncate=False)

df.write\
    .mode('overwrite')\
    .parquet("hdfs://localhost:9000/miniproject/raw/persist/")
