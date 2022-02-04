from pyspark import SparkContext, SparkConf, SQLContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, window, count
from pyspark.sql.types import *

master = 'local'
appName = 'PySpark_Streaming exam'
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

Stockschema = StructType([StructField('date', StringType(), True),
                          StructField('date_block_num', StringType(), True),
                          StructField('shop_id', StringType(), True),
                          StructField('item_id', StringType(), True),
                          StructField('item_price', StringType(), True),
                          StructField('item_cnt_day', StringType(), True)])
stock = ss.readStream.schema(Stockschema) \
    .option('header', False) \
    .option('delimiter', ',') \
    .csv('/home/theshree/streamingData')

stock.printSchema()

query = stock.writeStream \
    .outputMode('append')\
    .format('csv')\
    .option("checkpointLocation", "hdfs://localhost:9000/miniproject/checkpoint")\
    .option("path","hdfs://localhost:9000/miniproject/raw/stage").start()

query.awaitTermination(45)
