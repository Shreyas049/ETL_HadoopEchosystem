from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.stat import Correlation, Summarizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, trunc, count, isnan, countDistinct, sum
from pyspark.ml import Pipeline

appName = "Kaggle : Future Sales Prediction"
sparkSession = SparkSession.builder.master("local").appName(appName).getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {sparkSession.sparkContext.appName}')
    print(f'Master: {sparkSession.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

salesTrainData = sparkSession.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .parquet("hdfs://localhost:9000/user/hive/warehouse/miniproject.db/ext_sales_complete/*.parquet") \
    .sample(withReplacement=False, fraction=0.006, seed=200) \
    .drop(*["day","date_block_num"])


# salesTrainData.show(truncate=False)
# salesTrainData.summary().show()
# salesTrainData.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in salesTrainData.columns]).show()
# AggSalesTrainData = salesTrainData.groupBy("date_block_num", "shop_id", "item_id").agg(
#                     sum("item_cnt_day").alias("item_cnt_month"))

salesTrainData_agg = salesTrainData.groupBy("month","shop_id", "item_id").agg(sum("item_cnt_day").alias("monthly_count"))
print("Aggregated data-")
salesTrainData_agg.show(truncate=False)

# salesTrainData_new= salesTrainData_agg.join(salesTrainData,salesTrainData_agg['item_id']==salesTrainData['item_id'],"left")
# print("salesTrainData_new")

salesTrainData_new = salesTrainData.alias("s").join(salesTrainData_agg.alias("a"),col("s.item_id") == col("a.item_id"),'inner')\
    .select(col("a.month"),col("a.shop_id"),col("a.item_id"),col("monthly_count"),col("s.item_price"))

salesTrainData_new.show(truncate=False)
# .select(salesTrainData_new.month, salesTrainData_new.shop_id,salesTrainData_new.item_id,salesTrainData.item_price,salesTrainData_new['sum(item_cnt_day)'])

assembler = VectorAssembler(inputCols=["shop_id","item_id"],
                            outputCol="features").setHandleInvalid("skip")

# randomForest = RandomForestClassifier(labelCol="item_cnt_month", featuresCol="features",
#                                       maxDepth=5, numTrees=50, seed=2022)


logisticRegression = LinearRegression(featuresCol='features', labelCol='monthly_count')
# 22/01/16 00:25:22 ERROR Instrumentation: [fc218e35] Classification labels should be in [0 to 207]. Found 63 invalid labels.

pipeline = Pipeline(stages=[assembler, logisticRegression])
# This is the place where we create our model
SalesPredictionModel = pipeline.fit(salesTrainData_new)

SalesTestData = sparkSession.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .csv("file:///home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/Kaggle-Future_Sales_Prediction_Dataset/test.csv")
SalesTestData.show(truncate=False)
prediction = SalesPredictionModel.transform(SalesTestData)
# prediction.printSchema()
prediction.show()
prediction.withColumnRenamed("prediction","item_cnt_month").select("ID","item_cnt_month") \
    .sort("ID") \
    .write.mode('overwrite').csv("file:///home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/ml/PredictionOp:Kaggle_Future_Sales/prediction.csv", header=True)

print("Done")
#