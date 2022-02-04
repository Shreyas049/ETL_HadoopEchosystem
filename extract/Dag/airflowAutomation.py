from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'theshree'
}

dag = DAG(
    dag_id='MiniProject_Future_Sales_Prediction',
    default_args=args,
    start_date=days_ago(1),
    schedule_interval='0 0 * * *',
    tags=['spark-submit', 'shreyas', 'miniproject']
)

initiate_directories = BashOperator(task_id="initiateDirectories",
                                  bash_command="/home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/bean/initiateDirectories.sh ",
                                  dag=dag)

# streamCSVtoHdfs = SparkSubmitOperator(
#     task_id='stream_CSVfile_to_HDFS',
#     name="streamDataToHdfs",
#     application='/home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/bean/sparkStream.py',
#     env_vars={'PYSPARK_DRIVER_PYTHON': '/home/theshree/myEnv/airflow-env/bin/python',
#               'HADOOP_CONF_DIR': '/home/theshree/DBDA_HOME/hadoop-3.3.1/etc/hadoop',
#               'PYSPARK_PYTHON': '/home/theshree/myEnv/airflow-env/bin/python',
#               'AIRFLOW_CONN_SPARK_DEFAULT': 'local'},
#     dag=dag
# )

streamCSVtoHdfs = BashOperator(task_id='stream_CSVfile_to_HDFS',
                               bash_command="python /home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/bean/sparkStream.py ",
                               dag=dag)

MapReduce_job = BashOperator(task_id="MapReduce_data_ingestion_to_dq_good",
                             bash_command="hadoop jar /home/theshree/Documents/DBDA/BigData/eclipse-workspace/Miniproject_FutureSales_DQ/target/Miniproject_FutureSales_DQ-0.0.1-SNAPSHOT.jar com.demo.Miniproject_FutureSales_DQ.DQ_Job /miniproject/raw/stage/part-00000-a74fbbdf-9ac3-4ba5-a46b-7b9c22ceef5b-c000.csv /miniproject/raw/dq_good ",
                             dag=dag)

dq_goodToPersist = SparkSubmitOperator(
    task_id='Convert_format_into_parquet',
    name="airflow-Change_DataFormat",
    application='/home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/bean/persistParquet.py',
    env_vars={'PYSPARK_DRIVER_PYTHON': '/home/theshree/myEnv/airflow-env/bin/python',
              'HADOOP_CONF_DIR': '/home/theshree/DBDA_HOME/hadoop-3.3.1/etc/hadoop',
              'PYSPARK_PYTHON': '/home/theshree/myEnv/airflow-env/bin/python',
              'AIRFLOW_CONN_SPARK_DEFAULT': 'local'},
    dag=dag
)

createDbAndTables = BashOperator(task_id="create_DB_ext_tables_partions_Buckets",
                             bash_command="bash /home/theshree/Documents/DBDA/BigData/PyCharm/ETL_HadoopEcosystem/bean/hiveWarehousingFromParquet.sh ",
                             dag=dag)

initiate_directories >> streamCSVtoHdfs >> MapReduce_job >> dq_goodToPersist >> createDbAndTables