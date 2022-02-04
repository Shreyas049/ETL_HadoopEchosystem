
To clean everything and start from scratch, run 'clean.sh'.
Do 'pip install -r requirements.txt' to set the environment.

Start hdfs and yarn

To start the execution, copy bean/Dag/airflowAutomation.py to Dag directory of you Airflow.
Start Airflow with
$ airflow webserver --port 9999
(On different terminal)
$ airflow scheduler

Flow of execution goes as follows:

Extract:
--initiateDirectories.sh
--sparkStream.py(After running this program, copy datafile.csv to spool folder(streamingData) manually)
(Here data cleaning is done using MapReduce, find the MapReduce jar in project directory, output stored under dq_good. (Refer 'airflowAutomation.py' for more info))
--persistParquet.py
Transform:
--hiveWarehousingFromParquet.sh
ML:
--future_sales_prediction.py


# Big Data project by Shreyas
