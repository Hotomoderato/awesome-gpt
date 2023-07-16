from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import sys
jdbc = os.getcwd() + '/utils/ojdbc7.jar'  # change path where ojdbc7.jar file was saved
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ' + jdbc + ' pyspark-shell'

os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/SUPERCONDA/envs/python3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] ='/opt/cloudera/parcels/SUPERCONDA/envs/python3/bin/python'

def spark_session(appname='AA', yarn_queue='sts'):
    """
    Initialize sparkSession and sets the configuration specified
        in the config file

    :return: spark session
    """
    conf = SparkConf() \
        .set("spark.dynamicAllocation.initialExecutors", "8") \
        .set("spark.dynamicAllocation.maxExecutors", "64") \
        .set("spark.executor.memoryOverhead", "3g") \
        .set("spark.driver.maxResultSize", "5g") \
        .set("spark.kryoserializer.buffer.max", "1g") \
        .set("spark.executor.memory", "20g") \
        .set("spark.driver.memory", "16g") \
        .set("spark.driver.cores", "16") \
        .set('spark.sql.session.timeZone', 'UTC') \
        .set("spark.ui.showConsoleProgress", "true") \
        .set("spark.yarn.queue", yarn_queue)

    spark = SparkSession.builder \
        .appName(appname).config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    print(f"Created Spark Session {appname}")
    print(f"Spark Application ID: {spark.sparkContext.applicationId}")
    return spark


if __name__ == '__main__':
    spark = spark_session('TEST')
    spark.stop()