import os
import sys
jdbc = os.getcwd() + '/utils/ojdbc7.jar'  # change cdsw location where ojdbc7.jar file saved
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ' + jdbc + ' pyspark-shell'

from pyspark.sql import SparkSession
from pyspark import SparkConf


def spark_session():
    """
    Initialize sparkSession and sets the configuration specified
        in the config file

    :return: spark session
    """
    conf = SparkConf() \
        .set("spark.dynamicAllocation.initialExecutors", "8") \
        .set("spark.dynamicAllocation.maxExecutors", "64") \
        .set("spark.dynamicAllocation.enabled", "true")\
        .set("spark.executor.memoryOverhead", "10g") \
        .set("spark.driver.maxResultSize", "20g") \
        .set("spark.kryoserializer.buffer.max", "1g") \
        .set("spark.app.name", "ml trigger") \
        .set("spark.executor.memory", "20g") \
        .set("spark.driver.memory", "16g") \
        .set("spark.driver.cores", "16") \
        .set("spark.yarn.queue", "adhoc")        

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    
    return spark


if __name__ == '__main__':
    spark = spark_session()
    spark.stop()

