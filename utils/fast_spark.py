from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

class SparkHelper(object):
    """
    Standard class for initializing a spark session.
    """
    @staticmethod
    def get_session(app_name):
      """
      The below configuration options should not be changed unless you know what you are doing.
      Note that the spark.yarn.queue should be updated to correspond to your group.
      """
      return SparkSession.builder.appName(app_name) \
        .config("spark.yarn.executor.memoryOverhead", "32g") \
        .config("spark.driver.memory", "64g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.kryoserializer.buffer","128") \
        .config("spark.kryoserializer.buffer.max","1g") \
        .config("spark.yarn.executor.memoryOverhead", "12g") \
        .config("spark.driver.maxResultSize", "64g") \
        .config("spark.yarn.queue", "root.adhoc") \
        .getOrCreate()    


import pandas as pd

def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]

def toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand
  
if __name__ == '__main__':

    spark = SparkHelper.get_session("pouya-spark-demo")

    #3gb
    with Timer("Read large csv"):
      LRX_Patient_sp = spark.read.csv("/user/mluczko/check_large.csv", sep=",", header=True, inferSchema=True)

    with Timer("Read large csv coalesce 1"):
      LRX_Patient_sp_coal1 = spark.read.csv("/user/mluczko/check_large.csv", sep=",", header=True, inferSchema=True).coalesce(1)

    with Timer("Read large csv coalesce 4"):
      LRX_Patient_sp_coal4 = spark.read.csv("/user/mluczko/check_large.csv", sep=",", header=True, inferSchema=True).coalesce(4)


    with Timer("toPandas coalesce1"):
      LRX_Patient = LRX_Patient_sp_coal1.toPandas()
    del LRX_Patient

    with Timer("faster toPandas large"):
      LRX_Patient = toPandas(LRX_Patient_sp, LRX_Patient_sp.rdd.getNumPartitions())
    del LRX_Patient

    with Timer("toPandas large csv coalesce 4"):
      LRX_Patient_coal4 = LRX_Patient_sp_coal4.toPandas()
    del LRX_Patient_coal4

    with Timer("faster toPandas large 4"):
    #  https://gist.github.com/joshlk/871d58e01417478176e7
      LRX_Patient_faster_toPandas = toPandas(LRX_Patient_sp_coal4, 4)
    del LRX_Patient_faster_toPandas
