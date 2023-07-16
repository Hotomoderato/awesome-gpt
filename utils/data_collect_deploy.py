from subprocess import call
from contextlib import contextmanager
import logging
import pandas as pd
import pickle
import glob
import time
import json
import os, errno
from datetime import datetime
from utils.standardCode import *




def execImpala(query,cluster=None):
  r"""
    Run Impala command on given cluster. Returns nothing
      query   - Query to run
      cluster - Cluster where to run. Default is whatever current cluster is
  """
  real_cluster = thisCluster()
  if cluster != None:
    real_cluster = cluster
  
  impala_cmd = None
  if real_cluster == 'BDRA':
    impala_cmd = f'impala-shell --ssl -k -i usphdpimpala.rxcorp.com:21000 --delimited --output_delimiter=, --quiet --query="{query}"'
  elif real_cluster == 'DRML':
    impala_cmd = f'impala-shell --ssl -k -i usrhdpimpala.rxcorp.com:21000 --delimited --output_delimiter=, --quiet --query="{query}"'
  else:
    raise ValueError("Unsupported cluster")
  
  res_str = os.popen(impala_cmd).read()
  if re.search('ERROR ', res_str):
    raise ValueError(f"Query {res_str}")
    exit

      
def queryImpala(query,cluster=None):
  r"""
    Run Impala query on given cluster. Returns result as pandas DataFrame
      query   - Query to run
      cluster - Cluster where to run. Default is whatever current cluster is
  """
  real_cluster = thisCluster()
  if cluster != None:
    real_cluster = cluster
  
  impala_cmd = None
  if real_cluster == 'BDRA':
    impala_cmd = f'impala-shell --ssl -k -i usphdpimpala.rxcorp.com:21000 --delimited --output_delimiter=, --quiet --query="{query}"'

  elif real_cluster == 'DRML':
    impala_cmd = f'impala-shell --ssl -k -i usrhdpimpala.rxcorp.com:21000 --delimited --output_delimiter=, --quiet --query="{query}"'

  else:
    raise ValueError("Unsupported cluster")
  
  res_str = os.popen(impala_cmd).read()
  if re.search('ERROR:.+Exception:', res_str):
    raise ValueError(f"Query {res_str}")
  
  return pd.read_csv(io.StringIO(res_str))



@contextmanager
def timer(msg):
    """calculate elapse time
    """
    t0 = time.time()
    logging.info(f'[{msg}] start.')
    yield
    elapse_time = time.time() - t0
    logging.info(f'[{msg}] done in {elapse_time:.2f}s.')
    
    
def load_json(path):
    """
    Read in config file
    """ 
    with open(path, 'r') as handle:
        file = json.load(handle)
    return file

class PickleUtils(object):
    """
    Pickle file loader/saver utility functions
    """
    def __init__(self):
        pass
    
    @staticmethod
    def loader(directory):
        with open(directory, 'rb') as f:
            data = pickle.load(f)
        #print("load pickle from {}".format(directory))
        return data
    
    @staticmethod
    def saver(directory, data):
        os.makedirs(os.path.dirname(directory), exist_ok=True)
        with open(directory, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print("save pickle to {}".format(directory))


def read_multi_csv(directory):
    """
    Read multiple csv files in the same directory

    :param path: folder path
    :return: single pandas dataframe combining all csv files
    """
    file_list = glob.glob(os.path.join(directory, "*.csv.gz"))
    df_from_each_file = (pd.read_csv(f) for f in file_list)
    df = pd.concat(df_from_each_file, ignore_index=True)
    return df


def set_logger(log_path, new=False):
    """Sets the logger to log info in terminal and file `log_path`.
    In general, it is useful to have a logger so that every output to the terminal is saved
    in a permanent file. Here we save it to `model_dir/train.log`.
    Example:
    ```
    logging.info("Starting training...")
    ```
    Args:
        log_path: (string) where to log
        new: (string) if it create a new log file
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if new == True: silentremove(log_path)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging to a file
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s: %(message)s',
                                                    "%Y-%m-%d %H:%M:%S %Z"))
        logger.addHandler(file_handler)

        # Logging to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(stream_handler)


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred

    
def data_load_from_db_query(spark,QUERY,file_path,file_name,sep,HOST,DB,USER,PASSWORD):
    sp_df = spark.read.format('jdbc').options(driver='oracle.jdbc.driver.OracleDriver',
                                          url='jdbc:oracle:thin:@//{}/{}'.format(HOST, DB),
                                          user=USER,
                                          password=PASSWORD,
                                          dbtable=QUERY).load()
    df= sp_df.toPandas()    
    df.columns = [col.lower() for col in df.columns]
    file_name = file_name +  '.txt'
    df.to_csv(os.path.join(file_path,file_name), index=False,sep = sep)
    print('table is saved in {}/{}'.format(file_path, file_name))
    
    
#def data_save_to_db(file_sdf, tb_name , HOST,DB,USER,PASSWORD):
#     file_sdf.write.mode("Overwrite").format('jdbc')\
#    .option("driver", "oracle.jdbc.driver.OracleDriver")\
#    .option("url", "jdbc:oracle:thin:{}/{}@//{}/{}".format(USER, PASSWORD, HOST, DB))\
#    .option("dbtable", tb_name )\
#    .save()
    
    
def analyze_tab(spark_session,db_tab_name_in,partition_col_name='',partition_value=''):
  if partition_col_name != '':
    l_sql='analyze table ''' + db_tab_name_in  + ''' partition(''' + partition_col_name + '''= ''' + str(partition_value) + ''' ) compute statistics '''
  else:
    l_sql='analyze table ''' + db_tab_name_in  + '''  compute statistics '''
  spark_session.sql(l_sql)
  print('Record of ' + db_tab_name_in + ' : ' )
  spark_session.sql('select count(1) Records from ' + db_tab_name_in).show()
  
  
def data_save_to_db(file_sdf, tb_name , HOST,DB,USER,PASSWORD):
     file_sdf=file_sdf.toDF(*[c.upper() for c in file_sdf.columns])
     file_sdf.write.mode("Overwrite").format('jdbc')\
    .option("driver", "oracle.jdbc.driver.OracleDriver")\
    .option("url", "jdbc:oracle:thin:{}/{}@//{}/{}".format(USER, PASSWORD, HOST, DB))\
    .option("dbtable", tb_name )\
    .save()
    
    
def data_append_to_db(file_sdf, tb_name , HOST,DB,USER,PASSWORD):
     file_sdf.write.mode("Append").format('jdbc')\
    .option("driver", "oracle.jdbc.driver.OracleDriver")\
    .option("url", "jdbc:oracle:thin:{}/{}@//{}/{}".format(USER, PASSWORD, HOST, DB))\
    .option("dbtable", tb_name )\
    .save()
    
    
# for  oncology data sync project 


#functions 



def is_table_exists(spark ,database,table):
  table_list=spark.sql(f"""show tables in  {database}""")
  table=table.lower()
  table_name=table_list.filter(table_list.tableName== f"{table}").collect()
  if len(table_name)>0:
    return 1  
  else:
    return 0
  
  

def analyze_tab(spark_session,db_tab_name_in,partition_col_name='',partition_value=''):
  if partition_col_name != '':
    l_sql='analyze table ''' + db_tab_name_in  + ''' partition(''' + partition_col_name + '''= ''' + str(partition_value) + ''' ) compute statistics '''
  else:
    l_sql='analyze table ''' + db_tab_name_in  + '''  compute statistics '''
  spark_session.catalog.refreshTable(f'{db_tab_name_in}')
  spark_session.sql(l_sql)
  print('Record of ' + db_tab_name_in + ' : ' )
  spark_session.sql('select count(1) Records from ' + db_tab_name_in).show()
  

def read_from_oracle(spark,USER, PASSWORD, HOST, DB,l_query,fetchsize):
    l_result = spark.read.format('jdbc') \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("url", "jdbc:oracle:thin:{}/{}@//{}/{}".format(USER, PASSWORD, HOST, DB))\
        .option("dbtable", l_query)\
        .option("sessionInitStatement","""BEGIN execute immediate 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 10'; END;""")\
        .option("fetchsize",fetchsize)\
        .load()
    return l_result    


def read_from_oracle_using_partition_key(spark,USER, PASSWORD, HOST, DB,l_query,fetchsize,numPartitions,lowerBound,upperBound):
    l_result = spark.read.format('jdbc') \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("url", "jdbc:oracle:thin:{}/{}@//{}/{}".format(USER, PASSWORD, HOST, DB))\
        .option("dbtable", l_query)\
        .option("sessionInitStatement","""BEGIN execute immediate 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 10'; END;""")\
        .option("numPartitions",numPartitions)\
        .option("partitionColumn","partition_key")\
        .option("lowerBound",lowerBound)\
        .option("upperBound",upperBound)\
        .option("fetchsize",fetchsize)\
        .load()
    return l_result

  
def save_sdf_to_table(spark, table, spark_dataframe, partition, format = 'parquet', mode='append',database='prod_sts', repartition_num=50):
  r"""
    Create or insert into Hive table. returns DataFrame with query results to use further in the process
      table     - Required table name
      spark_dataframe  - Required spark dataframe
      partition - Optional, partition columns. No partitions by default
      format    - Optiona table format. Default or null to parquet. 
      mode      - Optional, how to insert. defaul to append.
      database  - Database to save to. Default to prod_sts, currently in use if null.
    
  """
  
  
  
  spark.sql('SET spark.sql.crossJoin.enabled=true')
  spark.conf.set("spark.sql.crossJoin.enabled", 'true')
  spark.sql('SET hive.merge.mapfiles=true')
  spark.sql('SET hive.merge.mapfiles=true')
  spark.conf.set('hive.merge.mapfiles','true')
  spark.conf.set('hive.merge.mapredfiles','true')
  spark.conf.set('hive.merge.size.per.task',256000000)
  spark.conf.set('hive.merge.smallfiles.avgsize',283115520)
  spark.conf.set('parquet.blocksize',268435456)
  spark.conf.set('dfs.block.size',268435456)    
  spark.conf.set('hive.exec.compress.output','true')
  spark.conf.set('parquet.compression','snappy')
  spark.conf.set('hive.exec.dynamic.partition.mode','nonstrict')
   
  
  v_return = spark_dataframe.toDF(*[c.upper() for c in spark_dataframe.columns])     

  if partition != None and partition in v_return.columns:
    v_return.repartition(repartition_num).write.mode(mode).format(format).partitionBy(partition).saveAsTable(f"{database}.{table}")
    spark.catalog.refreshTable(f"{database}.{table}")
#    spark.sql(f"ANALYZE TABLE {database}.{table} partition({partition}) COMPUTE STATISTICS")
  else:
    v_return.repartition(repartition_num).write.mode(mode).format(format).saveAsTable(f"{database}.{table}")
    spark.catalog.refreshTable(f"{database}.{table}")
#    spark.sql(f"ANALYZE TABLE {database}.{table}  COMPUTE STATISTICS")
  print(f"Record of {database}.{table}: " )
  spark.sql(f"select count(1) Records from {database}.{table}").show()

  
def save_sdf_to_table_cache(spark, table, spark_dataframe, partition, format = 'parquet', mode='append',database='prod_sts', repartition_num=50):
  r"""
    Create or insert into Hive table. returns DataFrame with query results to use further in the process
      table     - Required table name
      spark_dataframe  - Required spark dataframe
      partition - Optional, partition columns. No partitions by default
      format    - Optiona table format. Default or null to parquet. 
      mode      - Optional, how to insert. defaul to append.
      database  - Database to save to. Default to prod_sts, currently in use if null.
    
  """
  
  
  
  spark.sql('SET spark.sql.crossJoin.enabled=true')
  spark.conf.set("spark.sql.crossJoin.enabled", 'true')
  spark.sql('SET hive.merge.mapfiles=true')
  spark.sql('SET hive.merge.mapfiles=true')
  spark.conf.set('hive.merge.mapfiles','true')
  spark.conf.set('hive.merge.mapredfiles','true')
  spark.conf.set('hive.merge.size.per.task',256000000)
  spark.conf.set('hive.merge.smallfiles.avgsize',283115520)
  spark.conf.set('parquet.blocksize',268435456)
  spark.conf.set('dfs.block.size',268435456)    
  spark.conf.set('hive.exec.compress.output','true')
  spark.conf.set('parquet.compression','snappy')
  spark.conf.set('hive.exec.dynamic.partition.mode','nonstrict')
   
  
  v_return = spark_dataframe.toDF(*[c.upper() for c in spark_dataframe.columns])   
  
  
  l_table_view = 'table_view'
  
  l_table_cache = 'table_cache'
  
  v_return.createOrReplaceTempView(f'{l_table_view}')
  
  with timer(f' cache table  {l_table_cache}:'):
        spark.sql (f""" drop view if exists  {l_table_cache}""")
        spark.sql (f""" cache table  {l_table_cache}  as select * from  {l_table_view} a """)       
        
  l_sdf = spark.sql(f""" select * from  {l_table_cache}""") 

  if partition != None and partition in v_return.columns:
    l_sdf.repartition(repartition_num).write.mode(mode).format(format).partitionBy(partition).saveAsTable(f"{database}.{table}")
    spark.catalog.refreshTable(f"{database}.{table}")
#    spark.sql(f"ANALYZE TABLE {database}.{table} partition({partition}) COMPUTE STATISTICS")
  else:
    l_sdf.repartition(repartition_num).write.mode(mode).format(format).saveAsTable(f"{database}.{table}")
    spark.catalog.refreshTable(f"{database}.{table}")
#    spark.sql(f"ANALYZE TABLE {database}.{table}  COMPUTE STATISTICS")
  print(f"Record of {l_table_cache}: " )
  print(l_sdf.count())
  spark.catalog.clearCache()
#  spark.sql(f"select count(1) Records from {database}.{table}").show()



def insert_sdf_sql_to_table(spark,database, table, sql):
  r"""
    Create or insert into Hive table. returns DataFrame with query results to use further in the process
      table     - Required table name
      sql       - Required spark sql    
    
  """
  spark.sql(sql)
  spark.catalog.refreshTable(f"{database}.{table}")
  spark.sql(f"ANALYZE TABLE {database}.{table}  COMPUTE STATISTICS")
  print(f"Record of {database}.{table}: " )
  spark.sql(f"select count(1) Records from {database}.{table}").show()

	
def overwrite_partiton_table(spark,database,table,partition): 
  spark.sql('set hive.exec.parallel = true' )                  
  spark.sql('set hive.merge.mapfiles = true')                    
  spark.sql('set hive.merge.mapredfiles=true')                   
  spark.sql('set hive.merge.size.per.task=268435456')            
  spark.sql('set hive.merge.smallfiles.avgsize=283115520')       
  spark.sql('set mapred.max.split.size=68157440')                
  spark.sql('set mapred.min.split.size = 68157440')              
  spark.sql('set parquet.blocksize=268435456')                   
  spark.sql('set dfs.block.size=268435456')                      
  spark.sql('set hive.exec.compress.output=true')                
  spark.sql('set parquet.compression=snappy')                    
  spark.sql('set hive.optimize.sort.dynamic.partition=true')     
  spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')    
  spark.sql('set hive.support.quoted.identifiers=none')          
  spark.sql('set hive.exec.dynamic.partition=true')              
  spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')    
  spark.sql('set hive.exec.reducers.bytes.per.reducer=268435456')  
  
  l_sdf=spark.sql(f""" select {partition}, count(1) record_count from  {database}.{table} group by {partition} order by 1 """) 
    
  for l_part in l_sdf.collect():
    l_month_id = l_part[f'{partition}']
    l_record_count = l_part['record_count']
    print(f"""{partition}:""" + str(l_month_id) + f""" record_count: """ +  str(l_record_count))
    with timer(f"""overwrite partition : {l_month_id} """):
      spark.sql(f""" INSERT OVERWRITE TABLE {database}.{table} partition({partition}) select * from {database}.{table} where {partition}={l_month_id}""")
      print(spark.sql(f""" select count(1) record_count from  {database}.{table} where {partition} = {l_month_id} """).show())


def overwrite_table_self(spark,database,table,partition):
  l_tmp_tab =table + str(datetime.today().strftime('%Y%m%d%H%M%S%f'))
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.sql(f""" create table {database}.{l_tmp_tab} stored as parquet as select * from {database}.{table}""")
  spark.catalog.refreshTable(f"{database}.{l_tmp_tab}")  
  
  if partition != None:
    spark.sql(f"INSERT OVERWRITE TABLE {database}.{table} PARTITION({partition}) SELECT * FROM  {database}.{l_tmp_tab}")
  else:
    spark.sql(f"INSERT OVERWRITE TABLE {database}.{table}  SELECT * FROM  {database}.{l_tmp_tab}")
  
  spark.sql(f""" drop table {database}.{l_tmp_tab} purge""")
  analyze_tab(spark_session=spark,db_tab_name_in=database+'.'+table)  
  
def overwrite_table_self_distinct(spark,database,table,partition):
  l_tmp_tab =table + str(datetime.today().strftime('%Y%m%d%H%M%S%f'))
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.sql(f""" create table {database}.{l_tmp_tab} stored as parquet as select * from {database}.{table}""")
  spark.catalog.refreshTable(f"{database}.{l_tmp_tab}")  
  
  if partition != None:
    spark.sql(f"INSERT OVERWRITE TABLE {database}.{table} PARTITION({partition}) SELECT distinct * FROM  {database}.{l_tmp_tab}")
  else:
    spark.sql(f"INSERT OVERWRITE TABLE {database}.{table}  SELECT distinct * FROM  {database}.{l_tmp_tab}")
  
  spark.sql(f""" drop table {database}.{l_tmp_tab} purge""")
  analyze_tab(spark_session=spark,db_tab_name_in=database+'.'+table)  
  
def drop_table_if_exists(spark,database,table):
  spark.sql(f'drop table if exists {database}.{table} purge')
  
def truncate_table(spark,database,table):
  spark.sql(f"""truncate table {database}.{table}""")
  spark.catalog.refreshTable(f"{database}.{table}")
  spark.sql(f"ANALYZE TABLE {database}.{table}  COMPUTE STATISTICS")
  
def drop_partition(spark,database,table, partition_name, partition_value):
  spark.sql(f""" ALTER TABLE {database}.{table} DROP IF EXISTS PARTITION ({partition_name}={partition_value})  """)
  spark.sql(f"refresh {database}.{table}")
#  spark.sql(f"ANALYZE TABLE {database}.{table}  COMPUTE STATISTICS")
  
  
# functions


def get_month_list(spark,\
                   month_from,\
                   month_to
                  ):
  """
  month_from,\
  month_to
  """
#  month_from= 202010
#  month_to= 202010

  l_sql =f""" SELECT distinct a.month_id
          FROM prod_df2_us9.v_calendar a
          WHERE a.month_id BETWEEN {month_from} AND {month_to}
          order by 1
          """  
  l_result = spark.sql(l_sql)
  l_return_list = list(l_result.select('month_id').toPandas()['month_id'].astype(str))  
    
  return  l_return_list



def get_week_list(spark,\
                   week_from,\
                   week_to
                  ):
  """
  week_from,\
  week_to
  """ 

  l_sql =f""" SELECT distinct a.week_id
          FROM prod_df2_us9.v_calendar a
          WHERE a.week_id BETWEEN {week_from} AND {week_to}
          order by 1
          """  
  l_result = spark.sql(l_sql)
  l_return_list = list(l_result.select('week_id').toPandas()['week_id'].astype(str))  
    
  return  l_return_list


def get_month_week(spark,\
                   month_from,\
                   month_to
                  ):
  """
  month_from,\
  month_to
  """
#  month_from= 202001
#  month_to= 202010

  spark.catalog.refreshTable('prod_df2_us9.v_calendar')
  l_sql =f""" SELECT min(a.week_id) begin_week_id,
                 max(a.week_id) end_week_id,
                 min(from_unixtime(unix_timestamp(a.calendar_dt),'yyyyMMdd')) AS begin_svc_dt,
                 max(from_unixtime(unix_timestamp(a.calendar_dt),'yyyyMMdd')) AS end_svc_dt,
                 min(a.month_id) AS begin_month_id,
                 max(a.month_id) AS end_month_id,
                 count(distinct a.week_id) AS numworkers
          FROM prod_df2_us9.v_calendar a
          WHERE a.month_id BETWEEN {month_from} AND {month_to}
          """
  l_result = spark.sql(l_sql)
  l_result_pd=l_result.toPandas().astype(str)
  l_begin_week_id  =  l_result_pd.iloc[0]['begin_week_id']
  l_end_week_id =  l_result_pd.iloc[0]['end_week_id']
  l_begin_svc_dt= l_result_pd.iloc[0]['begin_svc_dt']
  l_end_svc_dt=l_result_pd.iloc[0]['end_svc_dt']
  l_begin_month_id= l_result_pd.iloc[0]['begin_month_id']
  L_end_month_id= l_result_pd.iloc[0]['end_month_id']
  l_numworkers= l_result_pd.iloc[0]['numworkers']
  
  return   l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers




def get_list_diff(li1, li2):
    return (list(list(set(li1)-set(li2)) + list(set(li2)-set(li1))))
  

    
  
def get_missing_months(spark,database,table,month_from,month_to):
  
  l_months=get_month_list(spark=spark,month_from=month_from,month_to=month_to)

  l_months_sdf = spark.sql(f""" select distinct month_id from {database}.{table} order by 1""")
  
  l_months_tab_list = list(l_months_sdf.select('month_id').toPandas()['month_id'].astype(str))
  
 
  
  check =  all(item in l_months_tab_list for item in l_months)
  
  if check is True:
    return get_list_diff(l_months,l_months)
  else :
    return list(set(l_months) - set(l_months_tab_list))


def print_month_count(spark,database,table, rows):
  l_sdf = spark.sql(f""" select count(distinct month_id) as number_of_months ,
                    cast(months_between( FROM_UNIXTIME(UNIX_TIMESTAMP(cast(max(month_id) as string),'yyyyMM')) 
                   ,FROM_UNIXTIME(UNIX_TIMESTAMP(cast(min(month_id) as string),'yyyyMM'))) + 1 as int)
                   as number_of_datamonhts from {database}.{table} """)
  
  print(l_sdf.show())
  
  l_months_sdf = spark.sql(f""" select month_id , count(1) as rows from {database}.{table} group by month_id order by 1""")
  
  print(l_months_sdf.show(rows))



#
#def  data_collect(spark,\
#                  database_target,\
#                  table_target,\
#                  month_from,\
#                  month_to,\
#                  query,\
#                  method='INITIAL'):
#  
#  
#  execImpala(cluster='DRML', \
#           query=f""" create table if not exists {database_target}.{table_target} partitioned BY (month_id) stored as parquet 
#           as select * from (  {query}  ) a where 1 = 0     """)
#  
#  truncate_table(spark=spark,database=database_target,table=table_target)
#
#  
#  l_month_list = get_month_list(spark,month_from,month_to)
#
#  for l_month_id in l_month_list: 
#    l_query  =  f""" insert into {database_target}.{table_target} partition(month_id) select * from ( {query} ) a 
#    where month_id  =  {l_month_id} """
#    
#    with timer(f'loading month: {l_month_id}'):
#      execImpala(cluster='DRML', \
#               query=l_query)
#      
#      queryImpala(cluster='DRML', \
#               query=f' select count(1) as records from {g_prod_pmx}.{g_v_aa_pmtxp_ndc_stck} where month_id={l_month_id}')
#  
#  sparkAnalyze(spark=spark,table=f'{database_target}.{table_target}')
#  overwrite_table_self(spark=spark,database=database_target,table=table_target,partition=None)
# 


def save_log(file_path):
  logging.basicConfig(level=logging.INFO,\
                      format='%(asctime)s :: %(levelname)s :: %(message)s',\
                      file=file_path)



def  data_collect(spark,\
                  database_target,\
                  table_target,\
                  month_from,\
                  month_to,\
                  query,\
                  method='INITIAL'):
  
  #### using impala as extraction engine
  
  l_sdf  = spark.sql(query)
  l_sdf.toDF(*[c.upper() for c in l_sdf.columns])
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  

  
  if method=='INITIAL':
    execImpala(cluster='DRML', \
           query=f""" drop table if  exists {database_target}.{table_target}  purge """)
    execImpala(cluster='DRML', \
           query=f""" create table if not exists {database_target}.{table_target} partitioned BY (month_id) stored as parquet 
           as select * from (  {query}  ) a where 1 = 0     """)
    truncate_table(spark=spark,database=database_target,table=table_target)
    l_month_list = get_month_list(spark,month_from,month_to)
  elif method=='APPEND':
    l_month_list = get_month_list(spark,month_from,month_to)

  
  for l_month_id in l_month_list: 

      if 'WEEK_ID' in l_sdf.columns:

        l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
        l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

        l_query  =  f""" insert into {database_target}.{table_target} 
                    partition(month_id) select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """

      else:
        l_where_time = f' where month_id  =  {l_month_id} '

        l_query  =  f""" insert into {database_target}.{table_target} 
                    partition(month_id) select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """
      
      
      with timer(f'loading month: {l_month_id}'):
        execImpala(cluster='DRML',query=f'alter table {database_target}.{table_target} drop if exists partition(month_id={l_month_id})')
        execImpala(cluster='DRML',query=l_query)
#        queryImpala(cluster='DRML',query=f' select count(1) as records from {database_target}.{table_target} where month_id={l_month_id}')    
      
      
  sparkAnalyze(spark=spark,table=f'{database_target}.{table_target}')
#  overwrite_table_self(spark=spark,database=database_target,table=table_target,partition=None)


def  data_collect_spark(spark,\
                  database_target,\
                  table_target,\
                  month_from,\
                  month_to,\
                  query,\
                  method='INITIAL'):
  
  
  #### using spark  as extraction engine
  l_sdf  = spark.sql(query)  
  l_sdf.toDF(*[c.upper() for c in l_sdf.columns])
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      
  
  if method=='INITIAL':
    execImpala(cluster='BDRA',\
               query=f""" drop table if  exists {database_target}.{table_target}  purge """)
    execImpala(cluster='BDRA', \
           query=f""" create table if not exists {database_target}.{table_target} partitioned BY (month_id) stored as parquet 
           as select * from (  {query}  ) a where 1 = 0     """)
    truncate_table(spark=spark,database=database_target,table=table_target)
    l_month_list = get_month_list(spark,month_from,month_to)
  elif method=='APPEND':
    l_month_list = get_month_list(spark,month_from,month_to)

  
  for l_month_id in l_month_list: 

      if 'WEEK_ID' in l_sdf.columns:

        l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
        l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

        l_query  =  f""" insert into {database_target}.{table_target} 
                    partition(month_id) select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """

      else:
        l_where_time = f' where month_id  =  {l_month_id} '

        l_query  =  f""" insert into {database_target}.{table_target} 
                    partition(month_id) select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """
      
      
      with timer(f'loading month: {l_month_id}'):
#        print(l_query)
        spark.sql(l_query)
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {database_target}.{table_target} where month_id={l_month_id}')    
      
      
  sparkAnalyze(spark=spark,table=f'{database_target}.{table_target}')
  overwrite_table_self(spark=spark,database=database_target,table=table_target,partition=None)
  


def  data_collect_month_cache(spark,\
                  database_target,\
                  table_target,\
                  month_from,\
                  month_to,\
                  query,\
                  method='INITIAL',\
                  repartiton_num=1):
  
  
  #### using spark  as extraction engine
  l_sdf  = spark.sql(query)  
  l_sdf.toDF(*[c.upper() for c in l_sdf.columns])
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      
  
  if method=='INITIAL':
    spark.sql(f""" drop table if  exists {database_target}.{table_target}  purge """)
    spark.sql(f""" create table if not exists {database_target}.{table_target}   USING PARQUET partitioned BY (month_id)  as select * from (  {query}  ) a where 1 = 0 """)
    truncate_table(spark=spark,database=database_target,table=table_target)
    l_month_list = get_month_list(spark,month_from,month_to)
  elif method=='APPEND':
    spark.sql(f""" create table if not exists {database_target}.{table_target}   USING PARQUET partitioned BY (month_id)  as select * from (  {query}  ) a where 1 = 0 """)
    l_month_list = get_month_list(spark,month_from,month_to)

  
  for l_month_id in l_month_list: 

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)

      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_query  =  f""" select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """
 
      l_table_cache = f'{table_target}_cache'
      
      spark.conf.set("hive.merge.mapfiles","true")
      spark.conf.set("hive.merge.mapredfiles","true")
      spark.conf.set("hive.merge.size.per.task",256000000)
      spark.conf.set("hive.merge.smallfiles.avgsize",134217728)
      spark.conf.set("hive.exec.compress.output","true")
      spark.conf.set("parquet.compression","snappy")
      spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
      spark.conf.set("hive.exec.dynamic.partition","true")
      
      l_table_view_repartition_200 = f'{table_target}_view_repartition_200'
      
      
#      print(l_query)
      
      l_sdf=spark.sql(l_query)
      
      l_sdf.repartition(200).createOrReplaceTempView(f'{l_table_view_repartition_200}')
      
      
      with timer(f' cache table  {l_table_cache}: {l_month_id}'):
          spark.sql (f""" drop view if exists  {l_table_cache}""")
          spark.sql (f""" cache table  {l_table_cache}  as select * from {l_table_view_repartition_200}  """)
      
      with timer(f'loading month: {l_month_id}'):
#        print(l_query)

        l_sdf=  spark.sql (f'select * from {l_table_cache}')
        
        l_table_view = f'{table_target}_view'
        
        l_sdf.repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')   
                
        spark.sql(f""" ALTER TABLE {database_target}.{table_target} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into  {database_target}.{table_target} partition(month_id) select * from {l_table_view} """)
        
        spark.sql (f""" drop view if exists  {l_table_cache}""")
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {database_target}.{table_target} where month_id={l_month_id}')    
        
#        spark.sql(f'ANALYZE TABLE {database_target}.{table_target} COMPUTE STATISTICS NOSCAN')
        spark.sql(f'REFRESH TABLE {database_target}.{table_target}')

#  sparkAnalyze(spark=spark,table=f'{database_target}.{table_target}')
  	
	

def  data_collect_week_cache(spark,\
                  database_target,\
                  table_target,\
                  week_from,\
                  week_to,\
                  query,\
                  method='INITIAL',\
                  repartiton_num=1):
  
  
  #### using spark  as extraction engine
  l_sdf  = spark.sql(query)  
  l_sdf.toDF(*[c.upper() for c in l_sdf.columns])
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      
  
  if method=='INITIAL':
    spark.sql(f""" drop table if  exists {database_target}.{table_target}  purge """)
    spark.sql(f""" create table if not exists {database_target}.{table_target}   USING PARQUET partitioned BY (week_id)  as select * from (  {query}  ) a where 1 = 0 """)
    truncate_table(spark=spark,database=database_target,table=table_target)
    l_week_list = get_week_list(spark,week_from,week_to)
  elif method=='APPEND':
    spark.sql(f""" create table if not exists {database_target}.{table_target}   USING PARQUET partitioned BY (week_id)  as select * from (  {query}  ) a where 1 = 0 """)
    l_week_list = get_week_list(spark,week_from,week_to)

  
  for l_week_id in l_week_list:       

      l_where_time = f' where week_id  =  {l_week_id} '

      l_query  =  f""" select * from ( select * from ( {query} ) a {l_where_time} ) a 
                    """
 
      l_table_cache = f'{table_target}_cache'
      
      spark.conf.set("hive.merge.mapfiles","true")
      spark.conf.set("hive.merge.mapredfiles","true")
      spark.conf.set("hive.merge.size.per.task",256000000)
      spark.conf.set("hive.merge.smallfiles.avgsize",134217728)
      spark.conf.set("hive.exec.compress.output","true")
      spark.conf.set("parquet.compression","snappy")
      spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
      spark.conf.set("hive.exec.dynamic.partition","true")
      
      l_table_view_repartition_200 = f'{table_target}_view_repartition_200'
      
      
#      print(l_query)
      
      l_sdf=spark.sql(l_query)
      
      l_sdf.repartition(200).createOrReplaceTempView(f'{l_table_view_repartition_200}')
      
      
      with timer(f' cache table  {l_table_cache}: {l_week_id}'):
          spark.sql (f""" drop view if exists  {l_table_cache}""")
          spark.sql (f""" cache table  {l_table_cache}  as select * from {l_table_view_repartition_200}  """)
      
      with timer(f'loading week: {l_week_id}'):
#        print(l_query)

        l_sdf=  spark.sql (f'select * from {l_table_cache}')
        
        l_table_view = f'{table_target}_view'
        
        l_sdf.repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')   
                
        spark.sql(f""" ALTER TABLE {database_target}.{table_target} DROP IF EXISTS PARTITION (week_id={l_week_id})  """)
        
        spark.sql(f""" insert into  {database_target}.{table_target} partition(week_id) select * from {l_table_view} """)
        
        spark.sql (f""" drop view if exists  {l_table_cache}""")
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_week_id} from {database_target}.{table_target} where week_id={l_week_id}')    
        
        spark.sql(f'REFRESH TABLE {database_target}.{table_target}')
  
def querySparkSQL(spark,query,rows=50):
    l_sdf = spark.sql(query)
    l_sdf.repartition(100)
    print(l_sdf.limit(rows).toPandas())
#    l_sdf.show(rows,truncate=False)
    
  
  
def impalaRefresh(spark,table):  
  execImpala(f"invalidate metadata {table}")
  execImpala(f"refresh  {table}")
  
	
def  spark_add_partition(spark,\
                  database,\
                  table,\
                  partition_column,\
                  partition_value,\
                  query,\
                  method='INITIAL'):
  
  
  #### using spark  as extraction engine
  l_sdf  = spark.sql(query)  
  l_sdf.toDF(*[c.upper() for c in l_sdf.columns])
  spark.sql('SET hive.merge.mapfiles = true')
  spark.conf.set('hive.merge.mapfiles','true')
  spark.conf.set('hive.merge.mapredfiles','true')
  spark.conf.set('hive.merge.size.per.task',256000000)
  spark.conf.set('hive.merge.smallfiles.avgsize',283115520)
  spark.conf.set('mapred.max.split.size',68157440)
  spark.conf.set('mapred.min.split.size',68157440)
  spark.conf.set('hive.exec.compress.output','true')
  spark.conf.set('parquet.compression','snappy')
  spark.conf.set('hive.exec.dynamic.partition.mode','nonstrict')
      
  
  database_target = database
  table_target = table
  execImpala(cluster='DRML', \
           query=f""" create table if not exists {database_target}.{table_target} partitioned BY ({partition_column}) stored as parquet 
           as select * from (  {query}  ) a where 1 = 0     """)
  
  spark.sql(f""" refresh {database_target}.{table_target} """) 
  
  if  method=='INITIAL': 
      dorp_partition(spark,database_target,table_target, partition_column, partition_value)

  temp_table = f"tmp_{table_target}"
  l_sdf.createOrReplaceTempView(temp_table)
      
  l_query  =  f""" insert into {database_target}.{table_target} 
                    partition({partition_column}) select * from {temp_table} a 
                    """


  with timer(f'loading {partition_column}: {partition_value}'):
#        print(l_query)
        spark.sql(l_query)
        querySparkSQL(spark=spark,query=f' select count(1) as records from {database_target}.{table_target} where {partition_column}={partition_value}')    
    
    
  sparkAnalyze(spark=spark,table=f'{database_target}.{table_target}')
  


	

	
#	extraction using dx tables
	
		
def dx_data_collect_using_diag(spark,\
                               table_out,\
                               column_list_out,\
                               diag_table_in,\
                               month_from,\
                               month_to,\
                               method='INITIAL',\
															 repartiton_num=1):
    
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    l_dx_gap_tab = 'prod_us9_onc.v_dx_data_gap_trans_ss'
    l_dx_tab = 'prod_us9_onc.v_dx_data_trans_ss'
    l_dx_ndw_load_dt_tab = 'prod_df2_us9.v_dx_trans_rstr'
    
    l_dx_gap_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
    
    l_dx_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,ndwx_load_dt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
        
    l_dx_all_cols ='a.claim_id,a.claim_typ_cd,a.diag_cd,a.diag_vers_typ_id,a.diag_cd_posn_nbr,a.drug_note_txt,a.plan_id,a.epoetin_dosage_amt,a.fmly_plnning_ind,a.hct_result_pct,a.hgb_hct_dt,a.hgb_result_amt,a.mammography_ctr_cert_id,a.natl_drug_unit_amt,a.ndc_cd,nvl(b.ndw_load_dt,a.ndwx_load_dt) as ndw_load_dt ,b.pat_age_mth_nbr,b.pat_age_yr_nbr,a.patient_id,a.pat_zip3_cd,a.place_of_svc_cd,a.prc_cd,a.prc_vers_typ_id,a.billing_provider_id,a.rendering_provider_id,a.referring_provider_id,a.facility_provider_id,a.rx_nbr,supplier_id,a.svc_crgd_amt,a.svc_fr_dt,a.svc_nbr,a.svc_to_dt,a.taxon_cd,a.tos_id,a.unit_of_svc_amt,a.upc_cd,a.vst_id,a.revenue_cd,a.fclt_typ_cd,a.prc_qlfr_cd,a.fclt_zip_cd,a.week_id,a.month_id'


    if str.upper(method)=='INITIAL':
      execImpala(cluster='DRML', \
             query=f""" drop table if  exists {table_out}  purge """)
      execImpala(cluster='DRML', \
             query=f""" create table if not exists {table_out} partitioned BY (month_id) stored as parquet 
             as  SELECT {column_list_out} FROM prod_us9_onc.v_dx_data_gap_trans_ss a where 1 = 0     """)
      l_month_list = get_month_list(spark,month_from,month_to)

    elif  str.upper(method)=='APPEND':
      l_month_list = get_month_list(spark,month_from,month_to)
    
        
    for l_month_id in l_month_list:      

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_dx_gap_tab_cache  = 'dx_gap_tab_cache'
      
      l_dx_tab_cache = 'dx_tab_cache'
      
      l_dx_ndw_load_dt_tab_cache = 'dx_ndw_load_dt_tab_cache' 
      
      
      
      with timer(f' cache table  {l_dx_gap_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_gap_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_gap_tab_cache}  as select {l_dx_gap_cols} from  {l_dx_gap_tab} a {l_where_time}  and exists 
                   (select null from {diag_table_in} b where a.diag_cd = b.diag_cd and a.diag_vers_typ_id = b.diag_vers_typ_id ) """)     
        
        
      with timer(f' cache table  {l_dx_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_tab_cache}  as select {l_dx_cols} from  {l_dx_tab} a {l_where_time}  and exists 
                  (select null from {diag_table_in} b where a.diag_cd = b.diag_cd and a.diag_vers_typ_id = b.diag_vers_typ_id ) """)       
        
      with timer(f' cache table  {l_dx_ndw_load_dt_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_ndw_load_dt_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_ndw_load_dt_tab_cache}  as select DIAG_CD_POSN_NBR,CLAIM_ID,SVC_NBR,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr from  {l_dx_ndw_load_dt_tab} a {l_where_time}   and exists 
                  (select null from {diag_table_in} b where a.diag_cd = b.diag_cd and a.diag_vers_typ_id = b.diag_vers_typ_id ) """)      
        
      l_query = f"""   select {l_dx_gap_cols}  from  {l_dx_gap_tab_cache} 
			               union all 
                     select {l_dx_all_cols} from  {l_dx_tab_cache} a  left outer join  {l_dx_ndw_load_dt_tab_cache} b on 
                      a.claim_id = b.claim_id   and   a.DIAG_CD_POSN_NBR = b.DIAG_CD_POSN_NBR  and  a.SVC_NBR = b.SVC_NBR
                     """  
      
      l_sdf=spark.sql(l_query)
      
      l_table_view = 'dx_out_table_view'
      
      l_sdf.dropDuplicates().repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')
      
      with timer(f'loading month: {l_month_id}'):       
        spark.sql(f""" ALTER TABLE {table_out} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into {table_out}  partition(month_id) select  {column_list_out} from {l_table_view} a""")
        
        spark.catalog.clearCache()
        
        spark.catalog.refreshTable(f'{table_out}')
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {table_out} where month_id={l_month_id}')    
        


def dx_data_collect_using_proc(spark,\
                               table_out,\
                               column_list_out,\
                               proc_table_in,\
                               month_from,\
                               month_to,\
                               method='INITIAL',\
															 repartiton_num=1):
    
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    l_dx_gap_tab = 'prod_us9_onc.v_dx_data_gap_trans_ss'
    l_dx_tab = 'prod_us9_onc.v_dx_data_trans_ss'
    l_dx_ndw_load_dt_tab = 'prod_df2_us9.v_dx_trans_rstr'
    
    l_dx_gap_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
    
    l_dx_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,ndwx_load_dt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
        
    l_dx_all_cols ='a.claim_id,a.claim_typ_cd,a.diag_cd,a.diag_vers_typ_id,a.diag_cd_posn_nbr,a.drug_note_txt,a.plan_id,a.epoetin_dosage_amt,a.fmly_plnning_ind,a.hct_result_pct,a.hgb_hct_dt,a.hgb_result_amt,a.mammography_ctr_cert_id,a.natl_drug_unit_amt,a.ndc_cd,nvl(b.ndw_load_dt,a.ndwx_load_dt) as ndw_load_dt ,b.pat_age_mth_nbr,b.pat_age_yr_nbr,a.patient_id,a.pat_zip3_cd,a.place_of_svc_cd,a.prc_cd,a.prc_vers_typ_id,a.billing_provider_id,a.rendering_provider_id,a.referring_provider_id,a.facility_provider_id,a.rx_nbr,supplier_id,a.svc_crgd_amt,a.svc_fr_dt,a.svc_nbr,a.svc_to_dt,a.taxon_cd,a.tos_id,a.unit_of_svc_amt,a.upc_cd,a.vst_id,a.revenue_cd,a.fclt_typ_cd,a.prc_qlfr_cd,a.fclt_zip_cd,a.week_id,a.month_id'


    if str.upper(method)=='INITIAL':
      execImpala(cluster='DRML', \
             query=f""" drop table if  exists {table_out}  purge """)
      execImpala(cluster='DRML', \
             query=f""" create table if not exists {table_out} partitioned BY (month_id) stored as parquet 
             as  SELECT {column_list_out} FROM prod_us9_onc.v_dx_data_gap_trans_ss a where 1 = 0     """)
      l_month_list = get_month_list(spark,month_from,month_to)

    elif  str.upper(method)=='APPEND':
      l_month_list = get_month_list(spark,month_from,month_to)
    
        
    for l_month_id in l_month_list:      

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_dx_gap_tab_cache  = 'dx_gap_tab_cache'
      
      l_dx_tab_cache = 'dx_tab_cache'
      
      l_dx_ndw_load_dt_tab_cache = 'dx_ndw_load_dt_tab_cache' 
      
      
      
      with timer(f' cache table  {l_dx_gap_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_gap_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_gap_tab_cache}  as select {l_dx_gap_cols} from  {l_dx_gap_tab} a {l_where_time}  and exists 
                   (select null from {proc_table_in} b where a.prc_cd = b.prc_cd ) """)     
        
        
      with timer(f' cache table  {l_dx_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_tab_cache}  as select {l_dx_cols} from  {l_dx_tab} a {l_where_time}  and exists 
                  (select null from {proc_table_in} b where a.prc_cd = b.prc_cd  ) """)       
        
      with timer(f' cache table  {l_dx_ndw_load_dt_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_ndw_load_dt_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_ndw_load_dt_tab_cache}  as select DIAG_CD_POSN_NBR,CLAIM_ID,SVC_NBR,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr from  {l_dx_ndw_load_dt_tab} a {l_where_time}   and exists 
                  (select null from {proc_table_in} b where a.prc_cd = b.prc_cd  ) """)      
        
      l_query = f"""   select {l_dx_gap_cols}  from  {l_dx_gap_tab_cache} 
			               union all 
                     select {l_dx_all_cols} from  {l_dx_tab_cache} a  left outer join  {l_dx_ndw_load_dt_tab_cache} b on 
                      a.claim_id = b.claim_id   and   a.DIAG_CD_POSN_NBR = b.DIAG_CD_POSN_NBR  and  a.SVC_NBR = b.SVC_NBR
                     """  
      
      l_sdf=spark.sql(l_query)
      
      l_table_view = 'dx_out_table_view'
      
      l_sdf.dropDuplicates().repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')
      
      with timer(f'loading month: {l_month_id}'):       
        spark.sql(f""" ALTER TABLE {table_out} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into {table_out}  partition(month_id) select  {column_list_out} from {l_table_view} a""")
        
        spark.catalog.clearCache()
        
        spark.catalog.refreshTable(f'{table_out}')
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {table_out} where month_id={l_month_id}')    
        
				
def dx_data_collect_using_pat(spark,\
                               table_out,\
                               column_list_out,\
                               pat_table_in,\
                               month_from,\
                               month_to,\
                               method='INITIAL',\
														   repartiton_num=1):
    
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    l_dx_gap_tab = 'prod_us9_onc.v_dx_data_gap_trans_ss'
    l_dx_tab = 'prod_us9_onc.v_dx_data_trans_ss'
    l_dx_ndw_load_dt_tab = 'prod_df2_us9.v_dx_trans_rstr'
    
    l_dx_gap_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
    
    l_dx_cols ='claim_id,claim_typ_cd,diag_cd,diag_vers_typ_id,diag_cd_posn_nbr,drug_note_txt,plan_id,epoetin_dosage_amt,ndwx_load_dt,fmly_plnning_ind,hct_result_pct,hgb_hct_dt,hgb_result_amt,mammography_ctr_cert_id,natl_drug_unit_amt,ndc_cd,patient_id,pat_zip3_cd,place_of_svc_cd,prc_cd,prc_vers_typ_id,billing_provider_id,rendering_provider_id,referring_provider_id,facility_provider_id,rx_nbr,supplier_id,svc_crgd_amt,svc_fr_dt,svc_nbr,svc_to_dt,taxon_cd,tos_id,unit_of_svc_amt,upc_cd,vst_id,revenue_cd,fclt_typ_cd,prc_qlfr_cd,fclt_zip_cd,week_id,month_id'
        
    l_dx_all_cols ='a.claim_id,a.claim_typ_cd,a.diag_cd,a.diag_vers_typ_id,a.diag_cd_posn_nbr,a.drug_note_txt,a.plan_id,a.epoetin_dosage_amt,a.fmly_plnning_ind,a.hct_result_pct,a.hgb_hct_dt,a.hgb_result_amt,a.mammography_ctr_cert_id,a.natl_drug_unit_amt,a.ndc_cd,nvl(b.ndw_load_dt,a.ndwx_load_dt) as ndw_load_dt ,b.pat_age_mth_nbr,b.pat_age_yr_nbr,a.patient_id,a.pat_zip3_cd,a.place_of_svc_cd,a.prc_cd,a.prc_vers_typ_id,a.billing_provider_id,a.rendering_provider_id,a.referring_provider_id,a.facility_provider_id,a.rx_nbr,supplier_id,a.svc_crgd_amt,a.svc_fr_dt,a.svc_nbr,a.svc_to_dt,a.taxon_cd,a.tos_id,a.unit_of_svc_amt,a.upc_cd,a.vst_id,a.revenue_cd,a.fclt_typ_cd,a.prc_qlfr_cd,a.fclt_zip_cd,a.week_id,a.month_id'


    if str.upper(method)=='INITIAL':
      execImpala(cluster='DRML', \
             query=f""" drop table if  exists {table_out}  purge """)
      execImpala(cluster='DRML', \
             query=f""" create table if not exists {table_out} partitioned BY (month_id) stored as parquet 
             as  SELECT {column_list_out} FROM prod_us9_onc.v_dx_data_gap_trans_ss a where 1 = 0     """)
      l_month_list = get_month_list(spark,month_from,month_to)

    elif  str.upper(method)=='APPEND':
      l_month_list = get_month_list(spark,month_from,month_to)
    
        
    for l_month_id in l_month_list:      

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_dx_gap_tab_cache  = 'dx_gap_tab_cache'
      
      l_dx_tab_cache = 'dx_tab_cache'
      
      l_dx_ndw_load_dt_tab_cache = 'dx_ndw_load_dt_tab_cache' 
      
      
      
      with timer(f' cache table  {l_dx_gap_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_gap_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_gap_tab_cache}  as select {l_dx_gap_cols} from  {l_dx_gap_tab} a {l_where_time}  and exists 
                   (select null from {pat_table_in} b where a.patient_id = b.patient_id ) """)     
        
        
      with timer(f' cache table  {l_dx_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_tab_cache}  as select {l_dx_cols} from  {l_dx_tab} a {l_where_time}  and exists 
                  (select null from {pat_table_in} b where a.patient_id = b.patient_id  ) """)       
        
      with timer(f' cache table  {l_dx_ndw_load_dt_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_dx_ndw_load_dt_tab_cache}""")
        spark.sql (f""" cache table  {l_dx_ndw_load_dt_tab_cache}  as select DIAG_CD_POSN_NBR,CLAIM_ID,SVC_NBR,ndw_load_dt,pat_age_mth_nbr,pat_age_yr_nbr from  {l_dx_ndw_load_dt_tab} a {l_where_time}   and exists 
                  (select null from {pat_table_in} b where a.patient_id = b.patient_id  ) """)      
        
      l_query = f"""   select {l_dx_gap_cols}  from  {l_dx_gap_tab_cache} 
			               union all 
                     select {l_dx_all_cols} from  {l_dx_tab_cache} a  left outer join  {l_dx_ndw_load_dt_tab_cache} b on 
                      a.claim_id = b.claim_id   and   a.DIAG_CD_POSN_NBR = b.DIAG_CD_POSN_NBR  and  a.SVC_NBR = b.SVC_NBR
                     """  
      
      l_sdf=spark.sql(l_query)
      
      l_table_view = 'dx_out_table_view'
      
      l_sdf.dropDuplicates().repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')
      
      with timer(f'loading month: {l_month_id}'):       
        spark.sql(f""" ALTER TABLE {table_out} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into {table_out}  partition(month_id) select   {column_list_out} from {l_table_view} a""")
        
        spark.catalog.clearCache()
        
        spark.catalog.refreshTable(f'{table_out}')
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {table_out} where month_id={l_month_id}')    

				
				
#	extraction using rx tables
	
		
def rx_data_collect_using_ndc(spark,\
                               table_out,\
                               column_list_out,\
                               ndc_table_in,\
                               month_from,\
                               month_to,\
                               method='INITIAL',\
															 repartiton_num=1):
    
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    l_rx_gap_tab = 'prod_us9_onc.v_rx_data_gap_trans_ss'
    l_rx_tab = 'prod_us9_onc.v_rx_data_trans_ss'
    l_rx_ndw_load_dt_tab = 'prod_df2_us9.v_rx_trans_rstr'
    
    l_rx_gap_cols ='auth_dt,auth_tm,auth_rfll_nbr,base_dt,bin,claim_id,cmf_otlt_nbr,cmf_prod_nbr,cmf_pack_nbr,cob_ind,copay_amt,cust_price,cyc_dt,dacon_qty,data_dt,supplier_id,daw_cd,days_supply_cnt,diag_cd,diag_vers_typ_id,dspnsd_qty,fill_nbr,cardhlder_grp_id,ingrd_cost_paid_amt,ingrd_cost_submit_amt,month_id,ncpdp_provider_id,ndc,ndw_data_use_qlfr_cd,ndw_load_dt,patient_id,ims_payer_id,ims_pln_id,ims_payer_hist_id,ims_pln_hist_id,pay_typ_cd,otlt_zip,pat_age_yr_nbr,pat_gender_cd,pat_pay_amt,pat_zip3,pln_id_brdg_cd,pcn,pharmacy_id,plan_id,product_id,provider_id,rx_dosage_amt,rx_nbr,rx_typ_cd,rxer_id,rx_written_dt,chnl_hist_cd,chnl_cd,store_nbr,store_cost_amt,svc_dt,tlc,total_paid_amt,usc_cd,usc_hist_cd,rev_shr_ind,week_id'
    
    l_rx_cols ='auth_rfll_nbr,base_dt,bin,claim_id,cmf_otlt_nbr,cmf_prod_nbr,cmf_pack_nbr,cob_ind,copay_amt,cust_price,dacon_qty,data_dt,supplier_id,daw_cd,days_supply_cnt,diag_cd,diag_vers_typ_id,dspnsd_qty,fill_nbr,cardhlder_grp_id,ingrd_cost_paid_amt,ingrd_cost_submit_amt,month_id,ncpdp_provider_id,ndc,ndwx_load_dt,patient_id,ims_payer_id,ims_pln_id,pay_typ_cd,otlt_zip,pat_gender_cd,pat_pay_amt,pat_zip3,pcn,pharmacy_id,plan_id,product_id,provider_id,rx_dosage_amt,rx_nbr,rx_typ_cd,rxer_id,rx_written_dt,chnl_cd,store_nbr,store_cost_amt,svc_dt,tlc,total_paid_amt,usc_cd,rev_shr_ind,week_id'
        
    l_rx_all_cols ="""B.AUTH_DT
		     , B.AUTH_TM
					,A.AUTH_RFLL_NBR
				, A.BASE_DT
				, A.BIN
				, A.CLAIM_ID
				, A.CMF_OTLT_NBR
				, A.CMF_PROD_NBR
				, A.CMF_PACK_NBR
				, A.COB_IND
				, A.COPAY_AMT
				, A.CUST_PRICE
			  , B.CYC_DT
				, A.DACON_QTY
				, A.DATA_DT
				, A.SUPPLIER_ID
				, A.DAW_CD
				, A.DAYS_SUPPLY_CNT
				, A.DIAG_CD
				, A.DIAG_VERS_TYP_ID
				, A.DSPNSD_QTY
				, A.FILL_NBR
				, A.CARDHLDER_GRP_ID
				, A.INGRD_COST_PAID_AMT
				, A.INGRD_COST_SUBMIT_AMT
				, A.MONTH_ID
				, A.NCPDP_PROVIDER_ID
				, A.NDC
				, B.NDW_DATA_USE_QLFR_CD
			  , nvl(
					B.NDW_LOAD_DT
					, a.ndwx_load_dt
				) as NDW_LOAD_DT				
				, A.PATIENT_ID
				, A.IMS_PAYER_ID
				, A.IMS_PLN_ID
				, B.IMS_PAYER_ID IMS_PAYER_HIST_ID
				, CAST(
					B.IMS_PLN_ID AS INT
				) IMS_PLN_HIST_ID
				, A.PAY_TYP_CD
				, A.OTLT_ZIP
				, B.PAT_AGE_YR_NBR
				, A.PAT_GENDER_CD
				, A.PAT_PAY_AMT
				, A.PAT_ZIP3
				, B.PLN_ID_BRDG_CD
				, A.PCN
				, A.PHARMACY_ID
				, A.PLAN_ID
				, A.PRODUCT_ID
				, A.PROVIDER_ID
				, A.RX_DOSAGE_AMT
				, A.RX_NBR
				, A.RX_TYP_CD
				, A.RXER_ID
				, A.RX_WRITTEN_DT
				, B.CHNL_HIST_CD
				, A.CHNL_CD
				, A.STORE_NBR
				, A.STORE_COST_AMT
				, A.SVC_DT
				, A.TLC
				, A.TOTAL_PAID_AMT
				, A.USC_CD
				, B.USC_HIST_CD
				, A.REV_SHR_IND
				, A.WEEK_ID"""


    if str.upper(method)=='INITIAL':
      execImpala(cluster='DRML', \
             query=f""" drop table if  exists {table_out}  purge """)
      execImpala(cluster='DRML', \
             query=f""" create table if not exists {table_out} partitioned BY (month_id) stored as parquet 
             as  SELECT {column_list_out} FROM prod_us9_onc.v_rx_data_gap_trans_ss a where 1 = 0     """)
      l_month_list = get_month_list(spark,month_from,month_to)

    elif  str.upper(method)=='APPEND':
      l_month_list = get_month_list(spark,month_from,month_to)
    
        
    for l_month_id in l_month_list:      

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_rx_gap_tab_cache  = 'rx_gap_tab_cache'
      
      l_rx_tab_cache = 'rx_tab_cache'
      
      l_rx_ndw_load_dt_tab_cache = 'rx_ndw_load_dt_tab_cache' 
      
      
      
      with timer(f' cache table  {l_rx_gap_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_gap_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_gap_tab_cache}  as select {l_rx_gap_cols} from  {l_rx_gap_tab} a {l_where_time}  and exists 
                   (select null from {ndc_table_in} b where a.ndc = b.ndc  ) """)     
        
        
      with timer(f' cache table  {l_rx_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_tab_cache}  as select {l_rx_cols} from  {l_rx_tab} a {l_where_time}  and exists 
                  (select null from {ndc_table_in} b where a.ndc = b.ndc ) """)       
        
      with timer(f' cache table  {l_rx_ndw_load_dt_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_ndw_load_dt_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_ndw_load_dt_tab_cache}  as select CLAIM_ID,AUTH_DT,AUTH_TM,CYC_DT,NDW_DATA_USE_QLFR_CD
				          ,NDW_LOAD_DT,IMS_PAYER_ID,IMS_PLN_ID,PAT_AGE_YR_NBR,PLN_ID_BRDG_CD,CHNL_HIST_CD,USC_HIST_CD from  
				          {l_rx_ndw_load_dt_tab} a {l_where_time}   and exists 
                  (select null from {ndc_table_in} b where a.ndc = b.ndc ) """)      
        
      l_query = f""" select {l_rx_gap_cols}  from  {l_rx_gap_tab_cache} 
			               union all 
                     select {l_rx_all_cols} from  {l_rx_tab_cache} a  left outer join  {l_rx_ndw_load_dt_tab_cache} b on 
                      a.claim_id = b.claim_id 
                     """  
      
      l_sdf=spark.sql(l_query)
      
      l_table_view = 'rx_out_table_view'
      
      l_sdf.dropDuplicates().repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')
      
      with timer(f'loading month: {l_month_id}'):       
        spark.sql(f""" ALTER TABLE {table_out} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into {table_out}  partition(month_id) select  {column_list_out} from {l_table_view} a""")
        
        spark.catalog.clearCache()
        
        spark.catalog.refreshTable(f'{table_out}')
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {table_out} where month_id={l_month_id}')    
        


				
def rx_data_collect_using_pat(spark,\
                               table_out,\
                               column_list_out,\
                               pat_table_in,\
                               month_from,\
                               month_to,\
                               method='INITIAL',\
															 repartiton_num=1):
    
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    l_rx_gap_tab = 'prod_us9_onc.v_rx_data_gap_trans_ss'
    l_rx_tab = 'prod_us9_onc.v_rx_data_trans_ss'
    l_rx_ndw_load_dt_tab = 'prod_df2_us9.v_rx_trans_rstr'
    
    l_rx_gap_cols ='auth_dt,auth_tm,auth_rfll_nbr,base_dt,bin,claim_id,cmf_otlt_nbr,cmf_prod_nbr,cmf_pack_nbr,cob_ind,copay_amt,cust_price,cyc_dt,dacon_qty,data_dt,supplier_id,daw_cd,days_supply_cnt,diag_cd,diag_vers_typ_id,dspnsd_qty,fill_nbr,cardhlder_grp_id,ingrd_cost_paid_amt,ingrd_cost_submit_amt,month_id,ncpdp_provider_id,ndc,ndw_data_use_qlfr_cd,ndw_load_dt,patient_id,ims_payer_id,ims_pln_id,ims_payer_hist_id,ims_pln_hist_id,pay_typ_cd,otlt_zip,pat_age_yr_nbr,pat_gender_cd,pat_pay_amt,pat_zip3,pln_id_brdg_cd,pcn,pharmacy_id,plan_id,product_id,provider_id,rx_dosage_amt,rx_nbr,rx_typ_cd,rxer_id,rx_written_dt,chnl_hist_cd,chnl_cd,store_nbr,store_cost_amt,svc_dt,tlc,total_paid_amt,usc_cd,usc_hist_cd,rev_shr_ind,week_id'
    
    l_rx_cols ='auth_rfll_nbr,base_dt,bin,claim_id,cmf_otlt_nbr,cmf_prod_nbr,cmf_pack_nbr,cob_ind,copay_amt,cust_price,dacon_qty,data_dt,supplier_id,daw_cd,days_supply_cnt,diag_cd,diag_vers_typ_id,dspnsd_qty,fill_nbr,cardhlder_grp_id,ingrd_cost_paid_amt,ingrd_cost_submit_amt,month_id,ncpdp_provider_id,ndc,ndwx_load_dt,patient_id,ims_payer_id,ims_pln_id,pay_typ_cd,otlt_zip,pat_gender_cd,pat_pay_amt,pat_zip3,pcn,pharmacy_id,plan_id,product_id,provider_id,rx_dosage_amt,rx_nbr,rx_typ_cd,rxer_id,rx_written_dt,chnl_cd,store_nbr,store_cost_amt,svc_dt,tlc,total_paid_amt,usc_cd,rev_shr_ind,week_id'
        
    l_rx_all_cols ="""B.AUTH_DT
		     , B.AUTH_TM
					,A.AUTH_RFLL_NBR
				, A.BASE_DT
				, A.BIN
				, A.CLAIM_ID
				, A.CMF_OTLT_NBR
				, A.CMF_PROD_NBR
				, A.CMF_PACK_NBR
				, A.COB_IND
				, A.COPAY_AMT
				, A.CUST_PRICE
			  , B.CYC_DT
				, A.DACON_QTY
				, A.DATA_DT
				, A.SUPPLIER_ID
				, A.DAW_CD
				, A.DAYS_SUPPLY_CNT
				, A.DIAG_CD
				, A.DIAG_VERS_TYP_ID
				, A.DSPNSD_QTY
				, A.FILL_NBR
				, A.CARDHLDER_GRP_ID
				, A.INGRD_COST_PAID_AMT
				, A.INGRD_COST_SUBMIT_AMT
				, A.MONTH_ID
				, A.NCPDP_PROVIDER_ID
				, A.NDC
				, B.NDW_DATA_USE_QLFR_CD
			  , nvl(
					B.NDW_LOAD_DT
					, a.ndwx_load_dt
				) as NDW_LOAD_DT				
				, A.PATIENT_ID
				, A.IMS_PAYER_ID
				, A.IMS_PLN_ID
				, B.IMS_PAYER_ID IMS_PAYER_HIST_ID
				, CAST(
					B.IMS_PLN_ID AS INT
				) IMS_PLN_HIST_ID
				, A.PAY_TYP_CD
				, A.OTLT_ZIP
				, B.PAT_AGE_YR_NBR
				, A.PAT_GENDER_CD
				, A.PAT_PAY_AMT
				, A.PAT_ZIP3
				, B.PLN_ID_BRDG_CD
				, A.PCN
				, A.PHARMACY_ID
				, A.PLAN_ID
				, A.PRODUCT_ID
				, A.PROVIDER_ID
				, A.RX_DOSAGE_AMT
				, A.RX_NBR
				, A.RX_TYP_CD
				, A.RXER_ID
				, A.RX_WRITTEN_DT
				, B.CHNL_HIST_CD
				, A.CHNL_CD
				, A.STORE_NBR
				, A.STORE_COST_AMT
				, A.SVC_DT
				, A.TLC
				, A.TOTAL_PAID_AMT
				, A.USC_CD
				, B.USC_HIST_CD
				, A.REV_SHR_IND
				, A.WEEK_ID"""


    if str.upper(method)=='INITIAL':
      execImpala(cluster='DRML', \
             query=f""" drop table if  exists {table_out}  purge """)
      execImpala(cluster='DRML', \
             query=f""" create table if not exists {table_out} partitioned BY (month_id) stored as parquet 
             as  SELECT {column_list_out} FROM prod_us9_onc.v_rx_data_gap_trans_ss a where 1 = 0     """)
      l_month_list = get_month_list(spark,month_from,month_to)

    elif  str.upper(method)=='APPEND':
      l_month_list = get_month_list(spark,month_from,month_to)
    
        
    for l_month_id in l_month_list:      

      l_begin_week_id , l_end_week_id, l_begin_svc_dt, l_end_svc_dt,l_begin_month_id,L_end_month_id, l_numworkers=get_month_week(spark=spark,month_from=l_month_id,month_to=l_month_id)
        
      l_where_time = f' where month_id  =  {l_month_id} and week_id between {l_begin_week_id} and {l_end_week_id} '

      l_rx_gap_tab_cache  = 'rx_gap_tab_cache'
      
      l_rx_tab_cache = 'rx_tab_cache'
      
      l_rx_ndw_load_dt_tab_cache = 'rx_ndw_load_dt_tab_cache' 
      
      
      
      with timer(f' cache table  {l_rx_gap_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_gap_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_gap_tab_cache}  as select {l_rx_gap_cols} from  {l_rx_gap_tab} a {l_where_time}  and exists 
                   (select null from {pat_table_in} b where a.patient_id = b.patient_id  ) """)     
        
        
      with timer(f' cache table  {l_rx_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_tab_cache}  as select {l_rx_cols} from  {l_rx_tab} a {l_where_time}  and exists 
                  (select null from {pat_table_in} b where a.patient_id = b.patient_id ) """)       
        
      with timer(f' cache table  {l_rx_ndw_load_dt_tab_cache}: {l_month_id}'):
        spark.sql (f""" drop view if exists  {l_rx_ndw_load_dt_tab_cache}""")
        spark.sql (f""" cache table  {l_rx_ndw_load_dt_tab_cache}  as select CLAIM_ID,AUTH_DT,AUTH_TM,CYC_DT,NDW_DATA_USE_QLFR_CD
				          ,NDW_LOAD_DT,IMS_PAYER_ID,IMS_PLN_ID,PAT_AGE_YR_NBR,PLN_ID_BRDG_CD,CHNL_HIST_CD,USC_HIST_CD from  
				          {l_rx_ndw_load_dt_tab} a {l_where_time}   and exists 
                  (select null from {pat_table_in} b where a.patient_id = b.patient_id ) """)      
        
      l_query = f""" select {l_rx_gap_cols}  from  {l_rx_gap_tab_cache} 
			               union all 
                     select {l_rx_all_cols} from  {l_rx_tab_cache} a  left outer join  {l_rx_ndw_load_dt_tab_cache} b on 
                      a.claim_id = b.claim_id 
                     """  
      
      l_sdf=spark.sql(l_query)
      
      l_table_view = 'rx_out_table_view'
      
      l_sdf.dropDuplicates().repartition(repartiton_num).createOrReplaceTempView(f'{l_table_view}')
      
      with timer(f'loading month: {l_month_id}'):       
        spark.sql(f""" ALTER TABLE {table_out} DROP IF EXISTS PARTITION (month_id={l_month_id})  """)
        
        spark.sql(f""" insert into {table_out}  partition(month_id) select  {column_list_out} from {l_table_view} a""")
        
        spark.catalog.clearCache()
        
        spark.catalog.refreshTable(f'{table_out}')
        
        querySparkSQL(spark=spark,query=f' select count(1) as records_{l_month_id} from {table_out} where month_id={l_month_id}')    
        

def get_begin_week_id(spark,end_week_id,interval_weeks):
  
  l_sdf=spark.sql(f""" select from_unixtime(unix_timestamp(date_sub(to_timestamp(unix_timestamp(cast({end_week_id} as string),'yyyyMMdd')),7*{interval_weeks})),'yyyyMMdd')  as beging_week_id """)
  for l_week in l_sdf.collect():
    l_week_id=l_week['beging_week_id']
  
  return l_week_id


def get_begin_month_id(spark,end_month_id,interval_months):
  
  l_sdf=spark.sql(f""" select cast(  date_format( cast(  add_months(FROM_unixtime( unix_timestamp(cast('{end_month_id}' as string),'yyyyMM') ) , -{interval_months} ) AS timestamp), 'yyyyMM')  AS int)  as beging_month_id """)
  for l_month in l_sdf.collect():
    l_month_id=l_month['beging_month_id']
  
  return l_month_id

def get_oracle_connection(HOST,DB,USER,PASSWORD,JAR='references/ojdbc7.jar'):
  
  ora_url = f"jdbc:oracle:thin:{USER}/{PASSWORD}@{HOST}/{DB}"
  conn = jaydebeapi.connect("oracle.jdbc.driver.OracleDriver", ora_url, [USER, PASSWORD], JAR)
  conn.cursor().execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 10')
  conn.cursor().execute('ALTER SESSION FORCE PARALLEL QUERY PARALLEL 10')
  
  return conn
