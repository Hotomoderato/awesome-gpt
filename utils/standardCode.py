import sys
import base64
import copy
import os
from os.path import expanduser, join, abspath
import errno
import time
from datetime import datetime as dt, date, timedelta
from pytz import timezone
#from tzlocal import get_localzone
from contextlib import contextmanager
import smtplib
import re
import io
from ftplib import FTP, FTP_TLS

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row, Window, functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import *

import pandas as pd

#first, run following to install
#!pip3 install JayDeBeApi
#!pip3 install JPype1==0.6.3 --force-reinstall
import jaydebeapi
import jpype

def pysparkInit(jdbc ='/home/cdsw/utils/ojdbc7.jar', timezone="EST"):
  """
  Initialize Python variables to run PySpark
    jdbc - Name of JDBC module to use. Default to ojdbc7.jar
           Module has to be uploaded and available in the path.
           Exception is raised when file is not found
    timezone - set to proper timezone. Default to US EST for now. Use current if None           
              If zone is not set, some Oracle Databases may refuse conection
  """
  if len(jdbc.strip()) > 0:
    if not os.path.exists(jdbc):
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), jdbc)
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ' + jdbc + ' pyspark-shell'
  os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/SUPERCONDA/envs/python3/bin/python'
  os.environ['PYSPARK_DRIVER_PYTHON'] ='/opt/cloudera/parcels/SUPERCONDA/envs/python3/bin/python'
  if len(timezone) > 0:
    os.environ['TZ'] = timezone
    time.tzset()
  

def timeStamp(inputDateTime = dt.now(),format="%Y%m%d%H%M%S"):
  """
    Convert datetime to long integer in format YYYYMMDDHH24MISS
  """
  if inputDateTime == None:
    return None
  return int(inputDateTime.strftime(format))

@contextmanager
def timer(msg, lvl = 1):
    """
    calculate elapse time. Output only if msg provided or lvl < 2.
    """
    global elapse_time
    t0 = time.time()
    yield
    elapse_time = time.time() - t0
    if msg and lvl < 2:
      print(f'[{msg}] done in {elapse_time:.2f}s.')

def thisCluster():
  clusters = {
              'usrhdpcdsw':'DRML',
              'usphdpcdsw':'BDRA'
             }
  host = os.environ['CDSW_DOMAIN'].split('.')[0]
  if host in  clusters.keys():
    return clusters[host]
  else:
    return host

def sparkInit(queue = 'sts', script = '<unknown>', log_level = 'FATAL'):
  """
Start Spark session, return SparkSession created. Set some parameters for optimal performance.
  Always print spark application id so could be used to debug errors.
  
  conf    - Optional, configuration to use. Default only sets queue
  script  - Optional, name of the script run
  """
  try:
    conf = SparkConf() 
    conf.set("spark.app.name", f"{os.environ['CDSW_PROJECT']} Script: {script}")
    conf.set("spark.yarn.queue", queue)
    conf.set("spark.sql.execution.arrow.enabled", "false")
    conf.set("spark.executor.memory", "12g") 
    conf.set("spark.yarn.executor.memoryOverhead", "3g") 
    conf.set("spark.dynamicAllocation.initialExecutors", "2") 
    conf.set("spark.driver.memory", "16g") 
    conf.set("spark.kryoserializer.buffer.max", "1g") 
    conf.set("spark.driver.cores", "32") 
    conf.set("spark.executor.cores", "8") 
    conf.set("spark.dynamicAllocation.maxExecutors", "32")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.sql.catalogImplementation", "hive")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel(log_level)
    ret_spark = SparkSession(sc).builder.getOrCreate()
    print(f"SPARK AppId {ret_spark._sc.applicationId}")  #always print spark application id when start
    if log_level not in ('FATAL','NONE'):
      for (k,y) in sorted(ret_spark._sc._conf.getAll()):
        print(f"{k.ljust(50)}  -> {y}")
     
  except Exception as e:
    ret_spark = None
    if log_level not in ('FATAL','NONE'):
      print(e)
    raise e
  finally:
    return ret_spark

def getBdfDataFrame(spark, input_obj):
  r"""
    getBdfDataFrame -- returns DataFrame if input either string or DataFrame 
    spark -- SparkSession
    input_obj -- if string, either table name in format [database.]table. If database name is not provided
                check if prod_sts is available. if not, uses default database.
                if string is a query (starts with SELECT or WITH keyword), just use it
  """
  bdf_df =None
  if isinstance(input_obj,str):
    bdf_tbl=''
    bdf_db =''
    clean_str=re.sub('/\*.*?\*/', ' ', input_obj, 0, re.S).strip()
    clean_str=re.sub('--.*$', ' ', clean_str, 0,re.M).strip()
    is_query = re.match('^(SELECT|WITH)\s+',clean_str, re.I) != None
    #print(re.match('^(SELECT|WITH)\s+',clean_str, re.I))
    #print(re.match('^(SELECT|WITH)\s+',clean_str, re.I) == None)
    dflt_db = 'prod_sts' if spark.sql("show databases like 'prod_sts'").count() > 0 else spark.catalog.currentDatabase()
    if is_query:
      #print('query')
      bdf_df = spark.sql(input_obj)
    else:
      #print('table')
      names = clean_str.strip('`').split('.')
      bdf_tbl = names[-1]
      bdf_db = names[0] if len(names) > 1 else dflt_db
      bdf_df = spark.table(f"{bdf_db}.{bdf_tbl}")
  elif isinstance(input_obj,DataFrame):
    bdf_df = input_obj
  return bdf_df
  
def oracleTNS(file='/home/cdsw/utils/tnsnames.ora',verbose_level='ERROR'):
  r"""
    oracleTNS -- get map from oracle server names to JDBC connection string
    
    file          -- file path to tnsnames.ora. has to be accesible. Default to tnsnames.ora
    verbose_level -- set how many messages in session log you want. Default to ERROR
                     Uses log4j names: DEBUG, INFO, WARNING, ERROR, FATAL and NONE
  """
  
  verbose_map = {'DEBUG': 0, 'INFO':1,'WARNING':2,'ERROR':3,'FATAL':4,'NONE':5}
  log_lvl = verbose_map[verbose_level.upper().strip()]
  
  file1 = open(file, 'r') 
  tnsnames_map = {}
  comment_ct = 0
  lines = file1.readlines()
  for line in lines: 
    no_comment_line = re.sub('#.*','',line).strip()
    db_link=''
    host=''
    port=''
    service_name=''
    sid_name=''
    if len(no_comment_line) == 0:
      comment_ct += 1
    else:
      db_link = re.match('^\s*([^=]+)\s*=',no_comment_line).group(1).strip()
      if db_link == None:
        continue;
      math_obj = re.search('\(\s*HOST\s*=\s*([^)]+)\s*\)',no_comment_line)
      if math_obj != None:
        host = math_obj.group(1).strip()
      math_obj = re.search('\(\s*PORT\s*=\s*([^)]+)\s*\)',no_comment_line)
      if math_obj != None:
        port = math_obj.group(1).strip()
      math_obj = re.search('\(\s*(SERVICE_NAME|SID)\s*=\s*([^)]+)\s*\)',no_comment_line)
      if math_obj != None:
        service_name = math_obj.group(2).strip()
      math_obj = re.search('\(\s*SID\s*=\s*([^)]+)\s*\)',no_comment_line)
      if math_obj != None:
        sid_name = math_obj.group(1).strip()
      if service_name == None and sid_name != None:
        service_name = sid_name
      if service_name == None and db_link != None:
        service_name = db_link
        if log_lvl < 3:
          print(f'{db_link}: WARNING: Substituted missing service name with TNS name')
      if None in [db_link, host, port, service_name]:
        if log_lvl < 4:
          print(f'{db_link}: ERROR: One of required parts is missing: HOST {host}, PORT {port} or SID {service_name}')
        continue
      if db_link in tnsnames_map:
        if log_lvl < 3:
          print(f'{db_link}: WARNING: Duplicate entry {host}:{port}/{service_name} ignored')
        continue
      if log_lvl < 2:
        print(f'Map {db_link} -> {host}:{port}/{service_name}')
      tnsnames_map[db_link] = f"{host}:{port}/{service_name}"

  if log_lvl < 1:
    print(log_lvl)

  if log_lvl < 5:
    print(f'{len(tnsnames_map)} out of {len(lines)} mapped; {comment_ct} commented lines ignored')
    
  return tnsnames_map
  
def queryOracle(spark,user,query,server=None,pwd=None,lowercase=True):
  r"""
    Run query in oracle and return dataframe.
      spark  - required, Spark Session object
      user   - required, Oracle Schema / User name
      server - optional, Oracle Server. Default to PAA
      pwd    - optional, Oracle schema password. Default to first word in user name
      lowercase - convert column names to lowercase after load. Deafualt to true
  """
  #ORACLE_SERVERS = {"PAA":"cdtspaa-scan.rxcorp.com:1521"}
  passwd = pwd
  if passwd == None:
    passwd = user.split('_')[0]
  real_server = server
  if real_server == None:
    real_server = 'PAA'
  ORACLE_URL = f"jdbc:oracle:thin:{user}/{passwd}@{oracleTNS(verbose_level='NONE')[real_server]}"
  print(ORACLE_URL)
  ret_df = spark.read.format('jdbc') \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .option("url", ORACLE_URL) \
      .option("dbtable", query)\
      .load()
  if lowercase:     
    ret_df = ret_df.select(*(f.lower() for f in ret_df.columns))
  return ret_df


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
  
  if len(res_str) > 0:
    return pd.read_csv(io.StringIO(res_str))
  else:
    return None

  
def sendEmail(body = 'Test', subject = 'CDSW Alert', to = os.getenv('GIT_AUTHOR_EMAIL'), sender = os.getenv('GIT_AUTHOR_EMAIL'), pwd = None):
  """
    Send email If password provided, uses Outlook365. Uses internal server if not.  
    
      body    - Optional, message body, could be empty to test, for example.  
      subject - Optional, Subject line. Default to 'CDSW Alert'.  
      to      - Optional recipient. if not set, send to currently logged in user.  
      sender  - Optional: if Outlook 365 is used than only valid email; default to current user. Otherwise app@cluster is default.  
      pwd     - Optional, scrambled password. Use base64.b64encode(real_pwd) to produce this string.  
  """
  app_sender = f"{os.environ['CDSW_PROJECT']}@{thisCluster()}"
  if pwd == None:
    smtpsrv = 'intemail.rxcorp.com'
    smtpserver = smtplib.SMTP(smtpsrv)
    msgbody = f'To:{to}\nFrom:{app_sender}\nSubject:{subject}\n\n\n{body}'
    smtpserver.sendmail(sender, to, msgbody)
    smtpserver.close()
  else:
    password = base64.b64decode(pwd).decode("utf-8")
    smtpsrv = "smtp.office365.com"
    smtpserver = smtplib.SMTP(smtpsrv,587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo()
    smtpserver.login(sender, password)
    msgbody = f'To:{to}\nFrom:{sender}\nSubject:{subject}\n\n\n{body}'
    smtpserver.sendmail(sender, to, msgbody)
    smtpserver.close()

def sparkType(f):
  r"""
    Internal. Map pandas type to python
  """
  if f == 'datetime64[ns]': return DateType()
  elif f == 'int64': return LongType()
  elif f == 'int32': return IntegerType()
  elif f == 'float64': return FloatType()
  else: return StringType()

def defineStructure(string, format_type):
  r"""
    Internal Return python type for column
    string      - Column Name
    format_type - Column Type
  """
  try: typo = sparkType(format_type)
  except: typo = StringType()
  return StructField(string, typo)

def pandasToSpark(sqlCtx, pandas_df, verbose_lvl = 'WARNING'):
  r"""
    Convert pandas DataFrame to spark for given context
      sqlCtx    - Spark SQL Context from session
      pandas_df - Pandas DataFrame
    
  """
  #verify verbose_lvl
  all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
  out_lvl = all_lvls[verbose_lvl] if verbose_lvl in all_lvls else 1 #ignore invalid levels
  if out_lvl < 2: print(f"Output verbose level requested: {verbose_lvl}; actual {out_lvl}")

  columns = list(pandas_df.columns)
  types = list(pandas_df.dtypes)
  struct_list = []
  for column, typo in zip(columns, types): 
    struct_list.append(defineStructure(column, typo))
  if out_lvl < 2:
    print(struct_list)
  p_schema = StructType(struct_list)
  return sqlCtx.createDataFrame(pandas_df, p_schema)

def sparkAnalyze(spark, table):
  spark.catalog.refreshTable(table)
  spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS")
  execImpala(f"invalidate metadata {table}")
  execImpala(f"compute stats {table}")
  
def sparkSave(spark, table, sql, partition = None, format = 'parquet', mode='append',database='prod_sts', verbose_lvl = 'INFO', analyze = False):
  r"""
    Create or insert into Hive table. returns DataFrame with query results to use further in the process
      table     - Required table name
      sql       - Required SQL to create table
      partition - Optional, partition columns. No partitions by default
      format    - Optiona table format. Default or null to parquet. 
      mode      - Optional, how to insert. defaul to append.
      database  - Database to save to. Default to prod_sts, currently in use if null.
      verbose_lvl - Contorl how much progress report to output: DEBUG,INFO,WARNING,ERROR,FATAL,NONE
      analyze - Set to True if you want table to be analyzed
    
  """
  #verify verbose_lvl
  all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
  out_lvl = all_lvls[verbose_lvl] if verbose_lvl in all_lvls else 1 #ignore invalid levels
  if out_lvl < 2: print(f"Output verbose level requested: {verbose_lvl}; actual {out_lvl}")
  
  v_return = getBdfDataFrame(spark,sql)
  with timer(msg = f"Save {database}.{table}" if out_lvl < 2 else None):
    if partition != None and partition in v_return.columns:
      v_return.write.mode(mode).format(format).partitionBy(partition).saveAsTable(f"{database}.{table}")
    else:
      v_return.write.mode(mode).format(format).saveAsTable(f"{database}.{table}")
  if analyze:
    with timer(msg = f"Analyze {database}.{table} in HIVE" if out_lvl < 2 else None):
      spark.catalog.refreshTable(f"{database}.{table}")
      if partition != None and partition in v_return.columns:
        spark.sql(f"ANALYZE TABLE {database}.{table} partition({partition}) COMPUTE STATISTICS")
      else:
        spark.sql(f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS")

    with timer(msg = f"Analyze {database}.{table} in Impala" if out_lvl < 2 else None):
      execImpala(f"invalidate metadata {database}.{table}")
      execImpala(f"compute stats {database}.{table}")

def oracleSave(spark, table, sql, partition = None, format = None, mode = 'overwrite', verbose_lvl = 'INFO'):
  r"""
    Create or append to table in Oracle. Table created is not exists
      spark  - required, Spark Session object
      table -- Oracle table name in format <schema>[/password].<table_name>[@server]
                Password defaults to first non-underscore characters in schema name
                Server defaults to PAA
      sql   -- Either SQL or dataframe or BDF table name in format [database.]table_name.
                database defaults to prod_sts if accessible; otherwise, uses default database
      partition -- Not implemented yet, list of columns for partition and subpartition
      format -- Not implemented yet, specify what type of partitions and other physical parameters, like parallel and such
      mode  -- overwrite, refresh or append
      verbose_lvl -- Session log output level. Uses log4j values. Default to INFO
  """
  #verify verbose_lvl
  all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
  out_lvl = all_lvls[verbose_lvl] if verbose_lvl in all_lvls else 1 #ignore invalid levels
  if out_lvl < 1: print(f"Output verbose level requested: {verbose_lvl}; actual {out_lvl}")
  
  try:
    ora_user    = table.split('.')[0].split('/')[0].upper().strip()
    project     = table.split('.')[0].split('/')[1].upper().strip() if len(table.split('.')[0].split('/')) > 1 else ora_user.split('_')[0].upper().strip()
    ora_server  = table.split('@')[1].upper().strip() if len(table.split('@')) > 1 else 'PAA'
    ora_table   = table.split('.')[1].split('@')[0].upper().strip()
    ora_url     = f"jdbc:oracle:thin:{ora_user}/{project}@{oracleTNS(verbose_level='NONE')[ora_server]}"
    if out_lvl == 0: print(f"Oracle target: {ora_user}/{project}.{ora_table}@{ora_server}")
    elif out_lvl == 1: print(f"Oracle target: {ora_user}.{ora_table}@{ora_server}")
  except Exception as e:
    raise ValueError(f"""table '{table}' is not in proper format; {str(e)}""")

  try:
    ora_conn = oracleConnect(ora_user,ora_server,project, verbose_lvl)
    oracleExec(ora_conn,f"drop table {ora_table} purge")
    oracleDisconnect(ora_conn)
  except jaydebeapi.DatabaseError as e:
    if out_lvl < 3: 
      print(str(e).split(':')[2].strip())
  
  df_tmp = getBdfDataFrame(spark, sql)
  df_ora = df_tmp.select(*(f.upper() for f in df_tmp.columns))
  if out_lvl < 2:
    with timer(msg = f"Copy {str(sql)} to Oracle {ora_user}.{ora_table}@{ora_server}" if out_lvl < 2 else None):
      df_ora.write.format('jdbc').option("driver", "oracle.jdbc.driver.OracleDriver")\
            .option("url", ora_url).option("dbtable", ora_table).save()
  else:
    df_ora.write.format('jdbc').option("driver", "oracle.jdbc.driver.OracleDriver")\
          .option("url", ora_url).option("dbtable", ora_table).save()
  
def copyTable(spark, source, target = None, partition = None, format = 'parquet'):
  r"""
    Streamlined table copy table from one source to another. 
    Currently supports only from oracle into BDF.
      spark     - Required spark connection object, returned by sparkInit()
      source    - Required Oracle source table name in format 'schema[/password].table[@server]'
                  Schema and table names are required;
                  Server defaults to PAA; 
                  Password defaults to first non underschore characters in schema name
      target    - Optional, target table name in format 'database.table'
                  If none, default to prod_sts as a database and uses same table name as original oracle table.
                  if no dots, check if the name provided is existing database. 
                  if yes, uses it as a database name and original oracle table name
                  if not, database defaults to 'prod_sts' and uses this string as table name.
      partition - Optional, partition columns. No partitions by default.
      format    - Optiona table format. Default or null to parquet. 
  
  """
  pi_user = None
  pi_pwd = None
  pi_table = None
  pi_server = None
  pi_database = None
  pi_target = None
  pi_default_db = None
  err = "Source string should be in format schema[/password].table[@server]; password and server are optional"
  warn = ''
  pi_pattern = "^(((t|u)"+os.environ['HADOOP_USER_NAME'].lower()+")|(b|i|s)\d{5,8})_"
  pi_cluster = thisCluster()
  
  if pi_cluster == 'BDRA':
    pi_default_db ='prod_sts'
  elif pi_cluster == 'DRML':
    pi_default_db ='prod_df2_us9_dpt'

  with timer(f"Parse source '{source}'"):
    tmp_list = re.split('[.]', source)

    if tmp_list == None:
      raise ValueError(err)

    if len(tmp_list) != 2:
      raise ValueError(err)

    tmp_list1=re.split('[/]', tmp_list[0])

    if len(tmp_list1) > 2:
      raise ValueError(err)

    if len(tmp_list1) == 2:
      pi_pwd = tmp_list1[1].strip().upper()

    pi_user = tmp_list1[0].strip().upper()

    tmp_list1=re.split('[@]', tmp_list[1])

    if len(tmp_list1) > 2:
      raise ValueError(err)

    if len(tmp_list1) == 2:
      pi_server = tmp_list1[1].strip().upper()

    pi_table = tmp_list1[0].strip().upper()

  with timer(f"Parse target '{target}'"):
    if target == None:
      if re.match(pi_pattern,pi_table.lower()):
        pi_target = pi_table.lower()
      elif re.match(pi_pattern,pi_user.lower()):
        pi_target = re.match(pi_pattern,pi_user.lower())[0] + pi_table.lower()
      pi_database = pi_default_db
    else:
      tmp_list = re.split('[.]', target)

      if len(tmp_list) > 2:
        raise ValueError(err)

      if tmp_list == None:
        if re.match(pi_pattern,pi_table.lower()):
          pi_target = pi_table.lower()
        elif re.match(pi_pattern,pi_user.lower()):
          pi_target = re.match(pi_pattern,pi_user.lower())[0] + pi_table.lower()
        pi_database = pi_default_db
      else:
        if len(tmp_list) == 2:
          pi_database = tmp_list[0].strip().lower()
          pi_target = tmp_list[1].strip().lower()
        elif len(tmp_list) == 1:
          tmpDF = spark.sql(f"show databases like '{tmp_list[0].strip().lower()}'")
          if tmpDF.count() == 1:
            pi_database = tmp_list[0].strip().lower()
            pi_target = pi_table.lower()
          else:
            pi_database = pi_default_db
            pi_target = tmp_list[0].strip().lower()

  if pi_server == None:
    pi_server = 'PAA'
    
  with timer(f"Load from Oracle {pi_user}.{pi_table}@{pi_server}"):
    srcDF = queryOracle(spark,
                        pi_user,
                        f"(SELECT * FROM {pi_table})",
                        pi_server,
                        pi_pwd)
    
  with timer(f"Save to {pi_database}.{pi_target}@{thisCluster()}"):
    spark.sql(f"drop  table if exists {pi_database}.{pi_target} purge")
    if partition != None and partition in v_return.columns:
      srcDF.write.mode('overwrite').format(format).partitionBy(partition).saveAsTable(f"{pi_database}.{pi_target}")
    else:
      srcDF.write.mode('overwrite').saveAsTable(f"{pi_database}.{pi_target}")

  with timer(f"HIVE: Analyze and update stats"):
    spark.catalog.refreshTable(f"{pi_database}.{pi_target}")
    
    if partition != None:
      spark.sql(f"ANALYZE TABLE {pi_database}.{pi_target} partition({partition}) COMPUTE STATISTICS")
    else:
      spark.sql(f"ANALYZE TABLE {pi_database}.{pi_target} COMPUTE STATISTICS")


  with timer(f"Impala: Analyze and update stats"):
    execImpala(f"invalidate metadata {pi_database}.{pi_target}")
    execImpala(f"compute stats {pi_database}.{pi_target}")
  
  if re.match(pi_pattern,pi_target.lower()) == None and pi_database == pi_default_db:
    print("""
WARNING: Non-AA standard target table name is used. Be advised that such tables could be removed as non-compliant.
AA BDF table name standard described at https://jiraims.rm.imshealth.com/wiki/display/AAUS/General+Coding+standards#GeneralCodingstandards-Naming
""")
    
def oracleConnect(schema,server='PAA',pwd=None, verbose_lvl = 'INFO'):
  #verify verbose_lvl
  all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
  out_lvl = all_lvls[verbose_lvl] if verbose_lvl in all_lvls else 1 #ignore invalid levels
  if out_lvl < 1: print(f"Output verbose level requested: {verbose_lvl}; actual {out_lvl}")
  jar=os.getcwd()+'/utils/ojdbc7.jar'
  
  if jpype.isJVMStarted() == False:
    args = f'-Djava.class.path={jar}'
    jvm_path = jpype.getDefaultJVMPath()
    #run below only onve per session if need another java classes, there are different function for that
    #jpype.startJVM(jvm_path, args)
    jpype.startJVM(jvm_path, args, convertStrings = False)
  elif out_lvl < 2:
    print("oracle already inited, call ignored. Only call ones per session")

  try:
    password = pwd
    if password == None:
      password = schema.split('_')[0].upper().strip() if '_' in schema else schema
    ora_url     = f"jdbc:oracle:thin:{schema}/{password}@{oracleTNS(verbose_level='NONE')[server]}"
    if out_lvl < 2:
      print(f"Oracle URL: {ora_url}")
      
  except Exception as e:
    raise ValueError(f"""Input '{schema} @ {server}' is not proper; {str(e)}""")
    
  #run some session setting to be fast
  conn = jaydebeapi.connect("oracle.jdbc.driver.OracleDriver", ora_url, [schema, password], jar)
  curs = conn.cursor()
  curs.execute('ALTER SESSION SET PARALLEL_FORCE_LOCAL=TRUE')
  return conn

def oracleDisconnect(conn):
  if conn:
    conn.close()  

def oracleExec(ora_conn, sql):
  r"""
    Execute oracle statemetn without returning anything..
      ora_conn  - Oracle Connection object, result of oracleConnect()
      sql       - Statement to run
  """
  curs = ora_conn.cursor()
  curs.execute(sql)
  curs.close()

def oracleSelect(ora_conn, sql):
  r"""
    Run query in oracle and return pandas DataFrame. Uses existing connection so fast init.
    Do not recommend for huge datasets since pandas limited by local memeory.
    For huge data sets, use queryOracle, it returns spark DataFrame
      ora_conn  - Oracle Connection object, result of oracleConnect()
      sql       - Query to run
  """
  curs = ora_conn.cursor()
  curs.execute(sql)
  ret = pd.DataFrame(curs.fetchall(), columns=[str(x[0]) for x in curs.description])
  curs.close()
  return ret

def ftp2Panda(ftp, filename, sep = ",", verbose_lvl = 'FATAL'):
  f"""
ftp2Panda -- Load file from given FTP connection into Panda DataFrame.  
Return None object if failed for any reason.

Parameters:
  ftp           -- FTP connection object, returned from ftplib.FTP method. 
  filename      -- Name of file to load into Panda
  sep           -- field separator. Default to coma.
  verbose_lvl   -- set how many messages in session log you want. Default to INFO
                   Uses log4j names: DEBUG, INFO, WARNING, ERROR, FATAL and NONE
  """

  all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
  out_lvl = all_lvls[verbose_lvl] if verbose_lvl in all_lvls else 1 #ignore invalid levels
  if out_lvl < 1: print(f"Output verbose level requested: {verbose_lvl}; actual {out_lvl}")
    
  df_ret = None

  with timer(f"FTP GE {filename}",out_lvl):
    sio = io.BytesIO()
    def writeFunc(s):
        sio.write(s)

    sio.truncate(0)
    sio.seek(0)
    try:
      ftp.retrbinary(f"RETR {filename}", writeFunc)
    except Exception as e:
      if out_lvl < 5: 
        print(f"FTP GET failed with {str(e)}")
      return df_ret

  with timer(f"Copy to Pandas DataFrame",out_lvl):
    df_ret = pd.read_csv(io.StringIO(sio.getvalue().decode('ascii')), sep,low_memory=False)

  sio.truncate(0)
  sio.seek(0)
  return df_ret


#preliminary work after this point
#ftp
import urllib
import requests

#openFTP('secureftp.imshealth.com', 'T1835', 'G@&9hweS', 21, 'In')
def openFTP(server, user, password, port, folder):
  svr_url = urllib.parse.quote_plus(server)
  usr_url = urllib.parse.quote_plus(user)
  pwd_url = urllib.parse.quote_plus(password)
  prt_url = urllib.parse.quote_plus(str(port))
  dir_url = urllib.parse.quote_plus(folder)
  return requests.get(f"ftp://{usr_url}:{pwd_url}@{svr_url}:{prt_url}/{dir_url}") 

