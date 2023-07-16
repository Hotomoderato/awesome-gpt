from utils.standardCode import *

########################################
## Candidates for general standard code
########################################
import math
import inspect

int_len = lambda z: int(math.log10(z))+1

class aa_sys:
  r"""
Various system helper methods, not readily available in Python or CDSW environment
"""
  @property
  def call_level(self):
    r"""call_level - call depth of current method. 0 if method called from comand line"""
    max_y = 0
    for y, x in enumerate(inspect.stack()):
      if x.function == '<module>' and x.filename.find("<ipython-input") >= 0:
        max_y = y
    return max(0, max_y - 1)

class aa_log:
  r"""
Set trace level, depending on call depth. Just put it as first line in module:

def myFunc(..., log_lvl):
  plog = aa_log(log_lvl)

log_lvl - Contorl how much progress report to output: DEBUG,INFO,WARNING,ERROR,FATAL,NONE
          Optional depth level after semicolon. if module called at the deeper level, trace bumped 1 level up.
          For example, if module A uses INFO level, then Module B, called from within module A will be at WARNING level
          Use it to determine how much messages to output.
          Default to INFO:1
"""
  def __init__(self,in_lvl = "INFO:1"):
    all_lvls = {'DEBUG':0,'INFO':1, 'WARNING':2, 'ERROR':3, 'FATAL':4, 'NONE':5}
    out_lvl = all_lvls[in_lvl.split(":")[0]] if in_lvl.split(":")[0] in all_lvls else 1 #ignore invalid levels
    depth_lvl = int(in_lvl.split(":")[1].upper().replace("ALL",'1000000')) if in_lvl.find(":") >= 0 else 1
    if depth_lvl < max(0, aa_sys().call_level - 1):
      out_lvl +=1
    self.lvl = out_lvl
    if out_lvl < 2 and aa_sys().call_level > 1:
      print(f"Output verbose level requested: {in_lvl}; actual {str(self)}; depth: {depth_lvl}; current call_level: {aa_sys().call_level}")
  
  def DEBUG(self, in_str):
    if self.lvl == 0: print(str(in_str))

  def INFO(self, in_str):
    if self.lvl <= 1: print(str(in_str))

  def WARNING(self, in_str):
    if self.lvl <= 2: print(str(in_str))

  def ERROR(self, in_str):
    if self.lvl <= 3: print(str(in_str))

  def FATAL(self, in_str):
    if self.lvl <= 4: print(str(in_str))

  def __str__(self):
    rev_lvls = {0:'DEBUG',1:'INFO', 2:'WARNING', 3:'ERROR', 4:'FATAL', 5:'NONE'}
    return str(rev_lvls[min(self.lvl, 5)])

# TODO: add operators
class aa_date:
  def __init__(self,in_var = dt.now()):
    self.source=in_var

  @property
  def datetime(self):
    if isinstance(self.source,dt):
      return self.source
    elif isinstance(self.source,int):
      if int_len(self.source) == 4:
        return dt(self.source,1,1)
      elif int_len(self.source) == 6:
        return dt(self.source//100,self.source%100,1)
      elif int_len(self.source) == 8:
        return dt(self.source//10000,self.source%10000//100,self.source%100)
      self.source = in_var
    elif isinstance(self.source,str):
      return None #not implemented yet self.source = in_var
    elif isinstance(self.source,float):
      return dt.fromtimestamp(self.source)
    elif isinstance(self.source,date):
      return dt(self.source.year, self.source.month, self.source.day)
#    elif isinstance(self.source,aa_date):
#      return self.source.datetime
    elif self.source == None:
      return self.source
    else:
      return None
      
  @property
  def date(self):
    if self.datetime != None:
      return self.datetime.date()
    else:
      return None
      
  @property
  def date_id(self):
    if self.datetime != None:
      return self.datetime.year * 10000 + self.datetime.month * 100 + self.datetime.day
    else: 
      return None 

  @property
  def week_id(self):
    if self.datetime != None:
      return aa_date(self.datetime + timedelta( (4 - self.datetime.weekday()) % 7 )).date_id
    else: 
      return None 

  @property
  def month_id(self):
    if self.datetime != None:
      return self.datetime.year * 100 + self.datetime.month
    else: 
      return None 

  @property
  def year_id(self):
    if self.datetime != None:
      return self.datetime.year
    else: 
      return None 

  @property
  def first(self):
    return self

  @property
  def last(self):
    if self.datetime != None:
      if isinstance(self.source,int):
        if int_len(self.source) == 4:
          return aa_date(dt(self.source,12,31))
        elif int_len(self.source) == 6:
          return aa_date(dt(self.source//100,self.source%100, ((self.datetime.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).day))
        elif int_len(self.source) == 8:
          return self
      else:
        return self
    else: 
      return None 

  @property
  def first_id(self):
    return self.date_id

  @property
  def last_id(self):
    return self.last.date_id
  
  @property
  def bdf_sql_str(self):
    return f"from_unixtime(unix_timestamp('{self.date_id}' ,'yyyyMMdd'))"

  @property
  def ora_sql_str(self):
    return f"to_date('{self.date_id}', 'YYYYMMDD')"

  def add(self, months = 0, weeks=0, days=0):
    curr_datetime = self.datetime
    if curr_datetime == None:
      return None 
    
    if months != 0:
      new_month = (curr_datetime.month + months)%12
      new_year = curr_datetime.year + (curr_datetime.month + months)//12
      if new_month == 0: 
        new_month = 12
        new_year-=1

      new_month_next = (new_month + 1)%12
      new_year_next = new_year + (new_month + 1)//12
      if new_month_next == 0: 
        new_month_next = 12
        new_year_next-=1

      max_date = (dt(new_year_next,new_month_next,1) - dt(new_year,new_month,1)).days
      new_datetime = dt(new_year,
                        new_month,
                        min(max_date,curr_datetime.day),
                        curr_datetime.hour, 
                        curr_datetime.minute, 
                        curr_datetime.second, 
                        curr_datetime.microsecond,
                        curr_datetime.tzinfo)
      return aa_date(new_datetime)
    elif weeks != 0:
      return aa_date(curr_datetime + timedelta(days=round(weeks*7)))
    elif days != 0:
      return aa_date(curr_datetime + timedelta(days=days))
    else:
      return aa_date(curr_datetime)
  
  def months_between(self, other):
    if isinstance(other,aa_date):
      norm_dt = other.datetime
    else:
      norm_dt = aa_date(other).datetime 
    if norm_dt == None or self.datetime == None:
      return None
    else:
      oth_fix = norm_dt + timedelta(days=1)
      cur_fix = self.datetime + timedelta(days=1)
      return (cur_fix.year - oth_fix.year)*12 + cur_fix.month - oth_fix.month + (cur_fix.day - oth_fix.day)/31
    
  def stamp(self, informat = "%Y%m%d%H%M%S"):
    if self.datetime == None:
      return None
    return int(self.datetime.strftime(format))
  
  def __str__(self):
     return str(self.datetime)

def timePeriods(start,end,types='M',lookback=12, lookfward=1,latestmth=None):
  r"""
Generate DataFrame with time periods
  """
  start_lst = list()
  lookback_lst = list()
  lookfward_lst = list()  
  id_lst = list()
  type_lst = list()
  for y, x in enumerate(pd.date_range(aa_date(start).first.datetime, aa_date(end).last.datetime,  freq=types), start=1):
    start_lst.append(aa_date(x).month_id)
    lookback_lst.append(aa_date(x).add(1-lookback).month_id)
    tmp=aa_date(x).add(lookfward).month_id
    if tmp>latestmth:
      lkf = latestmth
    else:
      lkf=tmp
    lookfward_lst.append(lkf)    
    id_lst.append(y)
    type_lst.append(types)
  return pd.DataFrame(list(zip(id_lst,type_lst,start_lst,lookback_lst,lookfward_lst)),index = id_lst, columns =['cohort', 'type', 'start', 'lookback','lookfward'])

########################################
## Standard Panel modules
########################################
def pharmacyStability(spark, start, end, cohort = None, output = None, time_share = 100, log_lvl = 'INFO:1'):
  r"""
  Return spark DataFrame with Stable Pharmacipanels for given cohort.
  spark       -- Spark connection.
  start       -- Stability start date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
  end         -- Stability end date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
  output      -- If not empty, persist DataFrame into BDF with the name. Format [database.]table_name. 
                 Create partition for each cohort; if overwrites cohort data if exists. 
  cohort      -- Cohort Id to use. if not set, month id for the start
  time_share- -- For multi period cohorts, determine how many period pharmacy have to be
                 eligible, in percent from 1 to 100. Default is 100
  log_lvl -- Contorl how much progress report to output: DEBUG,INFO,WARNING,ERROR,FATAL,NONE
  """
  logging = aa_log(log_lvl)
  panel_start = None
  panel_end = None
  real_cohort_id = None
  real_time_share = None
  errors = list()
  warnings = list()

  with timer("Validate Parameters", logging.lvl):
    for row in spark.sql("select min(week_id) as min_week_id, max(data_refresh_ind * week_id) as max_week_id from prod_df2_us9.v_pep_wk").rdd.toLocalIterator():
      pep_start = row["min_week_id"]
      pep_end = row["max_week_id"]
      break

    panel_start = aa_date(start).first
    panel_end   = aa_date(end).last
    
    if panel_end.date_id < panel_start.date_id:
      panel_start = aa_date(end).first
      panel_end   = aa_date(start).last
      warnings.append("Study start and end date switched") 

    if cohort == None:
      real_cohort_id = real_panel_start.month_id
      warnings.append("Empty Cohort id set to start month id") 
    elif isinstance(cohort, int) or isinstance(cohort, str):
      real_cohort_id = int(cohort)
    else:
      real_cohort_id = real_panel_start.month_id
      warnings.append("Invalid Cohort id set to start month id") 
      
    if cohort == None:
      real_time_share = 100
      warnings.append("Empty time_share ignored") 
    elif isinstance(cohort, float) or isinstance(cohort, int) or isinstance(cohort, str):
      real_time_share = float(time_share)
      if real_time_share < 0:
        real_time_share = 0
        warnings.append("Negative time share set to 0") 
      if real_time_share > 100:
        real_time_share = 0
        warnings.append("Truncated time share to 100") 
    else:
      real_time_share = 100
      warnings.append("Invalid time_share ignored") 

    if logging.lvl < 2:
      print(f"Panel range: {pep_start} - {pep_end}")
      print(f"Input range: {start} - {end}")
      print(f"Study range: {panel_start.week_id} - {panel_end.week_id}")
      print(f"Cohort:      {cohort} -> {real_cohort_id}")
      print(f"Time share:  {time_share} -> {real_time_share}")
    
    if logging.lvl < 3:
      for x in warnings:
        print(x);

    #add check if other parameters types are valid also catch exception if time periods are icorrect
    warnings.clear()

  with timer("Collect stable paharmacy panel", logging.lvl):
    week_sql = f"week_id BETWEEN {panel_start.week_id} AND {panel_end.week_id}"
    df_panel_sql = f"""
WITH tmp_base AS
 (SELECT pharmacy_id
        ,week_id
        ,pat_actn_ind
    FROM prod_df2_us9.v_pep_store_wk_panel
   WHERE {week_sql}
     AND pat_actn_ind = 1
  UNION
  SELECT b.pharmacy_id
        ,a.week_id
        ,1 AS pat_actn_ind
    FROM prod_df2_us9.v_pep_store_wk_panel_override b
    JOIN (SELECT DISTINCT week_id FROM prod_df2_us9.v_calendar WHERE {week_sql}) a
      ON a.week_id BETWEEN b.start_week_id AND b.end_week_id),
tmp_ct AS
 (SELECT FLOOR(COUNT(DISTINCT week_id) * {real_time_share} / 100) AS elig_week_ct
    FROM prod_df2_us9.v_calendar
   WHERE {week_sql}),
tmp_grp AS
 (SELECT pharmacy_id
        ,COUNT(DISTINCT week_id) AS elig_period_cnt
    FROM tmp_base v
   CROSS JOIN tmp_ct w
   GROUP BY pharmacy_id, w.elig_week_ct
  HAVING COUNT(DISTINCT week_id) >= w.elig_week_ct)
SELECT t.*
      ,CAST(TRIM(nvl(x.chnl_cd, 'z')) AS CHAR(1)) AS chnl_cd
      ,CAST(TRIM(nvl(x.store_typ_cd, 'z')) AS CHAR(1)) AS store_typ_cd
      ,{panel_start.bdf_sql_str} AS stable_period_start
      ,{panel_end.bdf_sql_str} AS stable_period_end
      ,CAST({real_cohort_id} AS bigint) AS cohort_id
  FROM tmp_grp t
  LEFT JOIN prod_df2_us9.v_pharmacy x
    ON t.pharmacy_id = x.pharmacy_id
"""
  if logging.lvl < 1:
    print(df_panel_sql)
  df_return = spark.sql(df_panel_sql)
  
  if output != None and isinstance(output,str):
    splitted = output.split(".")
    if len(splitted) == 2:
      tbl_nm =splitted[1]
      db_nm = splitted[0]
    elif len(splitted) == 1:
      tbl_nm =splitted[0]
      db_nm = "prod_sts"
    else:
      warnings.append("Unable to parse output table name, table not saved.") 
      
    if tbl_nm != None and db_nm != None:
      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      sparkSave(spark, 
                tbl_nm, 
                df_return,
                partition = "cohort_id",
                format = 'parquet',
                mode='overwrite',
                database=db_nm,
                verbose_lvl = log_lvl,
                analyze = True)

    for x in warnings:
      print(x);

    #add check if other parameters types are valid also catch exception if time periods are icorrect
    warnings.clear()
    
  return spark.sql(df_panel_sql)

def pharmacyStabilityBulk(spark, cohorts, output = None, time_share = 100, log_lvl = 'INFO:1'):
  if isinstance(cohorts,pd.DataFrame):
    real_cohorts = cohorts
  elif isinstance(cohorts,DataFrame):
    real_cohorts = cohorts.toPandas()
  else:
    raise TypeError(f"Type {type(cohort)} not valid for parameter 'cohorts'. Only spark or pandas DataFrame is accepted.")
  
  df_stable_pharmacies = None
  for row in real_cohorts.itertuples(index=True, name='Pandas'):
    if df_stable_pharmacies == None:
      df_stable_pharmacies = pharmacyStability(spark, row.lookback, row.start, row.cohort, output = None, time_share = time_share, log_lvl = log_lvl)
    else:
      df_stable_pharmacies = df_stable_pharmacies.union(pharmacyStability(spark, row.lookback, row.start, row.cohort, output = None, time_share = time_share, log_lvl = log_lvl))

  if output != None:
    splitted = output.split(".")
    if len(splitted) == 2:
      tbl_nm =splitted[1]
      db_nm = splitted[0]
    elif len(splitted) == 1:
      tbl_nm =splitted[0]
      db_nm = "prod_sts"
    else:
      warnings.append("Unable to parse output table name, table not saved.") 
      
    if tbl_nm != None and db_nm != None:
      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      sparkSave(spark, 
                tbl_nm, 
                df_return,
                partition = "cohort_id",
                format = 'parquet',
                mode='overwrite',
                database=db_nm,
                verbose_lvl = log_lvl,
                analyze = True)

def patientStability(spark, patient_data, start, end, cohort = None, use_market_outlet = True, output = None, output_panel = None, time_share = 100, log_lvl = 'INFO:1'):
  r"""
  Return spark DataFrame with Stable Pharmacipanels for given cohort.
  spark       -- Spark connection.
  patient_data-- Data Frame with patient data for eligibility. Should have patient_id and svc_dt columns. pharmacy_id required if use_market_outlet is True. Can you claim table.
  start       -- Stability start date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
  end         -- Stability end date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
  use_market_outlet -- set to True if check pharmacy volume in the patient_data only. uses NDWx patient mart if set to False.                  
  output      -- If not empty, persist DataFrame into BDF with the name. Format [database.]table_name. 
                 Create partition for each cohort; if overwrites cohort data if exists. 
  output_panel -- Output stable pharmacy panel if not empty.               
  cohort      -- Cohort Id to use. if not set, month id for the start
  time_share- -- For multi period cohorts, determine how many period pharmacy have to be
                 eligible, in percent from 1 to 100. Default is 100
  log_lvl -- Contorl how much progress report to output: DEBUG,INFO,WARNING,ERROR,FATAL,NONE
  """
  
  logging = aa_log(log_lvl)
  panel_start = None
  panel_end = None
  real_cohort_id = None
  real_time_share = None
  errors = list()
  warnings = list()

  with timer("Validate Parameters", logging.lvl):

    panel_start = aa_date(start).first
    panel_end   = aa_date(end).last
    
    if panel_end.date_id < panel_start.date_id:
      panel_start = aa_date(end).first
      panel_end   = aa_date(start).last
      warnings.append("Study start and end date switched") 

    if cohort == None:
      real_cohort_id = real_panel_start.month_id
      warnings.append("Empty Cohort id set to start month id") 
    elif isinstance(cohort, int) or isinstance(cohort, str):
      real_cohort_id = int(cohort)
    else:
      real_cohort_id = real_panel_start.month_id
      warnings.append("Invalid Cohort id set to start month id") 
      
    if cohort == None:
      real_time_share = 100
      warnings.append("Empty time_share ignored") 
    elif isinstance(cohort, float) or isinstance(cohort, int) or isinstance(cohort, str):
      real_time_share = float(time_share)
      if real_time_share < 0:
        real_time_share = 0
        warnings.append("Negative time share set to 0") 
      if real_time_share > 100:
        real_time_share = 0
        warnings.append("Truncated time share to 100") 
    else:
      real_time_share = 100
      warnings.append("Invalid time_share ignored") 

    if logging.lvl < 2:
      print(f"Input range: {start} - {end}")
      print(f"Study range: {panel_start.week_id} - {panel_end.week_id}")
      print(f"Cohort:      {cohort} -> {real_cohort_id}")
      print(f"Time share:  {time_share} -> {real_time_share}")
    
    if logging.lvl < 3:
      for x in warnings:
        print(x);

    #add check if other parameters types are valid also catch exception if time periods are icorrect
    warnings.clear()

  df_panel = pharmacyStability(spark, start = start, end = end, cohort = cohort, output = output_panel, time_share = time_share, log_lvl = log_lvl)

  df_cohort = patient_data
  select_cols = [x for x in df_cohort.columns if x in ['patient_id','cohort_id','pharmacy_id']]

#  if 'week_id' in df_cohort.columns:
#    df_cohort= df_cohort.where(f.col('week_id').between(*(panel_start.week_id,panel_end.week_id)))
#  if 'month_id' in df_cohort.columns:
#    df_cohort= df_cohort.where(f.col('month_id').between(*(panel_start.month_id,panel_end.month_id)))
#  if 'svc_dt' in df_cohort.columns:
#    df_cohort= df_cohort.where(f.col('svc_dt').between(*(panel_start.datetime, panel_end.datetime)))
#  if 'cohort_id' in df_cohort.columns:
#    df_cohort= df_cohort.where(df_cohort.cohort_id==real_cohort_id)

  df_cohort = df_cohort.select(select_cols).dropDuplicates()  
  
  if use_market_outlet:
    df_unstbl_pats = df_cohort.join(df_panel, "pharmacy_id","left_anti").select("patient_id")
    df_stbl_pats = df_cohort.join(df_unstbl_pats, "patient_id","left_anti")
  else:
    #use prod_df2_us9.v_pat_pharmacy_prod_chnl_mth
    df_unstbl_pats = df_cohort.join(df_panel, df.Seq("pharmacy_id"),"left_anti").select("patient_id")
    df_stbl_pats = df_cohort.join(df_unstbl_pats, "patient_id","left_anti")

  df_stbl_pats = df_stbl_pats.join(df_panel, "pharmacy_id","left").select(['cohort_id','patient_id','chnl_cd','stable_period_start','stable_period_end']).dropDuplicates()
  df_stbl_pats = df_stbl_pats.groupBy([x for x in df_stbl_pats.columns if x != "chnl_cd"]).agg(f.collect_list('chnl_cd').alias("cnls_cds"))
  #df_stbl_pats = df_stbl_pats.withColumn("market_patient_channels", f.concat_ws(",", "cnls_cds")).drop('cnls_cds')

  if output != None and isinstance(output,str):
    splitted = output.split(".")
    if len(splitted) == 2:
      tbl_nm =splitted[1]
      db_nm = splitted[0]
    elif len(splitted) == 1:
      tbl_nm =splitted[0]
      db_nm = "prod_sts"
    else:
      warnings.append("Unable to parse output table name, table not saved.") 
      
    if tbl_nm != None and db_nm != None:
      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      sparkSave(spark, 
                tbl_nm, 
                df_stbl_pats,
                partition = "cohort_id",
                format = 'parquet',
                mode='overwrite',
                database=db_nm,
                verbose_lvl = log_lvl,
                analyze = True)

    for x in warnings:
      print(x);

  return df_stbl_pats

def patientEligibility(spark, patients, start = None, end = None, cohort = None, output = None, log_lvl = 'INFO:1'):
  r"""
  Return spark DataFrame with Stable Pharmacipanels for given cohort.
  spark       -- Spark connection.
  patients    -- Data Frame with stable patient cohort. Created using patientStability() module.
  start       -- Eligibility start date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
                 Optional. Check for start date if set
  end         -- Eligibility end date in format YYYY[MM[DD]].
                 If 4 digit length, assumes Jan 1 of the year; if 6 digit length then 1st day of the month,
                 otherwise it is day. Procedure then determine internally which week id or month id it is.
                 Optional. Check for last quarter 
  output      -- If not empty, persist DataFrame into BDF with the name. Format [database.]table_name. 
                 Create partition for each cohort; if overwrites cohort data if exists. 
  cohort      -- Filter input to given Cohort Id. if not set, all cohorts are processed
  log_lvl     -- Contorl how much progress report to output: DEBUG,INFO,WARNING,ERROR,FATAL,NONE
  """
  
  logging = aa_log(log_lvl)
  panel_start = None
  panel_end = None
  errors = list()
  warnings = list()

  with timer("Validate Parameters", logging.lvl):

    if start != None:
      panel_start = aa_date(start).first
      start_str = f'{panel_start.date_id}'
    else:
      start_str = 'None'
    if end != None:
      panel_end   = aa_date(end).last
      end_str = f'{panel_end.add(-2).month_id} - {panel_end.date_id}'
    else:
      end_str = 'None'
    
    if start != None and end != None and panel_end.date_id < panel_start.date_id:
      panel_start = aa_date(end).first
      panel_end   = aa_date(start).last
      warnings.append("Study start and end date switched") 

    if logging.lvl < 2:
      print(f"Input range: {start} - {end}")
      print(f"Study range: {start_str}; {end_str}")
      print(f"Cohort:      {cohort}")
    
    if logging.lvl < 3:
      for x in warnings:
        print(x);

    #add check if other parameters types are valid also catch exception if time periods are icorrect
    warnings.clear()
  
  #filter to cohort
  df_initial = patients

#  if cohort != None:
#    df_initial = df_initial.filter(df_initial.cohort == cohort)

  #Start date chaeck from v_pat_strt_dt
  if start != None:
    df_started = df_initial.join(spark.read.table("prod_sts.by_pat_stdt"), "patient_id")
    df_started = df_started.filter(df_started.pat_strt_dt_rx < panel_start.datetime).select(patients.columns())
  else:
    df_started = df_initial 
    
  #last quarter check in v_pat_mth_rx 
  if start != None:
    df_ended = df_started.join(spark.read.table("prod_df2_us9.v_pat_mth_rx"), "patient_id")
    df_ended = df_started.filter((df_started.month_id <= panel_end.add(-2).month_id) & (df_started.month_id <= panel_end.month_id)).select(patients.columns())
  else:
    df_ended = df_started 
  
  df_output = df_ended

  if output != None and isinstance(output,str):
    splitted = output.split(".")
    if len(splitted) == 2:
      tbl_nm =splitted[1]
      db_nm = splitted[0]
    elif len(splitted) == 1:
      tbl_nm =splitted[0]
      db_nm = "prod_sts"
    else:
      warnings.append("Unable to parse output table name, table not saved.") 
      
    if tbl_nm != None and db_nm != None:
      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      sparkSave(spark, 
                tbl_nm, 
                df_output,
                partition = "cohort_id",
                format = 'parquet',
                mode='overwrite',
                database=db_nm,
                verbose_lvl = log_lvl,
                analyze = True)

  return df_output

def providerStability():
  return None



