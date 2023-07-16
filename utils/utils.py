from subprocess import call
from contextlib import contextmanager
import logging
import pandas as pd
import pickle
import glob
import time
import json
import os

from utils import time_utils

 
def sendEmail(body, 
              subject = 'CDSW Alert', 
              to = os.getenv('GIT_AUTHOR_EMAIL'), 
              sender = os.getenv('GIT_AUTHOR_EMAIL')):
  app_sender = f"{os.environ['CDSW_PROJECT']}"
  smtpsrv = 'intemail.rxcorp.com'
  smtpserver = smtplib.SMTP(smtpsrv)
  msgbody = f'To:{to}\nFrom:{app_sender}\nSubject:{subject}\n\n\n{body}'
  smtpserver.sendmail(sender, to, msgbody)
  smtpserver.close()
    
def test_empty_df(df):
  if df.empty:
    pass
  else:
    print('ERROR: Expected null QC table is populated.')
    #    sendEmail(body='ERROR: Expected null QC table is populated.')
    raise AssertionError 

def test_date_match(qc_dt):
  lastFriday = time_utils.findLastFriday(weeks=1)
  if qc_dt.strftime('%Y%m%d') == lastFriday:
    pass
  else:
    print('ERROR: QC max date does not equal to last Friday.')
    #sendEmail(body='ERROR: QC max date does not equal to last Friday.')
    raise AssertionError


@contextmanager
def timer(msg):
    """calculate elapse time
    """
    t0 = time.time()
    print(f'[{msg}] start.')
    yield
    elapse_time = time.time() - t0
    print(f'[{msg}] done in {elapse_time:.2f}s.')
    
    
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


def set_logger(log_path):
    """Sets the logger to log info in terminal and file `log_path`.
    In general, it is useful to have a logger so that every output to the terminal is saved
    in a permanent file. Here we save it to `model_dir/train.log`.
    Example:
    ```
    logging.info("Starting training...")
    ```
    Args:
        log_path: (string) where to log
    """
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



