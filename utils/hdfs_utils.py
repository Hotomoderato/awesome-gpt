import os
import pandas as pd
from subprocess import call
from io import BytesIO, StringIO
#from hdfs.ext.kerberos import KerberosClient
#from src.common.utils import timer
import shutil


class FileIO(object):
    def __init__(self,
                 server_address='https://usrhdphttpfs.rxcorp.com:14000'):
        """
        File IO Constructor
        @param server_address: hdfs server address
        """
        self.hdfs_client = KerberosClient(server_address)

    def _get_file_as_df(self, filename: str) -> pd.DataFrame:
        """
        Load df from a single hdfs file
        @param filename: filename for a single file in parquet file folder (not a directory)
        """
        io_object = BytesIO
        df = pd.DataFrame()
        with self.hdfs_client.read(filename) as fd:
            with io_object(fd.read()) as bytes_fd:
                df = pd.read_parquet(bytes_fd)
        return df

    def get_parquet_as_df(self, filename: str) -> pd.DataFrame:
        """
        Load df from a hdfs parquet files
        @param filename: parquet filename 
        """
        basenames = [f for f in hdfs_client.list(filename) if f.endswith(".parquet")]
        fullnames = [os.path.join(filename, basename) for basename in basenames]
        df = pd.DataFrame()
        for fullname in fullnames:
            df = df.append(self._get_file_as_df(fullname))
        df = df.reset_index(drop=True)
        return df
    
    def save_parquet_from_df(self, filename: str):
        """
        Save pandas df as hdfs parquet file
        """
        pass
    
    def get_filenames(self, hdfs_path):
        """
        Return a list of filenames in the folder
        """
        filenames = self.hdfs_client.list(hdfs_path, status=False)
        return filenames


def hdfs_to_local(hdir, ldir):
    """
    Copy file from hdfs to local
    
    Input
    @ldir           local directory
    @hdir           hdfs directory
    """
    fdir = os.path.join(ldir, os.path.basename(hdir))
    if os.path.exists(fdir): shutil.rmtree(fdir)
    if not os.path.exists(ldir): os.makedirs(ldir)
    string = ['hadoop', 'fs', '-copyToLocal', hdir, ldir]
    res = call(string)


#def local_to_hdfs(ldir, hdir):
#    """
#    Copy file from local to hdfs
#    
#    Input
#    @hdir           hdfs directory
#    @ldir           local directory or file path
#    """
#    exist = call(['hadoop','fs','-test','-p', hdir])
#    if exist != 0:
#        call(['hadoop','fs','-mkdir','-p', hdir])
#    call(['hadoop', 'fs', '-copyFromLocal', ldir, hdir])

def local_to_hdfs(ldir, hdir):
    """
    Copy file from local to hdfs
    
    Input
    @hdir           hdfs directory
    @ldir           local directory or file path
    """
    exist = call(['hadoop','fs','-test','-d', hdir])
    if exist != 0:
        call(['hadoop','fs','-mkdir','-p', hdir])
    call(['hadoop', 'dfs', '-copyFromLocal','-f', ldir, hdir])


def set_hdfs_permission(filename, code=777):
    """
    Set HDFS file permission
    """
    string = ['hadoop', 'fs', '-chmod', '-R', code, filename]
    res = call(string)  


def remove_hdfs_files(filename):
    """
    Remove HDFS files
    """
    string = ['hadoop', 'fs', '-rm', '-skipTrash', '-R', filename]
    res = call(string)  
