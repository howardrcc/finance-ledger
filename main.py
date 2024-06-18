from datetime import datetime
import platform
import os
import string
import sys
import duckdb
import numpy
import pandas as pd
#pathlib



pd.set_option('max_colwidth', 800)
print(datetime.now())

def init():
  """
  get os/platform using platform.release(), platform.version() platform.platform()
  """
  ps=platform.system()
  user=os.getlogin()

  if ps == 'Linux': #platform.system()
    print("linux")

    working_path = f'/home/{user}/workspace/finance-ledger'
    path = os.path.normpath(working_path)
    datapath = os.path.normpath(f"/home/{user}/Documents/MINT HC/2023/transactiedata/")

    os.chdir(path)

  else:
    working_path = rf'C:\Users\howie\1.Documenten\MINT HC\database\ingestion'
    path = os.path.normpath(working_path)
    datapath = os.path.normpath(rf"C:\Users\howie\1.Documenten\MINT HC\2023\transactiedata")
    os.chdir(path)

  #os.chdir(working_path)

  print(path, datapath)

  return(path, datapath)

paths=init()

#in memory database
#conn = duckdb.connect()
#conn = duckdb.connect()
#%sql conn --alias duckdb
#%sql duckdb:///:default: #%sql duckdb:///:memory:
#%sql duckdb:///minthc_db


def get_last_csv(datapath):

  ingestion_folder = datapath
  os.chdir(datapath)

  files = filter(os.path.isfile, os.listdir(ingestion_folder))

  files_filtered = [ fi for fi in files if fi.endswith(".csv") ]
  files_with_path = [os.path.join(ingestion_folder, f) for f in files_filtered]

  if (len(files_with_path) > 1):
   #files.sort(key=lambda x: os.path.getmtime(x))
    file_to_ingest=files_with_path.sort(key=os.path.getmtime, reverse=True)
  else:
    file_to_ingest=files_with_path
  #file_to_ingest = 'transactie-historie_NL97ASNB8831419706_20240426112409.csv'
  ts = datetime.fromtimestamp(os.path.getmtime(os.path.normpath(file_to_ingest[0]))).strftime('%Y-%m-%d %H:%M:%S')
  print(f'file to ingest' + file_to_ingest[0])
  print(ts)

  #file_to_ingest = files[0]
  # to new fn
  query = "SELECT * FROM read_csv_auto('" + file_to_ingest[0] + "',auto_detect=true);"
  #query = "SELECT * FROM read_csv_auto('" + file_to_ingest + "');"
  print(query)
  df = duckdb.sql(query)

  #df = conn.sql(query)
  #df
  df1 = df.df()

  df1.dtypes

  return df.df()
  #conn.sql("select * from temp.pg_catalog.pg_database")
  #df.fetch



if __name__ == '__main__':
  print('entrypoint')
  paths=init()

  datapath = paths[1]
  df = get_last_csv(datapath)

  print(os.getcwd())

  #dbpath = working_path + "/minthc_db"
  #print(dbpath)
  #in memory
  conn = duckdb.connect()
  conn.sql(f'select * from '{datapath})