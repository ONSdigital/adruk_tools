# import standard packages
import pytest
import pandas as pd
import pydoop.hdfs as pdh
import os

# import package to test function from
import adruk_tools.adr_functions as adr

def test_pandas_to_hdfs():
  #-----------
  # parameters
  #-----------
  
  # unclear if this directory has read/write access for everyone
  write_path = '/dapsen/de_testing/deleteme.csv'

  # make pandas df; contents are irrelevant
  dataframe = pd.DataFrame({'col1':[1,2,3],
                           'col2' : ['dummy file made for unit test', 
                                     'delete it on sight', 
                                     'no really, delete it.']})

  # write dataframe to HDFS
  adr.pandas_to_hdfs(dataframe = dataframe, 
                      write_path = f'{write_path}' )

  # TEST: check it has been written
  assert pdh.path.exists(write_path)

  # delete dataframe from HDFS
  os.system(f'hdfs dfs -rm -r {write_path}')
