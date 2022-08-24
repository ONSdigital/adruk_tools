"""
WHAT IT IS: python script
WHAT IT DOES: Runs a set of unit tests on function cull_columns
AUTHOR: Silvia Bardoni
CREATED: 22/08/2022

"""
from importlib.machinery import SourceFileLoader

import pandas as pd
from pandas.testing import assert_frame_equal
import conftest as ct
import pytest as pt
import os


# Provide explicit file path to updated function, otherwise the old version in the package is referenced
adr = SourceFileLoader("adr_functions", "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py").load_module()


# Create a test dataset to be used throughout the tests. This is intially created in Spark
# so the function can run, before being converted to pandas to undertake assertation.

test_columns = ['name', 'id', 'age', 'bmi', 'year']

test_rows = [('Nathan', 'A', 23, 45.679, '2008'),
     ('Joanna', 'B', 63, 25.080, '2008'),
     ('Tom', 'A', 89, 99.056, '2008'),
     ('Nathan', 'C', 23, 45.679, '2008'),
     ('Nathan', 'A', 23, 45.679, '2008'),
     ('Johannes', 'E', 67, 25.679, '2009'),
     ('Nathan', 'B', 23, 45.679, '2009'),
     ('Johannes', 'E', 67, 45.679, '2009'),
     ('Nathan', None, 23, 45.679, '2009'),
     ('Nathan', 'C', 23, 45.679, None),
     (None, 'F', 89, 99.056, '2008')]

# paths in HUE, note that the culled dataframe needs to go into a different directory and will have the same name
write_path = '/dapsen/de_testing/test_dataframe.csv'
write_path_cull = '/dapsen/de_testing/cull/'

# Note that all functions start with test_ and call in the spark_context created
# in conftest.py. Required for the tests to be run.


def test_cull_columns(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests the function cull_columns in adr_functions
  Uses the the method get_sorted_data_frame to sort the PySpark DFs and 
  converts PySpark DFs to pandas DFs to then use assert_frame_equal from
  pandas.testing.
  """

  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  # save into HUE
  input_dataset.coalesce(1).write.csv(
    f'{write_path}',
    sep=",",  # set the seperator
    header="true",  # Set a header
    mode="overwrite",
    ) 

  # cull_columns
  adr.cull_columns(cluster=spark_context,
                   old_files=[f'{write_path}'],
                   reference_columns=['NAME', 'YEAR'],
                   directory_out=f'{write_path_cull}')
  
  # read the culled csv
  expected_output = spark_context.read.csv(f'{write_path_cull}test_dataframe.csv', header=True)
  # sort and to Pandas
  expected_output = ct.get_sorted_data_frame(expected_output.toPandas(), ['NAME', 'YEAR'])

  real_output = pd.DataFrame([
    ['Nathan','2008'],
    ['Joanna', '2008'],
    ['Tom', '2008'],
    ['Nathan', '2008'],
    ['Nathan', '2008'],
    ['Johannes', '2009'],
    ['Nathan', '2009'],
    ['Johannes', '2009'],
    ['Nathan', '2009'],
    ['Nathan', None],
    [None, '2008']], columns=['NAME', 'YEAR'])
 
  # sort and to Pandas
  real_output = ct.get_sorted_data_frame(real_output, ['NAME', 'YEAR'])

  # Test equality between expected and generated outcomes
  
  print("columns culled")
  assert_frame_equal(expected_output, real_output, check_like=True)

# delete dataframes from HDFS
os.system(f'hdfs dfs -rm -r {write_path}')
os.system(f'hdfs dfs -rm -r {write_path_cull}')
  