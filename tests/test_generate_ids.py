"""
WHAT IT IS: python script
WHAT IT DOES: Runs a set of unit tests on the generate_ids() function in adr_functions.py
AUTHOR: Nathan Shaw
CREATED: 19/02/2022
LAST UPDATE: 22/02/2022

"""
from importlib.machinery import SourceFileLoader

import pandas as pd
import pytest as pt


# Provide explicit file path to updated function, otherwise the old version in the package is referenced 
# At least was for me
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

# Tests have been created to check two things for a variety of input parameters
# 1 - Check that the created adr_id column is unique
# 2 - Number of unique values match between adr_id and unique permutations of
# other input columns (from id_cols and start_year).

# These two tests are undertaken for the following input parameter scenarios
# a - 1 x id_col, 0 x start_year
# b - 2 x id_col, 0 x start_year
# c - 1 x id_col, 1 x start_year
# d - 2 x id_col, 1 x start_year

# Note that all functions start with test_ and call in the spark_context created
# in conftest.py. Requirements for the tests to be run.

def test_one_col_adr_id_unique(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests generate_ids() function creates a unique adr_id column
  in the case when one id_col is provided.
  """
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id']).toPandas()

  # Test whether generated adr_id column contains only unique values
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])

  # Expected outcome of uniqueness test i.e. we expect adr_id to be True
  is_expected_output_unique = True

  # Test equality between expected and generated outcomes
  print( "adr_id is not unique")
  assert is_expected_output_unique == is_real_output_unique
  

  
def test_one_col_equal_count(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests that number of unique values are the same between adr_id
  column and unique permutations of id_cols and start_year. In this case one id_col
  variable.
  """
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  real_output = adr.generate_ids(spark_context, input_dataset, ['id']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
  
  # Manually calculated number of unique values in permutations of id_cols and start_year
  expected_output_count = 6
  
  # Test equality between expected and generated outcomes
  print("count of unique values do not match between adr_id and input columns")
  assert expected_output_count == real_output_count
  
  

def test_two_col_adr_id_unique(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests generate_ids() function creates a unique adr_id column
  in the case when two id_col are provided.
  """

  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id']).toPandas()

  # Test whether generated adr_id column contains only unique values
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])
    
  # Expected outcome of uniqueness test i.e. we expect adr_id to be True
  is_expected_output_unique = True

  # Test equality between expected and generated outcomes
  print( "adr_id is not unique")
  assert is_expected_output_unique == is_real_output_unique


  
def test_two_col_equal_count(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests that number of unique values are the same between adr_id
  column and unique permutations of id_cols and start_year. In this case two id_col
  variables.
  """
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
  
  # Manually calculated number of unique values in permutations of id_cols and start_year
  expected_output_count = 8
  
  # Test equality between expected and generated outcomes
  print("count of unique values do not match between adr_id and input columns")
  assert expected_output_count == real_output_count
    
    

def test_one_col_start_year_adr_id_unique(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests generate_ids() function creates a unique adr_id column
  in the case when one id_col is provided and one start_year.
  """
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id'], ['age']).toPandas()

  # Test whether generated adr_id column contains only unique values
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])

  # Expected outcome of uniqueness test i.e. we expect adr_id to be True
  is_expected_output_unique = True

  # Test equality between expected and generated outcomes
  print( "adr_id is not unique")
  assert is_expected_output_unique == is_real_output_unique



def test_one_col_start_year_equal_count(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests that number of unique values are the same between adr_id
  column and unique permutations of id_cols and start_year. In this case one id_col
  variable and one start_year variable.
  """
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  real_output = adr.generate_ids(spark_context, input_dataset, ['id'], ['age']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
  
  # Manually calculated number of unique values in permutations of id_cols and start_year
  expected_output_count = 8
  
  # Test equality between expected and generated outcomes
  print("count of unique values do not match between adr_id and input columns")
  assert expected_output_count == real_output_count



def test_two_col_start_year_adr_id_unique(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests generate_ids() function creates a unique adr_id column
  in the case when two id_col are provided and one start_year variable.
  """

  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id'], ['age']).toPandas()

  # Test whether generated adr_id column contains only unique values
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])
    
  # Expected outcome of uniqueness test i.e. we expect adr_id to be True
  is_expected_output_unique = True

  # Test equality between expected and generated outcomes
  print( "adr_id is not unique")
  assert is_expected_output_unique == is_real_output_unique


  
def test_two_col_start_year_equal_count(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests that number of unique values are the same between adr_id
  column and unique permutations of id_cols and start_year. In this case two id_col
  variables and one start_year variable.
  """
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id'], ['age']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
  
  # Manually calculated number of unique values in permutations of id_cols and start_year
  expected_output_count = 8
  
  # Test equality between expected and generated outcomes
  print("count of unique values do not match between adr_id and input columns")
  assert expected_output_count == real_output_count