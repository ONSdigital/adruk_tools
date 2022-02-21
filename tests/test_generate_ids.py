# https://www.sicara.ai/blog/2019-01-14-tutorial-test-pyspark-project-pytest

# turn into panadas and checks can be undertaken using assert calls
# https://pandas.pydata.org/docs/reference/general_utility_functions.html#testing-functions


# https://github.com/MrPowers/chispa

# Could use chispa for pyspark testing. Looks a useful package.
# blog post here https://mungingdata.com/pyspark/testing-pytest-chispa/

#from adruk_tools import adr_functions as adr
import pyspark.sql.types as T
import pandas as pd
import pytest as pt


#######################################
from importlib.machinery import SourceFileLoader

adr = SourceFileLoader("adr_functions", "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py").load_module()
#######################################

# Test data

test_columns = ['name', 'id', 'age', 'bmi', 'year']

test_rows = [('Nathan', 'A', 23, 45.679, '2008'), 
     ('Joanna', 'B', 63, 25.080, '2008'), 
     ('Tom', 'A', 89, 99.056, '2008'),
     ('Nathan', 'C', 23, 45.679, '2008'), # same as row 1
     ('Nathan', 'A', 23, 45.679, '2008'),# same as row 1, different id 
     ('Johannes', 'E', 67, 25.679, '2009'), # new year
     ('Nathan', 'B', 23, 45.679, '2009'), # same as row 1, different year
     ('Johannes', 'E', 67, 45.679, '2009'), # same as row 1, different id, different year
     ('Nathan', None, 23, 45.679, '2009'), # test empty id
     ('Nathan', 'C', 23, 45.679, None), # test empty year
     (None, 'F', 89, 99.056, '2008')]





def test_one_col_adr_id_unique(spark_context):
  
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  is_expected_output_unique = True
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id']).toPandas()
  
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])
  
  #== real_output['id'].count()

  
  print( "columns in the output table are not unique")
  assert is_expected_output_unique == is_real_output_unique
  

  
def test_one_col_equal_count(spark_context):
  # count between adr_id and combination of other columns match
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  expected_output_count = 6
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
 
  print("count of unique dont match between id and adr_id in output table")
  assert expected_output_count == real_output_count
  
  

def test_two_col_adr_id_unique(spark_context):
  
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  is_expected_output_unique = True
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id']).toPandas()
  
  is_real_output_unique = real_output['adr_id'].nunique() == len(set(zip(real_output['name'], real_output['id'])))

  
  print( "columns in the output table are not unique")
  assert is_expected_output_unique == is_real_output_unique
  

  
def test_two_col_equal_count(spark_context):
  # count between adr_id and combination of other columns
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  expected_output_count = 8
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
 
  print("count of unique dont match between id and adr_id in output table")
  assert expected_output_count == real_output_count


def test_one_col_start_year_adr_id_unique(spark_context):
  
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  is_expected_output_unique = True
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id'], ['age']).toPandas()
  
  is_real_output_unique = real_output['adr_id'].nunique() == len(real_output['adr_id'])
  
  #== real_output['id'].count()

  
  print( "columns in the output table are not unique")
  assert is_expected_output_unique == is_real_output_unique
  

  
def test_one_col_start_year_equal_count(spark_context):
  # count between adr_id and combination of other columns match
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  expected_output_count = 8
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['id'], ['age']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
 
  print("count of unique dont match between id and adr_id in output table")
  assert expected_output_count == real_output_count
  
  
  
def test_two_col_start_year_adr_id_unique(spark_context):
  
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  is_expected_output_unique = True
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id'], ['age']).toPandas()
  
  is_real_output_unique = real_output['adr_id'].nunique() == len(set(zip(real_output['name'], real_output['id'])))

  
  print( "columns in the output table are not unique")
  assert is_expected_output_unique == is_real_output_unique
  

  
def test_two_col_start_year_equal_count(spark_context):
  # count between adr_id and combination of other columns
  
  input_dataset = spark_context.createDataFrame(test_rows, test_columns)
  
  expected_output_count = 8
  
  real_output = adr.generate_ids(spark_context, input_dataset, ['name', 'id'], ['age']).toPandas()
  real_output_count = real_output['adr_id'].nunique()
 
  print("count of unique dont match between id and adr_id in output table")
  assert expected_output_count == real_output_count
  
