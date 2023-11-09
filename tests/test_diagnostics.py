"""
LANGUAGE: pyspark, python
WHAT IT DOES: Runs a set of unit tests on functions in diagnostics.py
AUTHOR: Shedrack Ezu
UPDATED: 10/10/2023
"""

import pytest
import adruk_tools.adr_functions as adr
import adruk_tools.diagnostics as adrd
import pyspark.sql.functions as F

# create spark session
# TO DO: instead, use the shared spark session in conftest.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('practice').getOrCreate()


# TEST ATTEMPT
def test_duplicate_flag():
  """
  Unit test:
  The function being tested checks for duplicate flags for each recored 
  and outputs a boolean (True of False) indicating if duplicate exists or not
      
  Structure here follows the Arrange, Act, Assert pattern:
      - Arrange: set up the inputs and expected outputs
      - Act: call the function and return the result
      - Assert: Check that the actual result is as expected
  """
  
  # Arrange
  df = adr.make_test_df(spark)
  
  expected_flag = [False, False, False, False, False, True, True, False, False, False, False]
  
  # Act
  actual_flag = list(
  df.select(adrd.duplicates_flag( columns_to_flag = [F.col('strVar')] ).alias('flag'))
  .toPandas()
  .flag)
  
  # Assert
  assert actual_flag == expected_flag