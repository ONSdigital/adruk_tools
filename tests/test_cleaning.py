"""
LANGUAGE: python, pyspark
WHAT IT DOES: Runs a set of unit tests on functions in cleaning.py
AUTHOR: Nathan Shaw, johannes hechler
CREATED: 19/02/2022
LAST UPDATE: 10/01/2024
"""
from importlib.machinery import SourceFileLoader
import pyspark.sql.functions as F

repo_path = '/home/cdsw/adruk_tools/tests'
import sys
# map local repo so we can import local libraries
sys.path.append(repo_path)

import pandas as pd
import conftest as ct


# Provide explicit file path to updated function, otherwise the old version in the
# package is referenced. At least was for me
adr = SourceFileLoader("cleaning",
                       "/home/cdsw/adruk_tools/adruk_tools/cleaning.py").load_module()


# Create a test dataset to be used throughout the tests.
# This is intially created in Spark so the function can run,
# before being converted to pandas to undertake assertation.

# Extra comma included in test rows to force pyspark to create a single column
# dataframe, which otherwise throws an error. The comma unsures a tuple is created
# (not just a string) so the createDataFrame method can be used.

test_columns = ['ni no']

test_rows = [('Nathan',),
             ('Jo anna',),
             ('Tom  \n',),
             ('Na than\t',),
             ('     ',),
             (None,)]

# Tests designed to check for additional whitespace characters other than space
# for example newline, a tab and other unicode characters.

# Note that all functions start with test_ and call in the spark_context created
# in conftest.py. Required for the tests to be run.

# Further resources
# Original code + methods from here
# https://www.sicara.ai/blog/2019-01-14-tutorial-test-pyspark-project-pytest
# Pandas assert calls for checking DF and Series equality
# https://pandas.pydata.org/docs/reference/
# general_utility_functions.html#testing-functions
# PySpark tests for assertions between DF and Cols using chispa package
# https://github.com/MrPowers/chispa
# https://mungingdata.com/pyspark/testing-pytest-chispa/


def test_deduplicate_ordered(test_data):
  """
  unit test for deduplicate_ordered()
  
  author
  ------
  johannes hechler
  
  date
  ----
  10/01/2024
  
  notes
  -----
  uses test spark dataframe from conftest.py
  TO DO: add more tests: does the sorting work?
  """
  # define what columns to deduplicate and which to sort on
  subset = ['strVar', 'dob']
  order = [F.asc('numVar')]
  
  # create function output on chosen parameters
  deduped_data = adr.deduplicate_ordered(dataframe = test_data, 
                                         subset = subset, 
                                         order = order)
  
  # check if function output has as many rows as test input data
  # ... deduplicated using standard function
  assert deduped_data.count() == test_data.dropDuplicates(subset).count()
  

def test_remove_whitespace(spark_context):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: tests remove_whitespace function in cleaning.py
    """

    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    expected_output = input_dataset.withColumn('ni no', adr.remove_whitespace('ni no'))
    expected_output = ct.get_sorted_data_frame(expected_output.toPandas(), ['ni no'])

    real_output = pd.DataFrame([
        ['Nathan'],
        ['Joanna'],
        ['Tom'],
        ['Nathan'],
        [''],
        [None]], columns=['ni no'])

    real_output = ct.get_sorted_data_frame(real_output, ['ni no'])

    # Test equality between expected and generated outcomes
    pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)
