"""
WHAT IT IS: python script
WHAT IT DOES: Runs a set of unit tests on functions in cleaning.py
AUTHOR: Nathan Shaw
CREATED: 19/02/2022
LAST UPDATE: 28/02/2022

"""
from importlib.machinery import SourceFileLoader

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


def test_space_to_underscore(spark_context):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: tests space_to_underscore function in cleaning.py
    """

    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    output = adr.space_to_underscore(input_dataset)

    assert 'ni_no' in output.columns
