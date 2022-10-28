"""
WHAT IT IS: python script
WHAT IT DOES: Runs a set of unit tests on functions in rename_column.py
AUTHOR: Silvia Bardoni
CREATED: 26/10/2022
"""
from importlib.machinery import SourceFileLoader
import pandas as pd
import conftest as ct
import pytest


# Provide explicit file path to updated function, otherwise the old version in the
# package is referenced. At least was for me
adr = SourceFileLoader(
    "cleaning", "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py")\
    .load_module()


# Create a test dataset to be used throughout the tests.
# This is intially created in Spark so the function can run,
# before being converted to pandas to undertake assertation.


test_columns = ['name', 'age']

test_rows = [('Nathan', 23),
             ('Joanna', 63),
             ('Tomas', 89),
             ('Nathan', 23),
             ('Johannes', 28),
             ('Nathan', 23),
             ('Tim', 67),
             (None, 89)]


def test_column_recode(spark_context):

    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: tests column_recode function in adr_functions.py
    """
    # create dataframe
    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    expected_output = adr.column_recode(
        input_dataset,
        'name', {'Nathan': 'Nat', 'Tomas': 'Tom', 'Joanna': 'Jo'}, 'Other')

    expected_output = ct.get_sorted_data_frame(expected_output.toPandas(),
                                               ['name', 'age'])

    real_output = pd.DataFrame([
                               ['Jo', 63],
                               ['Nat', 23],
                               ['Nat', 23],
                               ['Nat', 23],
                               ['Other', 28],
                               ['Other', 67],
                               ['Other', 89],
                               ['Tom', 89]
                               ],
                               columns=['name', 'age'])

    real_output = ct.get_sorted_data_frame(real_output, ['name', 'age'])

    # Test equality between expected and generated outcomes
    pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


def test_wrong_type(spark_context):
    """
    Test that the type of the recoded column is string
    """

    # Create dataframe with column type not a string
    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    with pytest.raises(TypeError) as context:
        expected_output_df = adr.column_recode(
            input_dataset,
            'age', {'Nathan': 'Nat', 'Tomas': 'Tom', 'Joanna': 'Jo'}, 'Other')
    return expected_output_df
    assert isinstance(context.value, TypeError)
    assert str(context.value) == 'Column nust be a string'
