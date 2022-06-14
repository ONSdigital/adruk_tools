"""
Script to test cleaning functions after being linted.

Will be removed once all functions in package have been linted
"""

import adruk_tools.adr_functions as adr
import pandas as pd

from importlib.machinery import SourceFileLoader
dsc = SourceFileLoader(
    "describe", "/home/cdsw/adruk_tools/adruk_tools/describe.py").load_module()

# Create session and mock dataframe
spark = adr.session_small()

df = pd.DataFrame({
    "col1": ['A', 'A', None, 'C', ''],
    "col2": [1, 2, 2, None, -2],
    "col3": [15, 0, -6, -5, 10],
    "col4": [True, False, None, True, False]
})
df = spark.createDataFrame(df)
df.show()


# Test incorrect describe type error
result = dsc.describe(df, 'random')

# Test individual describe types
# wont display columns for string or boolean columns
result_sum = dsc.describe(df, 'sum')
result_positive = dsc.describe(df['col1', 'col2', 'col3'], 'sum')
result_sum

# Fails on boolean column; marks zero for string column
result_positive = dsc.describe(df, 'positive')
result_positive = dsc.describe(df['col1', 'col2', 'col3'], 'positive')
result_positive

# Fails on boolean column; marks zero for string column
result_negative = dsc.describe(df, 'negative')
result_negative = dsc.describe(df['col1', 'col2', 'col3'], 'negative')
result_negative

# Counts False as a zero in boolean column
result_zero = dsc.describe(df, 'zero')
result_zero

# Only works on string /bool columns, marks zero for all others.
result_null = dsc.describe(df, 'null')
result_null

# Fails on booleon types; only count/marks on numeric types. Others zero.
result_nan = dsc.describe(df, 'nan')
result_nan = dsc.describe(df['col1', 'col2', 'col3'], 'nan')
result_nan

result_unique = dsc.describe(df, 'unique')
result_unique

# Only counts for string columns
result_blank = dsc.describe(df, 'blank')
result_blank

# Only counts for numeric columns
result_mean = dsc.describe(df, 'mean')
result_mean

# Fails on Boolean. Only counts for numeric.
result_stddev = dsc.describe(df, 'stddev')
result_stddev = dsc.describe(df['col1', 'col2', 'col3'], 'stddev')
result_stddev

result_max = dsc.describe(df, 'max')
result_max

result_min = dsc.describe(df, 'min')
result_min


# Fails on Boolean. Only counts for numeric.
result_mode = dsc.mode_describe(df['col1', 'col2', 'col3'])
result_mode

result_special = dsc.special_describe(df, {'regex1': '[A]', 'regex2': '[C]'})
result_special

# Test extended describe
result_extended = dsc.extended_describe(
    df['col1', 'col2', 'col3'],
    all_=False,
    active_columns_=False,
    positive_=True,
    sum_=True,
    negative_=True,
    zero_=True,
    null_=True,
    nan_=True,
    count_=True,
    unique_=True,
    blank_=True,
    mean_=True,
    stddev_=True,
    length_mean_=True,
    min_=True,
    max_=True,
    range_=True,
    mode_=True,
    axis_=0
)
result_extended

# Test extended describe with all_ activated
result_extended = dsc.extended_describe(
    df['col1', 'col2', 'col3'],
    all_=True
)
result_extended


# Test extended describe with special dictionary
result_extended = dsc.extended_describe(
    df['col1', 'col2', 'col3'],
    all_=False,
    active_columns_=True,
    nan_=True,
    count_=True,
    percent_=True,
    blank_=True,
    mean_=True,
    min_=True,
    max_=True,
    range_=True,
    mode_=True,
    axis_=0,
    special_=True,
    special_dict_={'regex1': '[A]', 'regex2': '[C]'}
)
result_extended


# Test extended describe with fillna
# Note that the the fillna is done on the out so the result might not be what expected.
result_extended = dsc.extended_describe(
    df['col1', 'col2', 'col3'],
    all_=False,
    fillna_=7,
    active_columns_=False,
    positive_=True,
    sum_=True,
    negative_=True,
    zero_=True,
    null_=True,
    nan_=True,
    count_=True,
    unique_=True,
    special_=True,
    blank_=True,
    mean_=True,
    stddev_=True,
    length_mean_=True,
    min_=True,
    max_=True,
    range_=True,
    mode_=True,
    axis_=0
)
result_extended
