"""
Script to test describe functions after being linted.

Will be removed once all functions in package have been linted
"""

from importlib.machinery import SourceFileLoader
des = SourceFileLoader("describe",
                       "/home/cdsw/adruk_tools/adruk_tools/describe.py"
                       ).load_module()

adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()

# Test describe funcs
# -------------------

# Start session
spark = adr.session_small()


# Test dataframe
df = adr.make_test_df(spark)

# -----------------

out = des.pandas_describe(df)
out

# -----------------

out = des.sum_describe(df)
out

# -----------------

out = des.positive_describe(df)
out

# -----------------

out = des.negative_describe(df)
out

# -----------------

out = des.zero_describe(df)
out

# -----------------

out = des.null_describe(df)
out

# -----------------

out = des.nan_describe(df)
out

# -----------------

out = des.unique_describe(df)
out

# -----------------

out = des.blank_describe(df)
out

# -----------------

out = des.mean_describe(df)
out

# -----------------

out = des.means_describe(df)
out

# -----------------

out = des.stddev_describe(df)
out

# -----------------

out = des.min_describe(df)
out

# -----------------

out = des.max_describe(df)
out

# -----------------

out = des.mode_describe(df)
out

# -----------------

out = des.special_describe(df, {'regex': '[az]'})
out

# -----------------

out = des.extended_describe(df)
out
