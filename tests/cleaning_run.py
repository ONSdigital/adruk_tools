from importlib.machinery import SourceFileLoader
cln = SourceFileLoader("cleaning",
                       "/home/cdsw/adruk_tools/adruk_tools/cleaning.py"
                       ).load_module()

adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()

import pyspark.sql.functions as F

# Test cleaning funcs
# -------------------

# Start session
# Start session
spark = adr.session_small()

# Test dataframe
df = adr.make_test_df(spark)

col = df.select('postcode')

df_clean = cln.remove_whitespace(str(col))