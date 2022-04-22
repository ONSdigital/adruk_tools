"""
Script to test diagnostics functions after being linted.

Will be removed once all functions in package have been linted
"""

from importlib.machinery import SourceFileLoader
dia = SourceFileLoader("diagnostics",
                       "/home/cdsw/adruk_tools/adruk_tools/diagnostics.py"
                       ).load_module()

adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()


# Test diagnostics funcs

# Start session
spark = adr.session_small()

# Test dataframe
df = adr.make_test_df(spark)

# -----------------

diag = dia.list_columns_by_file(spark, ['/training/department_budget.csv'])
diag

# -----------------

diag = dia.missing_count(df)
diag

# -----------------

diag = dia.missing_by_row(df, 'strVar')
diag.show()

# -----------------

diag = dia.unique_function(df)
diag
