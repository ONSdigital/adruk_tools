"""
Script to test cleaning functions after being linted.

Will be removed once all functions in package have been linted
"""

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


df_clean = df.withColumn('clean_postcode',
                         cln.remove_whitespace('postcode'))
df_clean.toPandas()

#-----------------

df_upper = df.withColumn('upper_name',
                        cln.clean_nino('name'))
df_upper.toPandas()

#-----------------

df_clean_names = cln.clean_names(df, ['postcodeNHS'])
df_clean_names.toPandas()

#-----------------

df_clean_names_part = cln.clean_names(df, ['postcode'])
df_clean_names_part.toPandas()

#-----------------

df_clean_names_full = cln.clean_names(df, ['postcode'])
df_clean_names_full.toPandas()

#-----------------

df_array = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)
df_array_columns = cln.array_to_columns(
  df_array,
  ['numbers'],
  ['first_number', 'second_number'],
  number_of_columns = 2
)
df_array_columns.toPandas()

#-----------------

df_concatenate_cols = cln.concatenate_columns(
  df, 
  'joined',
  ['postcodeNHS', 'postcode'],
  '::::'
)
df_concatenate_cols.toPandas()

#-----------------

df_concatenate_cols_distinct = cln.concatenate_columns(
  df, 
  'joined',
  ['postcodeNHS', 'postcode'],
  '::::'
)
df_concatenate_cols_distinct.toPandas()

#-----------------

df_date_recode = cln.date_recode(df, 'dob', 'dob')
df_date_recode.toPandas()

#-----------------

df_lookup = cln.LAlookup(
  test_df = df,
  test_variables = ['postcode'],
  reference_table = 'training.elector_register_synthetic',
  reference_variable = 'postcode',
  LA_code_variable = 'local_authority',
  connection = spark
)
df_lookup.toPandas()

#-----------------

df_missing_none = cln.make_missing_none(df, ['strVar'])
df_missing_none.toPandas()

#-----------------

df_name_split = cln.name_split(df, ['name'], 'a')
df_name_split.toPandas()

#-----------------

df_array = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)

df_name_split_array = cln.name_split(df_array, ['id'], 'a')
df_name_split_array.toPandas()

#-----------------
# Cant test as table imported in code only available in DAP
df_nhs_recode = cln.NHS_postcode_recode(df, ['postcodeNHS'], ['NHS1'], spark)

#-----------------

df_clean_postcode = cln.clean_postcode(df, ['postcodeNHS'], 4)
df_clean_postcode.toPandas()

#-----------------

df_postcode_pattern = cln.postcode_pattern(df, 'postcode')
df_postcode_pattern.toPandas()

#-----------------

df_postcode_split = cln.postcode_split(df, 'postcode', '----', spark)
df_postcode_split.toPandas()

#-----------------

df_rename_columns = cln.rename_columns(df, ['postcode'], ['RENAME'])
df_rename_columns.toPandas()

#-----------------

test_columns = ['sex', 'id', 'age', 'bmi', 'year']

test_rows = [('M', 'A', 23, 45.679, '2008'),
     ('1', 'B', 63, 25.080, '2008'),
     ('MALE', 'A', 89, 99.056, '2008'),
     ('2', 'C', 23, 45.679, '2008'),
     ('F', 'A', 23, 45.679, '2008'),
     ('FEMALE', 'E', 67, 25.679, '2009'),
     ('3', 'B', 23, 45.679, '2009'),
     ('I', 'E', 67, 45.679, '2009'),
     ('0', None, 23, 45.679, '2009'),
     ('Nathan', 'C', 23, 45.679, None),
     (None, 'F', 89, 99.056, '2008')]

df_sex_recode = spark.createDataFrame(test_rows, test_columns)

df_sex_recode = cln.sex_recode(df_sex_recode, 'sex', 'sex_recode')
df_sex_recode.toPandas()

#-----------------

test_columns = ['s e x', 'i d', 'a  ge', 'bmi', 'year']

test_rows = [('M', 'A', 23, 45.679, '2008'),
     ('1', 'B', 63, 25.080, '2008'),
     ('MALE', 'A', 89, 99.056, '2008'),
     ('2', 'C', 23, 45.679, '2008'),
     ('F', 'A', 23, 45.679, '2008'),
     ('FEMALE', 'E', 67, 25.679, '2009'),
     ('3', 'B', 23, 45.679, '2009'),
     ('I', 'E', 67, 45.679, '2009'),
     ('0', None, 23, 45.679, '2009'),
     ('Nathan', 'C', 23, 45.679, None),
     (None, 'F', 89, 99.056, '2008')]

df_space = spark.createDataFrame(test_rows, test_columns)

df_space_underscore = cln.space_to_underscore(df_space)
df_space_underscore.toPandas()

#-----------------

df_title_remove = cln.title_remove(df, ['name'])
df_title_remove.toPandas()