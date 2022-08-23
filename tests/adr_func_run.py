from importlib.machinery import SourceFileLoader
adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()

# Test adr functions
# ------------------

# Start session
spark = adr.session_medium()

# Make dummy ninos
dummy = adr.make_dummy_ninos(spark)

# Read file straight to pandas
test = adr.pydoop_read(file_path='/training/animal_rescue.csv')

# Read file straight to pandas
test = adr.hdfs_to_pandas(file_path='/training/animal_rescue.csv')

# unzip to csv
# will fail but creates output folder in shawn1
# cant find csv.gz to use as test.
adr.unzip_to_csv('/ons/det_training',
                 'taxi_nathan.csv',
                 '/user/shawn1/'
                 )

# write to csv
adr.pandas_to_hdfs(dataframe=dummy.toPandas(),
                   write_path='/user/shawn1/sample.csv')

# cull columns
adr.cull_columns(spark, ['/training/animal_rescue.csv'], ['INCIDENTNUMBER'],
                 '/user/shawn1/')

# equalise file name
adr.equalise_file_and_folder_name('/user/shawn1/animal_rescue.csv')

# Mani fest class
path = '/ons/det_training/dummy_manifest.mani'

instance = adr.manifest(path)
details = instance.whole()
details

# Create test dataframe
test = adr.make_test_df(spark)

# Anonymise ids
anom = adr.anonymise_ids(dummy, ['nino'])

# Generate ids
gen = adr.generate_ids(spark, dummy, ['nino'], 'index')

# Glob on directory
adr.spark_glob(host='hechlj',
               directory='/dapsen/landing_zone/hmrc/self_assessment/2017/v1'
               )

# Glob on directory
adr.spark_glob_all(host='hechlj',
                   directory='/dapsen/landing_zone/hmrc/self_assessment/2017/v1'
                   )

# Lookup class
test = adr.Lookup('key', 'value')
test.create_lookup_source(spark)
test.add_to_lookup(spark, anom, 'nino', 'adr_id')
