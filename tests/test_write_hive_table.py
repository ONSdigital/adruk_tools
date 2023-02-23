"LEFTOVER: convert to proper unit test"

from importlib.machinery import SourceFileLoader
adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py"
                       ).load_module()

# Start session
spark = adr.session_medium()

# Make dummy dataframe
dummy = adr.make_dummy_ninos(spark)

# Testing of write to hive function

# Not all mandatory properties
properties_1 = {'project': 'adr_functions',
                'description': 'test_description'}

# Mandatory parameters
properties_2 = {'project': 'adr_functions',
                'description': 'test_description',
                'tags': 'delete, adruk_tools, nathan'}

# Extra random parameters
properties_3 = {'project': 'adr_functions',
                'description': 'test_description',
                'tags': 'delete, adruk_tools, nathan',
                'random': 'random text'}

adr.write_hive_table('train_tmp', 'adr_hive_test', dummy, properties_1)
adr.write_hive_table('train_tmp', 'adr_hive_test', dummy, properties_2)
adr.write_hive_table('train_tmp', 'adr_hive_test', dummy, properties_3)
