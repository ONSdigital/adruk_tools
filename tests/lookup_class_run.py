from importlib.machinery import SourceFileLoader
adr = SourceFileLoader("adr_functions", "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py").load_module()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession.builder.appName("lookup_class_test")
	.config("spark.sql.shuffle.partitions", 1)
    .enableHiveSupport()
    .getOrCreate()
)


#----------------------------
# start tests
#----------------------------

# Basic hard coded datasets
# Initialise mock dataframes

test_columns1 = ['name', 'id']
test_columns2 = ['first_name', 'nino']

test_rows1 = [('Nathan', 'A'),
     ('Joanna', 'B'),
     ('Tom', 'A')]

test_rows2 = [
     ('Nathan', 'B'),
     ('Mike', 'A'),
     ('Silvia', 'E'),
     ('Sophie', 'B'),
     ('Ben', 'E')]

test_dataset_one = spark.createDataFrame(test_rows1, test_columns1)
test_dataset_two = spark.createDataFrame(test_rows2, test_columns2)

#-----------------------------------------------------------
## First example: No lookup source, created first and then updated
## Small hard coded dataset used above
#-----------------------------------------------------------

# Intialise lookup
empty_lookup = adr.Lookup(key = 'name', 
                          value = 'id')

# Create lookup source
if empty_lookup.source is None:
  empty_lookup.create_lookup_source(spark)

  
# Get dataset to append
dataset_to_append = test_dataset_two


# Update empty lookup with data from new dataset
# Different column names taken care off
empty_lookup.add_to_lookup(dataset = dataset_to_append, 
                           dataset_key = 'first_name',
                           dataset_value = 'nino')

empty_lookup.source.show()

#------------------------------------------------------------------------
## Second example: No source lookup, flagged when trying to add to lookup
## Small hard coded dataset used above
#------------------------------------------------------------------------

# Intialise lookup
empty_lookup = adr.Lookup(key = 'name', 
                          value = 'id')


# Get dataset to append
dataset_to_append = test_dataset_two


# Update empty lookup with data from new dataset
empty_lookup.add_to_lookup(dataset = dataset_to_append, 
                           dataset_key = 'first_name',
                           dataset_value = 'nino')

# Above fails as no source for lookup
empty_lookup.create_lookup_source(spark)

# Try again to update empty lookup with data from new dataset
empty_lookup.add_to_lookup(dataset = dataset_to_append, 
                           dataset_key = 'first_name',
                           dataset_value = 'nino')

empty_lookup.source.show()


#-----------------------------------------------------------------------
## Third example: Source lookup, updated with dataset with key and value
## Simply find values to add and append
## Small hard coded dataset used above
#-----------------------------------------------------------------------

# Create source lookup
source = test_dataset_one


# Intialise lookup 
non_empty_lookup = adr.Lookup(key = 'name', 
                              value = 'id', 
                              source = source)


# Get dataset to append
dataset_to_append = test_dataset_two

# Update empty lookup with data from dataset two, where value has been provided
non_empty_lookup.add_to_lookup(dataset = dataset_to_append,
                               dataset_key = 'first_name',
                               dataset_value = 'nino')

non_empty_lookup.source.show()


#-------------------------------------------------------------------------------
## Fourth example: Source lookup, updated with dataset with only key
## As only dataset key provided, value will be generated when update takes place
## ## Small hard coded dataset used above
#-------------------------------------------------------------------------------


# Create source lookup
source = test_dataset_one


# Intialise lookup 
non_empty_lookup = adr.Lookup(key = 'name', 
                              value = 'id', 
                              source = source)


# Get dataset to append
dataset_to_append = test_dataset_two

# Update empty lookup with data from dataset two, where value has been provided
non_empty_lookup.add_to_lookup(dataset = dataset_to_append,
                               dataset_key = 'first_name')

non_empty_lookup.source.show()


#-------------------------------------------------------------------------------
## Fifth example: Source lookup, updated with dataset with only key
## As only dataset key provided, value will be generated when update takes place
## Larger dataset imported from hdfs
## Checks on number of records updated
#-------------------------------------------------------------------------------


data = spark.read.csv(
  "/ons/det_training/sd2011.csv",
  header = True, inferSchema = True
)


# create two smaller datasets for testing purposes
source_data = data.sample(0.05)
new_data = data.sample(0.05)


# create lookup to be appended using anonymise_ids
source_data_anonymised = adr.anonymise_ids(source_data, 
                                           ['id'])


# initialise lookup
# NOTE: assumes anonymise_ids created adr_id not adr_id_new
lookup = adr.Lookup(key = 'id', 
                    value = 'adr_id', 
                    source = source_data_anonymised)


# Try to append with a key that is a different type e.g. BMI is double rather than int
lookup.add_to_lookup(dataset = new_data,
                     dataset_key = 'bmi')

# Try again with matching key types
lookup.add_to_lookup(dataset = new_data,
                     dataset_key = 'id')

lookup.source.show()


# Check correct amount of rows have been added

# calculate the total number of rows that should be expected after append takes place
source_data_columns = list(source_data.select('id').toPandas()['id'])
new_data_columns = list(new_data.select('id').toPandas()['id'])

total_rows = len(list(set(source_data_columns + new_data_columns)))

if (lookup.source.count() != total_rows):
  print('incorrect append')


#-------------------------------------------------------------------------------
## Sixth example: Removing records from lookup
#-------------------------------------------------------------------------------  
  
df1 = adr.make_test_df(spark)

# create lookup by chaining
disco = adr.Lookup(key = 'key', value = 'value')\
.create_lookup_source(cluster = spark)\
.add_to_lookup(dataset = df1, dataset_key = 'strVar', dataset_value = None)


# try to add key, fails due to different type
disco.add_to_lookup(df1, 'numVar')

# try again with another key
# we see two nulls here when using show(). This is because pysaprk classifies different
# empty rows into null. If you use toPandas() and look at original data, you see that
# these two rows actually empty different empty values
disco.add_to_lookup(df1, 'strNumVar')
disco.source.show()
disco.source.toPandas()

# remove set of values
keys_to_remove = (1,2,3)

disco.remove_from_lookup(keys_to_remove)

disco.source.show()

  

#---------------------------
# Check some type/value errors
#---------------------------

# key must be string
key_fail = adr.Lookup(['id'], 'adr_id')

# value must be string
value_fail = adr.Lookup('id', 2345)

# column and value must be in existing lookup
key_value_contained_in_existing = adr.Lookup(key = 'name', 
                                             value = 'age', 
                                             source = test_dataset_one)

# unique column in existing lookup
# add both created spark dataframes together to create duplicate values in 'name' column

duplicates_name_df = test_dataset_one.union(test_dataset_two)
unique_column = adr.Lookup(key = 'name', 
                           value = 'id', 
                           source = duplicates_name_df)

# schemas dont match
# checked above

# Exceptions in add_to_lookup method
lookup.add_to_lookup(dataset = test_dataset_one,
                     dataset_key = 1234)

lookup.add_to_lookup(dataset = test_dataset_one,
                     dataset_key = 'name', 
                     dataset_value = 1234)

lookup.add_to_lookup(dataset = test_dataset_one,
                     dataset_key = 'test', 
                     dataset_value = 1234)

lookup.add_to_lookup(dataset = test_dataset_one,
                     dataset_key = 'id', 
                     dataset_value = '1234')

lookup.add_to_lookup(dataset = duplicates_name_df,
                     dataset_key = 'name',
                     dataset_value = 'id')

lookup.remove_from_lookup(keys_to_remove = [1,2,3])

