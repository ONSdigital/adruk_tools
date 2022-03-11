from importlib.machinery import SourceFileLoader
adr = SourceFileLoader("adr_functions", "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py").load_module()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession.builder.appName("easy_pipeline_nathan")
	.config("spark.sql.shuffle.partitions", 1)
    .enableHiveSupport()
    .getOrCreate()
)


#----------------------------
# start tests
#----------------------------

# Basic hard coded datasets
# Initialise mock dataframes

test_columns = ['name', 'id']

test_rows1 = [('Nathan', 'A'),
     ('Joanna', 'B'),
     ('Tom', 'A')]

test_rows2 = [
     ('Nathan', 'A'),
     ('Mike', 'A'),
     ('Silvia', 'E'),
     ('Sophie', 'B'),
     ('Ben', 'E')]

test_dataset_one = spark.createDataFrame(test_rows1, test_columns)
test_dataset_two = spark.createDataFrame(test_rows2, test_columns)

#-----------------------------------------------------------
## First example: No lookup source, created and then updated
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
empty_lookup.update_lookup(cluster = spark, 
                           dataset = dataset_to_append, 
                           dataset_key = 'name',
                           dataset_value = 'id')

empty_lookup.source.show()

#------------------------------------------------------------------------------------
## Second example: No source lookup, updated
## create_lookup_source method takes place in update method as well incase user forgot
## left method in as could be useful in other workflows involving lookups
## Small hard coded dataset used above
#------------------------------------------------------------------------------------

# Intialise lookup
empty_lookup = adr.Lookup(key = 'name', 
                          value = 'id')


# Create lookup source
# Not needed as called in update
#if empty_lookup.source is None:
#  empty_lookup.create_lookup_source(spark)

  
# Get dataset to append
dataset_to_append = test_dataset_two


# Update empty lookup with data from new dataset
empty_lookup.update_lookup(cluster = spark, 
                           dataset = dataset_to_append, 
                           dataset_key = 'name',
                           dataset_value = 'id')

empty_lookup.source.show()


#-----------------------------------------------------------------------
## Third example: Source lookup, updated with dataset with key and value
## Simply append and de-depulicate lookup and dataset
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
non_empty_lookup.update_lookup(cluster = spark,
                               dataset = dataset_to_append,
                               dataset_key = 'name',
                               dataset_value = 'id')

non_empty_lookup.source.show()

##########################################
# GOT TO HERE
##########################################

#-------------------------------------------------------------------------------
## Fourth example: Source lookup, updated with dataset with only key
## As only dataset key provided, value will be generated when update takes place
## Larger dataset imported from HDFS
#-------------------------------------------------------------------------------





  




data = spark.read.csv(
  "/ons/det_training/sd2011.csv",
  header = True, inferSchema = True
)


# create two smaller datasets for testing purposes
source_data = data.sample(0.01)
new_data = data.sample(0.01)


# calculate the total number of rows that should be expected after append takes place
source_data_columns = list(source_data.select('id').toPandas()['id'])
new_data_columns = list(new_data.select('id').toPandas()['id'])

total_rows = len(list(set(source_data_columns + new_data_columns)))


# create lookup to be appended using anonymise_ids
source_data_anonymised = adr.anonymise_ids(spark, source_data, ['id'])


# initialise lookup
# NOTE: assumes anonymise_ids created adr_id not adr_id_new
source_lookup = adr.Lookup(column = 'id', value = 'adr_id', existing = source_data_anonymised)


# Set existing lookup attribute if empty
# Note: Not needed but keeping in for consistency with above example
if source_lookup.existing is None:
  source_lookup.existing = source_lookup.create_lookup(spark, schema)

  
#--------------------------------------------
# Try to append new data to source lookup
# NOTE: should fail as schemas dont match

lookup = source_lookup.update_lookup(new_data)
#---------------------------------------------


# So create lookup of same scheme
new_data_anonymised = adr.anonymise_ids(spark, new_data, ['id'])


# Try to append again
lookup = source_lookup.update_lookup(new_data_anonymised)


# Check correct amount of rows have been added
if (lookup.count() != total_rows):
  print('incorrect append')

  
lookup.show()


#---------------------------
# Check some type/value errors
#---------------------------

# column must be string
column_fail = adr.Lookup(['id'], 'adr_id')

# value must be string
value_fail = adr.Lookup('id', 2345)

# column and value must be in existing lookup
column_value_contained_in_existing = adr.Lookup(column = 'name', value = 'age', existing = dataset_source)

# unique column in existing lookup
# add both created spark dataframes together to create duplicate values in 'name' column

duplicates_name_df = dataset_source.union(dataset_to_add)
unique_column = adr.Lookup(column = 'name', value = 'id', existing = duplicates_name_df)

# schemas dont match
# checked above

# test new exceptions i update method.

