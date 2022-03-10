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
schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("id", T.StringType(), True)])



test_columns = ['name', 'id']

test_rows1 = [('Nathan', 'A'),
     ('Joanna', 'B'),
     ('Tom', 'A')]

test_rows2 = [
     ('Nathan', 'C'),
     ('Mike', 'A'),
     ('Silvia', 'E'),
     ('Sophie', 'B'),
     ('Ben', 'E')]


#-------------------------------------------
## First example with no lookup to append to
#-------------------------------------------

# Intialise lookup (existing = None)
empty_lookup = adr.Lookup(column = 'name', value = 'id')


# Set existing lookup attribute if empty
if empty_lookup.existing is None:
  empty_lookup.existing = empty_lookup.create_lookup(spark, schema)

  
# Create new dataset to add to lookup
dataset_to_add = spark.createDataFrame(test_rows2, test_columns, schema)	


# Update empty lookup with data from new dataset
lookup = empty_lookup.update_lookup(dataset_to_add)

lookup.show()

#-----------------------------------------
## Second example with lookup to append to
#-----------------------------------------

# Create existing lookup
dataset_source = spark.createDataFrame(test_rows1, test_columns, schema)	


# Intialise lookup 
non_empty_lookup = adr.Lookup(column = 'name', value = 'id', existing = dataset_source)


# Set existing lookup attribute if empty
# Note: Not needed but keeping in for consistency with above example
if non_empty_lookup.existing is None:
  non_empty_lookup.existing = non_empty_lookup.create_lookup(spark, schema)
 

# Update empty lookup with data from new dataset
lookup = non_empty_lookup.update_lookup(dataset_to_add)

lookup.show()
  

#------------------------------------
# Third example - using anonymise ids
#------------------------------------  


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


