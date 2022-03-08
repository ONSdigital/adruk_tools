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

# calculate number of extra ids that will be added
source_data_columns = source_data.select('id').toPandas()['id']
new_data_columns = new_data.select('id').toPandas()['id']

extra = len(list(set(source_data_columns) - set(new_data_columns)))

# create lookup using anonymise_ids

anonymised_source = adr.anonymise_ids(spark, source_data, ['id'])

#initialise lookup

sd2011_source = adr.Lookup(column = 'id', value = 'adr_id', existing_lookup = anonymised_source)

# create EMPTY lookup if existing lookup not provided
if sd2011.existing_lookup is None:
  lookup = sd2011.create_lookup(spark, schema)
else:
  lookup = sd2011.existing_lookup

# update

# create new lookup to add
anonymised_new = adr.anonymise_ids(spark, new_data, ['id'])


updated_lookup = sd2011.update_lookup(dataset_to_add, lookup) 





# check exceptions

# types

# schema mismatch
