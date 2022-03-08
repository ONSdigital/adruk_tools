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


# create instance of lookup

# import sd2011 table lookup into memory

# add this to empty lookup dataframe






test_columns = ['name', 'id', 'age', 'bmi', 'year']

test_rows = [('Nathan', 'A', 23, 45.679, '2008'),
     ('Joanna', 'B', 63, 25.080, '2008'),
     ('Tom', 'A', 89, 99.056, '2008'),
     ('Nathan', 'C', 23, 45.679, '2008'),
     ('Nathan', 'A', 23, 45.679, '2008'),
     ('Johannes', 'E', 67, 25.679, '2009'),
     ('Nathan', 'B', 23, 45.679, '2009'),
     ('Johannes', 'E', 67, 45.679, '2009'),
     ('Nathan', None, 23, 45.679, '2009'),
     ('Nathan', 'C', 23, 45.679, None),
     (None, 'F', 89, 99.056, '2008')]

dataset = spark.createDataFrame(test_rows, test_columns, schema)	

schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("id", T.IntegerType(), False),
    T.StructField("age", T.FloatType(), False),
    T.StructField("bmi", T.FloatType(), False),
    T.StructField("year", T.StringType(), False)])