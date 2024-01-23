# TO DO: Make unit test proper
# Provide explicit file path to updated function

repo_path = '/home/cdsw/adruk_tools/tests'
import sys

# map local repo so we can import local libraries
sys.path.append(repo_path)

import conftest as ct
import pyspark.sql as ps
import adruk_tools.adr_functions as adr
import pytest

from pyspark.sql import SparkSession

cluster = (SparkSession.builder.appName("lookup_class_uuid_tests")
           .config("spark.sql.shuffle.partitions", 1)
           .enableHiveSupport()
           .getOrCreate())

# Make test data

# 5 known unique id's in 10 rows of data

unique_ids = 5

test_columns = ['name', 'id', 'age', 'bmi', 'year']

test_rows = [('Nathan', 'A', 23, 45.679, '2008'),
            ('Joanna', 'B', 63, 25.080, '2008'),
            ('Tom', 'A', 89, 99.056, '2008'),
            ('Nathan', 'C', 23, 45.679, '2008'),
            ('Nathan', 'A', 23, 45.679, '2008'),
            ('Johannes', 'E', 67, 25.679, '2009'),
            ('Nathan', 'B', 23, 45.679, '2009'),
            ('Johannes', 'E', 67, 45.679, '2009'),
            ('Nathan', 'C', 23, 45.679, None),
            (None, 'F', 89, 99.056, '2008')]

dataset = cluster.createDataFrame(test_rows, test_columns)

lookup = adr.Lookup(key = 'id', 
                  value = 'adr_id')

lookup.create_lookup_source(cluster = cluster)

lookup.add_to_lookup(dataset = dataset,
                             dataset_key = 'id')

# Does the lookup contain expected number of rows?
assert lookup.source.count() == unique_ids, "Lookup contains incorrect number of rows"

""" 
lookup.source.show() 

+---+--------------------+
| id|              adr_id|
+---+--------------------+
|  A|559aead08264d5795...|
|  B|df7e70e5021544f48...|
|  C|6b23c0d5f35d1b11f...|
|  E|a9f51566bd6705f7e...|
|  F|f67ab10ad4e4c5312...|
+---+--------------------+
"""