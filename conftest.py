"""
WHAT IT IS: PySpark (python) script
WHAT IT DOES: Stanard pytest file so that tests from multiple modules in the directory can access
the fixture function. Creates a single spark session for all tests (scripts starting with test_) and
when all of them have been run, the session is closed. This script needs to sit in the root of the
working directory.
AUTHOR: Nathan Shaw
CREATED: 19/02/2022
LAST UPDATE: 22/02/2022 
"""

from pyspark.sql import SparkSession
import pytest

# The pytest fixture prepares, and manages, the environment for testing - in this case create and stopping
# a spark session.
# To ensure the spark session is visible for all tests accross many modules, scope variable is set to session
# implying the spark session stays open for the entirity of the test.
# See here for further info: https://docs.pytest.org/en/6.2.x/fixture.html#funcargs
@pytest.fixture(scope='session')
def spark_context():
  
    spark = (
      SparkSession.builder.appName("adruk_tests")
      .config("spark.sql.shuffle.partitions", 10)
      .enableHiveSupport()
      .getOrCreate()
    )
    
    yield spark
    
    spark.stop()