"""
WHAT IT IS: PySpark (python) script
WHAT IT DOES: pipeline to complete easy_pipeline task. JIRA ticket ONSDS-3235
AUTHOR: Nathan Shaw
CREATED: 02/02/2022
LAST UPDATE: 
"""

from pyspark.sql import SparkSession

import pytest


@pytest.fixture(scope='session')
def spark_context():
  
    spark = (
    SparkSession.builder.appName("harder_pipeline_nathan")
    .config("spark.sql.shuffle.partitions", 10)
    .enableHiveSupport()
    .getOrCreate()
    )
    
    yield spark
    
    spark.stop()