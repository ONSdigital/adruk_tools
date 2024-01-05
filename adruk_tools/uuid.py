from pyspark.sql import functions as F
import pandas as pd
import uuid

def make_python_uuids(number_values_required:int)->list:
  
  """
  creates UUIDs (Universal Unique IDs), based on uuid4()
  
  language
  --------
  python
  
  returns
  -------
  uuid values
  
  return type
  -----------
  list
  
  author
  ------
  Alex Anthony
  
  date
  ------
  14/12/2023
  
  version
  -------
  0.0.1
  
  parameters
  ----------
  number_values_required = the number of uuids to generate
  `(datatype = integer)`, e.g. 10

  example
  -------
  >>> make_python_uuids(number_values_required = 10)
  """

  return [uuid.uuid4() for element in range(number_values_required)]



def make_pyspark_uuids():    
  """
  creates UUIDs (Universal Unique IDs)

  language
  --------
  pyspark

  returns
  -------
  1 uuid value for each row of the spark dataframe it is used on

  return type
  -----------
  column object

  author
  ------
  Shedrack Ezu

  date
  ------
  14/12/2023

  version
  -------
  0.0.1

  example
  -------
  >>> df.withColumn('new_id', make_pyspark_uuids())

  sample output:
  --------------
  +----+---+--------------------+
  |  ID|age|              new_id|
  +----+---+--------------------+
  | AA3| 23|8ecc1648c3cd33f91...|
  | AA8| 32|7a1b295804c9effcf...|
  | AA4| 44|1c920b721a1f094fb...|
  | AA3| 61|6664b717b66fa3234...|
  |null| 44|71ee45a3c0db9a986...|
  +----+---+--------------------+

  """
  return F.expr("uuid()")
