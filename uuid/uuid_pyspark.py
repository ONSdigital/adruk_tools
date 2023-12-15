from pyspark.sql import functions as F
import uuid

def make_pyspark_uuids():
    df_col = F.expr("uuid()")
    
    """
    Creates UUIDs (Universal Unique IDs) as a spark column object that can be used to create an id column within a dataframe.
    
    language
    --------
    Pyspark
    
    returns
    -------
    unique set of uuid values
    
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
    
    parameters
    ----------
    Does not take any parameters, uuid are created per row or data
  
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
    
    return df_col
