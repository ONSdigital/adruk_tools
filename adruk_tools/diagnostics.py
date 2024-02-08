import pyspark.sql.functions as F
import pyspark.sql.window as W
import pandas as pd
import numpy as np
import collections as co
import adruk_tools.adr_functions as adr

def duplicates_flag(columns_to_flag:list):
  """
  creates a boolean spark column object flagging duplicate values in some source columns
  
  language
  --------
  pyspark
  
  author
  ------
  johannes hechler; idea: elias kellow
  
  date
  ----
  31/07/2023
  
  version
  -------
  0.0.1
  
  return type
  -----------
  spark column object, boolean
  
  known issues
  ------------
  fails on NULL values. Always considers them unique.
  Due to F.count() behaviour.
  May be solved with higher versions of pyspark.
    
  parameters
  ----------
  * columns_to_flag: columns to check for duplication
    * type = list of column objects 
    * e.g. [F.col('column1'), F.trim('column2')]
  
  examples
  -------
  >>> duplicates_flag( columns_to_flag = [F.col('column1')] )
                                          
  >>> duplicates_flag( columns_to_flag = [F.col('column1'), 
                                          F.trim(F.col('column2'))] )
  """
  
  # specify how to group the dataframe
  window_spec = W.Window.partitionBy(columns_to_flag)

  # create flag
  # F.count() only accepts 1 string, so must concatenate in case there 
  # are several columns
  return F.count(F.concat(*columns_to_flag)).over(window_spec) >1


def list_columns_by_file(cluster, paths):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: records variable names by dataset, for a list of .csv files on HDFS

  :RETURNS:
  * 1 dictionary where key = file name, values = variable names
  * 1 dictionary where key = file name, values = row count

  :OUTPUT VARIABLES TYPE: Python dictionaries

  :AUTHOR: Johannes Hechler

  :VERSION: 0.0.1
  :DATE: 07/10/2021
  :CAVEATS: only runs on .csv files on HDFS

  :PARAMETERS:
  * cluster : an active spark cluster
      `(datatype = cluster name, no string)`, e.g. spark
  * paths : list of file paths to examine
      `(datatype = list of strings)`, e.g. ['/folder1/file1.csv', 'folder2/file2.csv']


  :EXAMPLE:
  >>> list_columns_by_file( cluster = spark, paths =  ['/folder1/file1.csv',
                                                       'folder2/file2.csv'])
  """

  # make an empty dictionaries to later add results to
  cols_by_file = {}  # ... for column names
  counts = {}  # ... for counts

  # read in and evaluate columns from each dataset in turn
  for path in paths:

      # tell users which file is being processed
      print("current file: " + path)

      # read in file
      current_file = cluster.read.format("csv").option("header", "True").load(path)

      # record file name (without folder path) and variable names in dictionary
      cols_by_file.update({path.split("/")[-1]: current_file.columns})
      counts.update({path.split("/")[-1]: current_file.count()})

  return cols_by_file, counts

  
def save_random_samples(dataframe, with_replacement, fraction, write_path, sample_size) :
  """
  :LANGUAGE: pyspark

  :WHAT IT DOES:
  * draws as user-specified number of records based on fraction and replacement from a spark dataframe
  * saves them to a selected HDFS location in csv format

  :RETURNS: nothing in memory; writes out a comma-separated file

  :TESTED TO RUN ON: spark dataframe from covid test and trace dataset

  :AUTHOR: Suresh Jebamalai Muthu
  :DATE: 17/11/2022
  :VERSION: 0.0.1
  :CHANGE FROM PREVIOUS VERSION:
  :KNOWN ISSUES: requires package pydoop

  :PARAMETERS:
  * dataframe = spark dataframe
      `(datatype = dataframe name, no string)`, e.g. ctas_data
  * with_replacement = Sample with replacement or not. Default = False
      `(datatype = boolean)`, e.g. True
  * fraction = Fraction of rows to generate, range [0.0 to 1.0]
      `(datatype = float)`, e.g. 0.7
  * sample_size = how many rows to take from dataset. Default values = 20.
      `(datatype = numeric)`, e.g. 20
  * write_path = the directory and filename for the file to be written to.
      `(datatype = string)`, e.g. "/dapsen/workspace_zone/adruk/sample.csv"

  :EXAMPLE:
  >>> save_random_samples( dataframe = pii_data,
                          with_replacement = True,
                          fraction = 0.001,
                          write_path = '/dapsen/workspace_zone/my_project/sample.csv',
                          sample_size = 20)
  """
  # drwas sanples based on replacement, fraction and converts it to a pandas dataframe
  data_samples = dataframe.sample(withReplacement = with_replacement, 
                                  fraction = fraction).limit(sample_size).toPandas()

  # write sample to the chosen HDFS file_path in comma-separate format.
  adr.pandas_to_hdfs(dataframe = data_samples, write_path = write_path)



def make_lookup_metrics(dataframe, 
                        key_column, 
                        value_column,
                        prefix):
  """
  collects 4 standard metrics of a lookup
  
  language
  -----------
  pyspark
  

  author
  -----------
  Johannes Hechler
  

  date
  -------------
  18/11/2022
  

  version
  -------------------
  0.1
  

  parameters
  ---------------------------------
  * dataframe: dataset to analyse, type = spark dataframe, e.g. my_lookup
  * key_column: name of column(s) to use as key(s), type = list of strings, e.g.  ['nhs_numbers']
  * value_column: name of column(s) to use as value(s),  type = list of strings,  ['hashed_id']
  * prefix: what to write in front of output dictionary keys to tell apart lookups, type = str, e.g. 'old_'


  returns
  ---------------
  dictionary with 4 metrics
  * total row count
  * number of duplicate keys
  * number of duplicate values
  * number of duplicate key-value pairs
  

  
  example
  ------------------------
  >>> make_lookup_metrics(dataframe = my_lookup, 
                          key_column = ['nhs_numbers'],
                          value_column = ['hashed_id'],
                          prefix = 'old_lookup_')
                          
  
  """
  # create empty dictionary to add metrics to
  metrics = co.OrderedDict()
  
  
  # record dataframe in worked node memory to speed up its repeated us
  dataframe.persist()
  
  # record total row numbers
  metrics[f'{prefix}rows'] = dataframe.count()

  # record row count minus number of distinct keys
  # should be 0
  metrics[f'{prefix}key_duplicates'] = (metrics[f'{prefix}rows'] -
                                       dataframe.
                                       dropDuplicates( subset = key_column).
                                       count()
                                       )

  # number of distinct values
  # likely less than distinct keys
  metrics[f'{prefix}value_duplicates'] = (metrics[f'{prefix}rows'] - 
                                          dataframe.
                                          dropDuplicates( subset = value_column).
                                          count()
                                         )

  # row number minus number of distinct key-value pairs
  # should be 0, because keys should be fully unique
  metrics[f'{prefix}keyvalue_duplicates'] = (metrics[f'{prefix}rows'] - 
                                             dataframe.
                                             dropDuplicates(subset = key_column + 
                                                                     value_column).
                                             count()
                                       )
  # remove dataframe from worker node memory
  dataframe.unpersist()

  return metrics



def columns_qa(dataframe_to_check,
               required_columns:list,
               output_prefix:str)->dict:
  """
  checks for missing and unexpected columns in a dataset
  
  author
  --------
  johannes hechler
  
  date
  --------
  08/12/2022
  
  version
  -------
  0.0.1
  
  returns
  -------
  dictionary with 2 keys, each prefixed with output_prefix
  * columns_missing
  * columns_unexpected
  
  parameters
  ----------
  dataframe_to_check = the dataset whose columns to check
    data type = spark dataframe
  
  required_columns = the columns you expect in the dataframe
    data type = list

  output_prefix = a string to put in front of the output dictionary keys
    data type = string

  
  example
  -------
  >>> columns_qa(dataframe_to_check = my_big_dataset,
                 required_columns = ['age', 'sex'],
                 output_prefix = 'my_big_dataset_')
  """
  # make empty dictionary to collect metrics in
  metrics = co.OrderedDict()
  
  # are any required columns missing from the dataframe?
  metrics[f"{output_prefix}columns_missing"] = np.setdiff1d(required_columns, 
                                                            dataframe_to_check.columns)

  # are there any additional columns?
  metrics[f"{output_prefix}columns_unexpected"] = np.setdiff1d(dataframe_to_check.columns, 
                                                               required_columns)

  return metrics


def countby(dataframe : pd.DataFrame(),
            grouping_column : str) -> pd.DataFrame():
  """
  count records in a dataframe, grouped by 1 column
  
  returns
  -------
  count of records, grouped by chosen column
  
  return type
  -----------
  pandas Dataframe
  
  author
  ------
  johannes hechler
  
  date
  ----
  03/11/2023
  
  version
  -------
  0.1
  
  parameters
  -------
  dataframe = the dataframe to count records in
  `(datatype = pandas Dataframe)`, e.g. my_dataframe
   grouping_column = name of column to group by
  `(datatype = string)`, e.g. 'sex'

  example
  -------
  >>> countby(dataframe = my_dataframe,
              grouping_column = 'sex')
  """
  # count records. NB NULL values are included as a group
  grouped_counts = pd.DataFrame(dataframe.
                                groupby(grouping_column,
                                        dropna = False).
                                size(),
                                columns = ['count'])
  
  sorted_counts = grouped_counts.sort_values(by = 'count',
                                             ascending = False,
                                             na_position = 'first') # shows NULL values on top

  return sorted_counts



