import pyspark.sql.functions as F
import pandas as pd
import collections as co
import adruk_tools.adr_functions as adr



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
