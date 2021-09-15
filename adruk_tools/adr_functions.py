def session_small():
  """
  Small Session

  This session is similar to that used for DAPCATS training
  It is the smallest session that is realistically used

  Details:
      Only 1g of memory and 3 executors
      Only 1 core
      Number of partitions are limited to 12, which can improve
      performance with smaller data

  Use case:
      Simple data exploration of small survey data

  Example of actual usage:
      Used for DAPCATS PySpark training
      Mostly simple calculations
  """
  from pyspark.sql import SparkSession

  return (
      SparkSession.builder.appName("small-session")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.cores", 1)
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.maxExecutors", 3)
      .config("spark.sql.shuffle.partitions", 12)
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.ui.showConsoleProgress", "false")
      .enableHiveSupport()
      .getOrCreate()
  )

def session_medium():
  """
  Medium Session

  A standard session used for analysing survey or synthetic
  datasets. Also used for some Production pipelines based on
  survey and/or smaller administrative data.

  Details:
      6g of memory and 3 executors
      3 cores
      Number of partitions are limited to 18, which can improve
      performance with smaller data

  Use case:
      Developing code in Dev Test
      Data exploration in Production
      Developing Production pipelines on a sample of data
      Running smaller Production pipelines on mostly survey data

  Example of actual usage:
      Complex calculations, but on smaller synthetic data in
          Dev Test
  """
  from pyspark.sql import SparkSession

  return (
          SparkSession.builder.appName("medium-session")
          .config("spark.executor.memory", "6g")
          .config("spark.executor.cores", 3)
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.dynamicAllocation.maxExecutors", 3)
          .config("spark.sql.shuffle.partitions", 18)
          .config("spark.shuffle.service.enabled", "true")
          .config("spark.ui.showConsoleProgress", "false")
          .enableHiveSupport()
          .getOrCreate()
        )

def session_large():
  """
  Large Session

  Session designed for running Production pipelines on large
  administrative data, rather than just survey data. Will often
  develop using a smaller session then change to this once the
  pipeline is complete.

  Details:
      10g of memory and 5 executors
      1g of memory overhead
      5 cores, which is generally optimal on larger sessions

  Use case:
      Production pipelines on administrative data
      Cannot be used in Dev Test, as 9 GB limit per executor

  Example of actual usage:
      One administrative dataset of 100 million rows
      Many calculations
  """
  from pyspark.sql import SparkSession

  return (
      SparkSession.builder.appName("large-session")
      .config("spark.executor.memory", "10g")
      .config("spark.yarn.executor.memoryOverhead", "1g")
      .config("spark.executor.cores", 5)
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.maxExecutors", 5)
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.ui.showConsoleProgress", "false")
      .enableHiveSupport()
      .getOrCreate()
  )

  
def session_xl():
  """
  Extra Large session

  Used for the most complex pipelines, with huge administrative
  data sources and complex calculations. Uses a large amount of
  resource on the cluster, so only use when running Production
  pipelines

  Details:
      20g of memory and 12 executors
      2g of memory overhead
      5 cores; using too many cores can actually cause worse
          performance on larger sessions

  Use case:
      Running large, complex pipelines in Production on mostly
          administrative data
      Do not use for development purposes; use a smaller session
          and work on a sample of data or synthetic data

  Example of actual usage:
      Three administrative datasets of around 300 million rows
      Significant calculations, including joins and writing/reading
          to many intermediate tables
  """

  return (
      SparkSession.builder.appName("xl-session")
      .config("spark.executor.memory", "20g")
      .config("spark.yarn.executor.memoryOverhead", "2g")
      .config("spark.executor.cores", 5)
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.maxExecutors", 12)
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.ui.showConsoleProgress", "false")
      .enableHiveSupport()
      .getOrCreate()
  )

class manifest:
  """
  :WHAT IT IS: Python class
  :WHAT IT DOES: 
  * creates an object of class 'manifest'
  * assign several methods to the object
  * designed extract data from nested dictionaries, in particular .mani files on HDFS
  :AUTHOR: Johannes Hechler
  :DATE: 09/02/2021
  :VERSION: 0.1
  """

  # give the object 
  def __init__(self, path):
    """
    :WHAT IT IS: Python method for objects of class 'manifest'
    :WHAT IT DOES: 
    * generates base property for object, i.e. reads in the specified file from HDFS into a pandas dataframe
    
    :AUTHOR: Johannes Hechler
    :DATE: 09/02/2021
    :VERSION: 0.1
    """
    import pydoop.hdfs as hdfs
    import pandas as pd
    
    with hdfs.open(path, "r") as f:
      self.content = pd.read_json(f)
      f.close()
        
  def whole(self): 
    """
    :WHAT IT IS: Python method for objects of class 'manifest'
    :WHAT IT DOES: 
    
    * generates property 'whole', i.e. information about the overall delivery, as a pandas dataframe with 1 row
    :AUTHOR: Johannes Hechler
    :DATE: 09/02/2021
    :VERSION: 0.1
    """
    return self.content.iloc[0]
      
  def parts(self, variable):
    """
    :WHAT IT IS: Python method for objects of class 'manifest'
    :WHAT IT DOES: 
    * generates property 'parts', i.e. information about the individual files included in a delivery, as a pandas dataframe with as many rows as there are files
    :AUTHOR: Johannes Hechler
    :DATE: 09/02/2021
    :VERSION: 0.1
    """
    import pandas as pd
    return pd.DataFrame(list( self.content[ variable ]))
  
  
def unzip_to_csv(file_path,file_name,destination_path):
  '''
  :WHAT IT IS: PYSPARK FUNCTION
  
  :WHAT IT DOES:
  * unzips a .csv.gz file into cdsw from a chosen location
  * puts the resulting unzipped csv into a user defined destination folder, 
  * then deletes the original file from CDSW.
  
  :OUTPUT: csv file
  
  
  :NOTES:
  * will only work on zipped files which are less than 10GB
  
  :TESTED TO RUN ON: zipped csv dataframe

  :AUTHOR: Sophie-Louise Courtney
  :DATE: 18/01/2021
  :VERSION: 0.0.1


  :PARAMETERS:
  * file_path = path to file
      `(datatype = 1 string)`, e.g. '/dapsen/landing_zone'
  * file_name = full name of the file 
      `(datatype = 1 zipped dataframe)`, e.g. 'test_results.csv.gz'
  * destination_path = path to folder where the unzipped file will be stored
      `(datatype = 1 string)`, e.g. '/dapsen/workspace_zone'
      
  :EXAMPLE:
  >>> unzip_to_csv(file_path = adr_directory,
                      file_name = file,
                      destination_path = out)
  '''
  import os
  
  #separate file name from the extentions (e.g. 'file_name.csv.gz' will become 'file_name')
  file_trunc_name = file_name.split(".")[0]
  
  # file in imported into cdsw, unzipped, put into destination folder and removed from cdsw
  os.system(f"hdfs dfs -cat {file_path}/{file_trunc_name}.csv.gz | gzip -d | hdfs dfs -put - {destination_path}/{file_trunc_name}.csv")

  
def save_sample(dataframe, sample_size, filepath, na_variables = []):
	
	"""
  :WHAT IT IS: PYSPARK FUNCTION
  
  :WHAT IT DOES: draws as user-specified number of records from the top of a dataset and saves them to a selected location in csv format
  :RETURNS: nothing in memory; writes out a comma-separated file
  :OUTPUT VARIABLE TYPE: not applicable
  
  :TESTED TO RUN ON: spark dataframe from covid test and trace dataset

  :AUTHOR: Ben Marshall-Sheen, Johannes Hechler
  :DATE: 17/12/2020
  :VERSION: 0.0.1
  :KNOWN ISSUES: None
  
  :PARAMETERS:
  * dataframe = spark dataframe
      `(datatype = dataframe name, no string)`, e.g. ctas_data
  * sample_size = how many rows to take from dataset. Default values = 20.
      `(datatype = numeric)`, e.g. 20
  * filepath = the directory and filename for the file to be written to.
      `(datatype = string)`, e.g. "/dapsen/workspace_zone/adruk/sample.csv"
	* na_variables = if you want to exclude records with missing data from the sample, you can specify the names of columns to check for missingness. Records are removed if ANY of the selected variables is missing. Optional. Default value = []
      `(datatype = list of strings)`, e.g. ['age', 'sex]
      
  :EXAMPLE:
  >>> save_sample( dataframe = pii_data, 
                   sample_size = 20, 
                   filepath = '/dapsen/workspace_zone/my_project/sample.csv)))
	"""
	
	dataframe = dataframe.na.drop(subset=na_variables, how = 'any') #This filters out na.drop values.
  
	out = dataframe.limit(sample_size) # draws the sample
	
	return (out.coalesce(1) # This writes out the sample to csv to the location called file_path.
          .write
          .csv(filepath,
               sep = ',',
               header = "true",
               mode='overwrite'))

  
  
  
  
  
  
  
def make_test_df(session_name):
  """
  :WHAT IT IS: Function
  :WHAT IT DOES: creates a dataframe with several columns of different data types for testing purposes. Intentionally includes various errors, e.g. typos.
  :RETURNS: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 27/08/2019
  :VERSION: 0.1

  :PARAMETERS:
    :session_name = name of the spark session to use:
      `(datatype = session name, unquoted)`, e.g. spark

  :EXAMPLE:
  >>> make_test_df(spark)

  """

  columns = ['strVar', 'numVar', 'strNumVar', 'postcode', 'postcodeNHS', 'dob', 'name'] #set up variable names
  values = [('A', 1, '1', 'KT1 9AR' , 'ZZ99"3CZ', '1999-01-03', 'MR Name'), #create data for both variables, row by row
            (' A', 2, '2', 'PO4 9HJ' , 'PO4 9HJ', 'UNK', 'MRS NAME'),
            ('A ', 3, '3',  'QO4 9HJ' , 'ZZ994QZ', '1999/01/02', 'MISS name'), #dob: only plausible date in list, show that to_date() has an inbuilt plausibility check
            (' A ', 4, '4',  'SO10 9-K' , 'ZZ994UZ', '2019-10-15', 'ms naMe'), #test if hyphens get removed
            ('', 5, '5',  'E$1 0SM' , 'ZZ997RZ', '2019-10-16', 'MSTR   NaMe '), #test if postcodes of correct format but with illegal characters are rejected
            (None, 6, '6',  'Q4 2WQ' , None, '1999/01/42', 'DR  naME  '),#test postcode sectors with 1 letters, 1 numbers
            ('null', 7, '7',  'ZZ99 3WZ' , 'KE1    6HD',  '1999/01/42', 'PROF name'), #test NHS generic postcode
             ('NaN', 13, '14',  'OE1    4KQ' , 'ZZ9 94E', '1999/01/42', '   PROF NAME'), #to test if full duplicates get removed
            ('NaN', 14, '15',  'oe1 4KQ' , '  ZZ994E', '1999/01/42', '   SIR   NaMe'), #to test if full duplicates get removed
            ('EN4 8XH', 15,  '16', 'EN4 8XH' , '  ZZ99  4E  ', '1999/01/42', '  MR name'),
            (None, None, None,  None, None, None, None) #to test if empty rows get removed
           ]
  return session_name.createDataFrame(values, columns) #create pyspark dataframe with variables/value from above




 
def generate_ids(session, df, id_cols, start_year, id_len = None):
  """
  :WHAT IT IS: pyspark function
  :WHAT ID DOES: recodes a given column to random numerical values
  :WHY IT DOES IT: to anonymise ID variables in ADRUK projects
  :RETURNS: dataframe with 1 new column called 'adr_id', holding the new ID
  :OUTPUT VARIABLE TYPE: spark dataframe
  :KNOWN ISSUES: input dataset must not have existing column called 'adr_id'

  :AUTHOR: David Cobbledick
  :DATE: 2020
  :VERSION: 0.0.1


  :PARAMETERS:
    : session = name of current spark cluster:
      `(datatype = cluster name, no string)`, e.g. spark
    : df = spark dataframe:
      `(datatype = dataframe name, no string)`, e.g. PDS
    : id_cols = column(s) to use for new ID:
      `(datatype = list of strings)`, e.g. ['year', 'name']
    : start_year = name of additional column(s) to use as (unchanged) prefix in ID:
      `(datatype = list of strings)`, e.g. ['year', 'name']
    : id_len = set uniform length of ID values if required. Pads out values with leading zeroes if needed. Default value = None, i.e. accept different lengths:
      `(datatype = numeric)`, e.g. 9

  :EXAMPLE:
  >>> generate_ids(sessions = spark, 
                    df = AEDE, 
                    id_cols = ['name', 'ID'],
                    start_year = ['year'], 
                    id_len = 9)
  """
  
  
  #==========================================================================
  """LOAD REQUIRED PACKAGES"""
  #==========================================================================
  import pyspark.sql.functions as F   # generically useful functions package
  import pyspark.sql.types as T    # package to create columns of specific type
  import pyspark.sql.window as W   # package used for linking old to new IDs
  import random   # package used to generate random numbers for new IDs

  
  
  #==========================================================================
  """CHECK INPUTS AND PREPARE INPUT DATA"""
  #==========================================================================
  
  # check that the ID columns were passed as a list, and if not, make it one
  if type(id_cols)!=list:
    id_cols = [id_cols]
  
  # check that the columns expressing which period an ID first appeared were passed as a list, and if not, make it one
  if type(start_year)!=list:
    start_year = [start_year]
  
  # reduce dataframe to only the ID and the Year columns. then remove records where the same ID was used more than once in the year it was first used.
  df = df.select(id_cols + start_year)
  df = df.drop_duplicates()
  
  # count how many IDs ( = people) are left, i.e. how many need a new ID generated
  n_persons = df.count()

  
  
  #==========================================================================
  """CREATE RANDOM VALUES FOR NEW IDs"""
  #==========================================================================
  
  # if you don't care whether your numbers will have a specific length of digits
  if id_len is None:
    id_list = random.sample(range(n_persons), # how many numbers to generate
                            n_persons)        # how many of those numbers to pick
    
  # if you want your numbers to have a specific lenght. NB sometimes the numbers will shorter - these values are padded out later
  # generates numbers up to 10 to a chosen power.
  # using range() means there are no duplicates in the numbers that get sampled from, i.e. sampling is without replacement
  # abs() is a safeguard in case users passed a negative value
  else:
    id_list = random.sample(range(1*10**abs(id_len)),
                            n_persons)

  # turn the base Python list into a spark dataframe
  list_df = session.createDataFrame(id_list, T.IntegerType())

  # change the default column name to 'adr_id'
  list_df = list_df.withColumnRenamed('value','adr_id')

  
  #==========================================================================
  """MAIN DATASET: GIVE EACH OLD ID VALUE A UNIQUE BUT UNRELATED NUMBER TO LATER JOIN ON"""
  #==========================================================================
  # make a new, purely auxiliary columm called 'instance'. For now populated with the number 1, to be used in a calculation, then later deleted
  df = df.withColumn('instance',F.lit(1))
  
  # define a window function specification that...
  w = (W.Window
       .partitionBy('instance')   # for each unique value in the 'instance' column...
       .orderBy(id_cols)          # ... and ordered by the column of ID values created earlier ...
       .rangeBetween(W.Window.unboundedPreceding, 0))   # ... add as many to the previous group's value as there are records in the current groups

  # apply the window specification - essentially makes a (non-unique) ranking, where each group's rank number is the previous group's number, plus as the number of times that the current ID value appears in the data
  df = df.withColumn('cum_sum', F.sum('instance').over(w))

  # remove the auxiliary 'instance' column from the main dataframe
  df = df.drop('instance')

  
  
  #==========================================================================
  """NEW ID DATASET: GIVE EACH NEW ID VALUE A UNIQUE BUT UNRELATED NUMBER TO LATER JOIN ON"""
  #==========================================================================
  # make a new, auxiliary column called 'instance' in the auxiliary dataframe that holds the numbers created for use as IDs
  list_df = list_df.withColumn('instance',F.lit(1))

  # define a window function specification that is the same as for the main dataframe but...
  w = (W.Window
       .partitionBy('instance')
       .orderBy('adr_id')   # ... orders by the newly created ID values
       .rangeBetween(W.Window.unboundedPreceding, 0))

  # apply the window specification - essentially makes a (non-unique) ranking, where each group's rank number is the previous group's number, plus as the number of times that the current ID value appears in the data
  list_df = list_df.withColumn('cum_sum', F.sum('instance').over(w))

  # remove the auxiliary 'instance' column from the main dataframe
  list_df = list_df.drop('instance')

  
  #==========================================================================
  """ADD NEW ADR_ID VALUES TO MAIN DATAFRAME"""
  #==========================================================================
  # join the dataframe with the adr_id column onto the main dataframe
  # keeps only records whose cum_sum value exists in both dataframes
  # NB this by definition never creates duplicate records because the linkage variable 'cum_sum' is unique in the adr_id dataframe
  df = df.join(list_df,
               on  = 'cum_sum',
               how = 'inner')

  # remove auxiliary 'cum_sum' column
  df = df.drop('cum_sum')

  #==========================================================================
  """WHERE ADR_ID VALUES ARE NOT OF DESIRED LENGTH PAD THEM OUT WITH LEADING ZEROES"""
  #==========================================================================
  # if you don't care how many digits your new ID values ought to have...
  if id_len is None:
    n_characters = str(len(str(n_persons)))   # how many digits are in the number of records of the main dataframe - turn that from numeric into string
  
  # if you want the ID values to have a specific length
  else:
    n_characters = str(id_len)   # simply turn from numeric to string the number of digits you want to have in your new ID values

  # overwrite the existing 'adr_id' column in the main dataframe, that turns 
  # the numeric values to string, and adds leading zeros if they're shorter
  # than the selected number of digits
  # "%0" means 'potentially start with leading zeroes'
  # n_characters means 'if the original value isn't this long already
  # "d" : unclear what it does but without it spark throws a memory error
  df = df.withColumn("adr_id", F.format_string("%0"+n_characters+"d","adr_id"))

  
  
  #==========================================================================
  """ADD DELIVERY PERIOD TO ADR_ID"""
  #==========================================================================
  # overwrite the new ID column with a version of itself that has the delivery period added in front
  df = df.withColumn('adr_id',
                     F.concat( F.col( start_year[0] ),
                               F.col('adr_id')
                             )
                    )
  
  # remove from the main dataframe the (first) column used to specify the period an original ID value was added
  df = df.drop(start_year[0])

  #==========================================================================
  return df

	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
def complex_harmonisation(df, log = None):
  
  '''
  :WHAT IT IS: function
  :WHAT IT DOES:
  * where harmonisation leads to duplicate named variables within a dataset, this function harmonised to a single variable
  * a multiple record (_mr) flag is generated as an additional column to indicate if there is discrepancy in values for harmonised variables
  
  :USE: used in 05c_aggregate_hive_tables.py
  :AUTHOR: David Cobbledick
  :DATE: 08/01/2021
  '''
	
  import pandas as pd
  import pyspark.sql.functions as F
	
  
  dup_cols = pd.DataFrame({'dup_cols':df.columns})
  dup_cols = list((dup_cols[dup_cols.duplicated(['dup_cols'],keep=False)]
     .drop_duplicates()['dup_cols']))

  for col in dup_cols:

    df = df.toDF(*[y+'<<>>'+str(x) for x,y in enumerate(df.columns)])

    #dup_cols_raw = [x for x in df.columns if x.startswith(col)]
    dup_cols_raw = [x for x in df.columns if x.split('<<>>')[0]==col]

    df = df.withColumn(col+'_mr',
                     F.col(dup_cols_raw[0])!=F.col(dup_cols_raw[1]))

    harmonised_df = (df
               .select([x for x in df.columns if x not in dup_cols_raw])
               .withColumn(col,F.lit(None))
              .limit(0))

    #harmonised_df = (harmonised_df
    #                 .toDF(*[x.split('<<>>')[0] for x in harmonised_df.columns]))

    harmonised_df = (harmonised_df
                     .toDF(*[x.split('<<>>')[0] if x.split('<<>>')[0]==col
                             else x
                             for x in harmonised_df.columns]))  

    for col_raw in dup_cols_raw:

      temp_df = df.drop(col_raw)

      #temp_df = temp_df.toDF(*[x.split('<<>>')[0] for x in temp_df.columns])

      temp_df = (temp_df
                     .toDF(*[x.split('<<>>')[0] if x.split('<<>>')[0]==col
                             else x
                             for x in temp_df.columns]))  

      harmonised_df = harmonised_df.unionByName(temp_df).dropDuplicates()

    df = harmonised_df.toDF(*[x.split('<<>>')[0] for x in harmonised_df.columns])
  
  if log != None:
	  log.append(f"made {col} reflect _mr when duplicated")
	  return dataframe1, dataframe2, log
  else:
	  return df


	  
	  
def complex_standardisation(df, gender):
  
  '''
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: 
  * Enables more detailed secondary engineering of columns secified within the function
  
  :USE: used in 05c_aggregate_hive_tables.py
  :NOTES: 
  * This can be adapted to suit data and processing requirements
  * The examples below show application for standardising sex, name and postcode variables

  :AUTHOR: David Cobbledick
  :DATE: 08/01/2021
  '''
  import pyspark.sql.functions as F
	
  #========================================================================================
  #========================================================================================
  ''' Standardises gender'''
  #========================================================================================
  
  sex_cols = [x for x in df.columns if x in gender] 
  
  if len(sex_cols)!=0:
    
    male_regex = "(?i)^m$"
    female_regex = "(?i)^f$"
    other_regex = "(?i)^N$|(?i)^u$|0|9"
    #gender_null_regex = "N"
    
    for column in sex_cols:
    
      df = df.withColumn(column,F.regexp_replace(F.col(column),male_regex, '1'))
      df = df.withColumn(column,F.regexp_replace(F.col(column),female_regex, '2'))
      df = df.withColumn(column,F.regexp_replace(F.col(column),other_regex, '3'))
    
  #========================================================================================
  #========================================================================================
  ''' Standardises name columns'''
  #========================================================================================
    
  name_cols = [x for x in df.columns if x in ['FORENAME',
                                             'MIDDLENAMES',
                                             'SURNAME']]
  
  if len(name_cols)!=0:
    
    clean_name_regex = \
    "|".join(['^Mr.$','^Mrs.$','^Miss.$','^Ms.$','^Mx.$','^Sir.$','^Dr.$'])\
    +"|[^ A-Za-z'-]"
    
    for column in name_cols:
      
      df = df.withColumn(column,F.upper(F.col(column)))
      df = df.withColumn(column,F.trim(F.regexp_replace(F.col(column),clean_name_regex, "")))
      df = df.withColumn(column,F.trim(F.regexp_replace(F.col(column), " +", " ")))
       

  #========================================================================================
  #========================================================================================
  ''' Standardises postcode columns'''
  #========================================================================================
    
  postcode_cols = [x for x in df.columns if x in ['POSTCODE',
                                                 'HOMEPOSTCODE',
                                                 'WORKPOSTCODE']]
  
  if len(postcode_cols)!=0:
    
    postcode_regex = "[^A-za-z0-9]|[_]|[\^]"
    
    for column in postcode_cols:

      df = df.withColumn(column,F.trim(F.regexp_replace(F.col(column),postcode_regex, "")))    
      df = df.withColumn(column,F.upper(F.col(column)))

  #========================================================================================
  #========================================================================================    

  return df

  
  
  
  
  
  
  
def extended_describe(
	df,
	all_=True,
	trim_=False,
	active_columns_=False,
	sum_=False,
	positive_=False,
	negative_=False, 
	zero_=False, 
	null_=False, 
	nan_=False, 
	count_=False, 
	unique_=False, 
	special_=False, 
	blank_=False, 
	mean_=False, 
	stddev_=False, 
	min_=False, 
	max_=False,
	range_=False,
	mode_=False,
	length_mean_=False, 
	length_stddev_=False, 
	length_min_=False, 
	length_max_=False,
	length_range_=False,
	length_mode_=False,
	special_dict_=False,
	percent_=True,
	pandas_=False,
	axis_=0,
	fillna_=0
):
	"""  
	:WHAT IT IS: PYSPARK FUNCTION

	:WHAT IT DOES: This function extends all of the functions listed in parameters to apply on a dataset.
	:RETURNS: Pandas dataframe with information on the data in the specified dataframe.
	:OUTPUT VARIABLE TYPE:
  * Information on the data you have in the dataset as a pandas dataframe.


	:AUTHOR: David Cobbledick
	:DATE: 01/12/2020
	:VERSION: 0.0.1
	:KNOWN ISSUES: There are multiple columns that require boolean and some that do not work on boolean data.
	
		:PARAMETERS:
	* df = the dataframe that you are calling this on.
	* all = chooses that all are true and overwrites calling subfunctions.
	* trim = this imports the trim function to be used a sub functon.
	* Active_columns = 
	* Sum = The function sum_describe gets called in here.
	* Positive = The function posiitve_describe gets called in here.
	* Negative = The function negative_decribe gets called in here.
	* Zero = The function zero_describe gets called in here.
	* Null = The function null_describe gets called in here.
	* Nan = The function nan_describe gets called in here.
	* count = The function count gets called in here.
	* Unique = The function unique_descrbie gets called in here.
	* Special = The function special_describe gets called in here.
	* Blank = The function blank_describe gets called in here.
	* Mean = The function mean_describe gets called in here.
	* Stddev = The function stddev_describe gets called in here.
	* Min = The function min_descrie gets called in here.
	* Max = The function max_describe gets called in here.
	* Range =
	* Mode = The function mode_descrie gets called in here.
	* Length-mean = The length of the mean in mean_describe is set here.
	* Length-stddev = This sets the default ofr the length of Stddev.
	* Length-min = The function here describes the lenght of the min_describe function.
	* Length-max = The funciton defines the length of the max-describe function.
	* Length-range = 
	* Length-mode = The length of the mode_describe function is set here.
	* Special-dict = 
	* Percent = 
	* Pandas = 
	* Axis = 
	* Fillna = 
     
	:EXAMPLE:
		>>> raw_describe = extended_describe(raw_df,
                                         all_=False,
                                         trim_=True,
                                         active_columns_=True,
                                         null_=True,
                                         nan_=True,
                                         special_=True,
                                         special_dict_=nulls_dict,
                                         unique_=True,
                                         #length_max_=False,
                                         percent_=False,
                                         pandas_=True,
                                         axis_=1,
                                        fillna_=0)

 	"""

	
	import pandas as pd
	import numpy as np
	import string
	from ashe.setup.functions import clean_header
	import pyspark.sql.functions as F
	from pyspark.sql.types import (StructType, StructField, ArrayType, FloatType,
                               DoubleType, IntegerType, StringType, DateType)

	#================================================================================
	'''
	default output and determines numeric columns - so that numeric methodologies are
	only applied to the numeric columns where they are relevant

	the base output file is 'out' to which all other measurment output is merged
	'''
	#================================================================================

	count = df.count()
	column_count = len(df.columns)

	out = pd.DataFrame({'variable':[x[0] for x in df.dtypes],
											'type':[x[1] for x in df.dtypes],
											'total_columns':column_count,
											'total_rows':count})

	numeric_types = ['int',
									 'bigint',
									 'smallint',
									 'double',
									 'float',
									 'decimal']

	numeric_columns = list(out[out['type']
														 .isin(numeric_types)]['variable'])

	out_columns=['variable',
							 'type',
							 'total_rows',
							 'total_columns']

	#================================================================================
	'''
	option to trim whitespace
	'''
	#================================================================================

	if trim_==True:
		df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])

	#================================================================================
	'''
	application of measure sub functions depending on user arguments
	'''  
	#================================================================================

	if sum_==True or all_==True:
		if len(numeric_columns)==0:
			out['sum']=None
		else:
			sum_df = sum_describe(df.select(numeric_columns))
			out = out.merge(sum_df, on ='variable', how='left')
		out_columns.append('sum')

	if positive_==True or all_==True:
		if len(numeric_columns)==0:
			out['positive']=None
		else:
			positive_df = positive_describe(df.select(numeric_columns))
			out = out.merge(positive_df, on ='variable', how='left')

	if negative_==True or all_==True:
		if len(numeric_columns)==0:
			out['negative']=None
		else:
			negative_df = negative_describe(df.select(numeric_columns))
			out = out.merge(negative_df, on ='variable', how='left')

	if zero_==True or all_==True:
		if len(numeric_columns)==0:
			out['zero']=None
		else:
			zero_df = zero_describe(df.select(numeric_columns))
			out = out.merge(zero_df, on ='variable', how='left')

	if null_==True or active_columns_==True or count_==True or all_==True:
		null_df = null_describe(df)
		out = out.merge(null_df, on ='variable', how='left')

	if active_columns_==True or all_==True:
		null_column_count = out[out['total_rows']==out['null']].shape[0]
		active_column_count = column_count-null_column_count
		out['active_columns']=active_column_count
		out['null_columns']=null_column_count
		out_columns.append('active_columns')
		out_columns.append('null_columns')

	if count_==True or all_==True:
		out['count'] = out['total_rows']-out['null']
		out_columns.append('count')

	if (active_columns_==True or count_==True) and null_==False and all_==False:
		out = out.drop(['null'],axis=1)

	if nan_==True or all_==True:
		if len(numeric_columns)==0:
			out['NaN']=None
		else:
			nan_df = nan_describe(df.select(numeric_columns))
			out = out.merge(nan_df, on ='variable', how='left')

	if unique_==True or all_==True:
		unique_df = unique_describe(df)
		out = out.merge(unique_df, on ='variable', how='left')
		out_columns.append('unique')

	if (special_==True or all_==True) and special_dict_!=False:
		special_df = special_describe(df,special_dict_)
		out = out.merge(special_df, on ='variable', how='left')

	if blank_==True or all_==True:
		blank_df = blank_describe(df)
		out = out.merge(blank_df, on ='variable', how='left')

	if mean_==True or all_==True:
		if len(numeric_columns)==0:
			out['mean']=None
		else:
			mean_df = mean_describe(df.select(numeric_columns))
			out = out.merge(mean_df, on ='variable', how='left')
		out_columns.append('mean')

	if stddev_==True or all_==True:
		if len(numeric_columns)==0:
			out['stddev']=None
		else:  
			stddev_df = stddev_describe(df.select(numeric_columns))
			out = out.merge(stddev_df, on ='variable', how='left')
		out_columns.append('stddev')

	if min_==True or range_==True or all_==True:
		min_df = min_describe(df)
		out = out.merge(min_df, on ='variable', how='left')
		if min_==True or all_==True:
			out_columns.append('min')

	if max_==True or range_==True or all_==True:
		max_df = max_describe(df)
		out = out.merge(max_df, on ='variable', how='left')
		if max_==True or all_==True:
			out_columns.append('max')

	if range_==True or all_==True:
		range_df = out[out['type'].isin(numeric_types)]\
		.reset_index(drop=True)
		range_df['range'] = range_df['max']-range_df['min']
		range_df = range_df[['variable','range']]
		out = out.merge(range_df, on ='variable', how='left')
		out_columns.append('range')
		if min_==False and all_==False:
			out = out.drop(['min'],axis=1)
		if max_==False and all_==False:
			out = out.drop(['max'],axis=1)

	if mode_==True or all_==True:
		mode_df = mode_describe(df)
		out = out.merge(mode_df, on ='variable', how='left')
		out_columns.append('mode')

	#================================================================================
	'''
	if any measure of value length is selected in user arguments, a dataframe of 
	value lengths in the principle data frame is created and analysed
	'''

	#================================================================================

	if length_mean_==True or\
		length_stddev_==True or\
		length_mode_==True or\
		length_min_==True or\
		length_max_==True or\
		length_range_==True or\
		all_==True:

		df = df.na.fill('')  
		length_df = df.select(df.columns)
		for col in df.columns:
			length_df = length_df.withColumn(col+'_l',F.length(df[col]))
			length_df = length_df.drop(col)
		length_df = length_df.toDF(*[x[:-2] for x in length_df.columns])

	if length_mean_==True or all_==True:
		mean_length_df = mean_describe(length_df)
		mean_length_df.columns = ['variable','length_mean']
		out = out.merge(mean_length_df, on ='variable', how='left')
		out_columns.append('length_mean')

	if length_stddev_==True or all_==True:
		stddev_length_df = max_describe(length_df)
		stddev_length_df.columns = ['variable','length_stddev']
		out = out.merge(stddev_length_df, on ='variable', how='left')
		out_columns.append('length_stddev')

	if length_min_==True or length_range_==True or all_==True:
		min_length_df = min_describe(length_df)
		min_length_df.columns = ['variable','length_min']
		out = out.merge(min_length_df, on ='variable', how='left')
		if length_min_==True or all_==True:
			out_columns.append('length_min')

	if length_max_==True or length_range_==True or all_==True:
		max_length_df = max_describe(length_df)
		max_length_df.columns = ['variable','length_max']
		out = out.merge(max_length_df, on ='variable', how='left')
		if length_max_==True or all_==True:
			out_columns.append('length_max')

	if length_range_==True or all_==True:
		out['length_range'] = out['length_max']-out['length_min']
		out_columns.append('length_range')
		if length_min_==False and all_==False:
			out = out.drop(['length_min'],axis=1)
		if length_max_==False and all_==False:
			out = out.drop(['length_max'],axis=1)

	if length_mode_==True or all_==True:
		mode_length_df = mode_describe(length_df)
		mode_length_df.columns = ['length_mode','variable']
		out = out.merge(mode_length_df, on ='variable', how='left')
		out_columns.append('length_mode')

	#================================================================================
	'''dynamically orders output columns depemnding on user arguments'''
	#================================================================================

	fixed_columns = [
		'variable',
		'type',
		'total_rows',
		'total_columns',
		'active_columns',
		'null_columns',
		'sum',
		'mean',
		'stddev',
		'min',
		'max',
		'range',
		'mode',
		'unique',
		'length_mean',
		'length_stddev',
		'length_min',
		'length_max',
		'length_range',  
		'length_mode',
	]

	fixed_columns = [x for x in fixed_columns if x in out_columns]
	out = out.fillna(np.nan)

	if percent_==True:
		percent_columns = [x for x in list(out) 
											 if x not in fixed_columns]

		for column in percent_columns:
			out[column+'_%'] = [(x/count)*100 for x in out[column].astype(float)]

	out_columns = [x for x in list(out) if x not in fixed_columns]
	out_columns = fixed_columns + sorted(out_columns)

	out = out[out_columns]

	#================================================================================
	'''fills na depending on user argument'''
	#================================================================================
	if fillna_!=False: 
		out = out.fillna(fillna_)


	#================================================================================
	'''orientates output depending on user argument'''
	#================================================================================
	if axis_==0:
		out = (out.transpose()
					.reset_index())
		out.columns = ['summary']+(list(out.iloc[0])[1:])
		out = out.iloc[1:].reset_index(drop=True)

	#================================================================================
	'''outputs in pandas/spark depending on user argument'''
	#================================================================================
	if pandas_==False:
			out = df[list(df)].astype(str)
			out = spark.createDataFrame(out)

	return out


	

def spark_glob(host,directory):
  from pyspark.context import SparkContext as sc
  URI           = sc._gateway.jvm.java.net.URI
  Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
  FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
  Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
  
  fs = FileSystem.get(URI(host), Configuration())
  
  status = fs.listStatus(Path(directory))
  
  files = [str(fileStatus.getPath()) for fileStatus in status]
  
  return files

# returns files in directory
def spark_glob_all(host,directory):

  files = spark_glob(host,directory)

  for file in files:
    if len(files)==len(set(files)):
      files.extend(spark_glob(host,file))
    else:
      break

  files = list(set(files))
  
  return files