Help on function complex_harmonisation in adruk_tools.adr_functions:

adruk_tools.adr_functions.complex_harmonisation = complex_harmonisation(df, log=None)
    WHAT IT IS: function
    WHAT IT DOES: - where harmonisation leads to duplicate named variables within a 
    dataset, This function harmonised to a single variable
    - a multiple record (_mr) flag is generated as an addittional column to indicate
    if there is discrepancy in values for harmonised variables
    USE: function is employed in 05c_aggregate_hive_tables.py
    AUTHOR: David Cobbledick
    DATE: 08/01/2021

Help on function complex_standardisation in adruk_tools.adr_functions:

adruk_tools.adr_functions.complex_standardisation = complex_standardisation(df, gender)
    WHAT IT IS: function
    WHAT IT DOES: - Enables more detailed secondary engineering of columns secified within
    the function
    USE: the complex_standardisation function is employed in 05c_aggregate_hive_tables.py
    NOTES: - This can be adapted to suit data and processing requirements
    - The examples below show application for standardising sex, name and postcode 
    variables
    AUTHOR: David Cobbledick
    DATE: 08/01/2021

Help on function extended_describe in adruk_tools.adr_functions:

adruk_tools.adr_functions.extended_describe = extended_describe(df, all_=True, trim_=False, active_columns_=False, sum_=False, positive_=False, negative_=False, zero_=False, null_=False, nan_=False, count_=False, unique_=False, special_=False, blank_=False, mean_=False, stddev_=False, min_=False, max_=False, range_=False, mode_=False, length_mean_=False, length_stddev_=False, length_min_=False, length_max_=False, length_range_=False, length_mode_=False, special_dict_=False, percent_=True, pandas_=False, axis_=0, fillna_=0)
    :WHAT IT IS: PYSPARK FUNCTION
    
    :WHAT IT DOES: This function extends all of the functions listed in parameters to apply on a dataset.
    :RETURNS: Pandas dataframe with information on the data in the specified dataframe.
    :OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset as a pandas dataframe.
    :TESTED TO RUN ON: test data in adruk.test.describe
    
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
                            raw_describe = extended_describe(raw_df,
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
    
    raw_describe['total_nulls'] =         raw_describe[[k for k,v in nulls_dict.items()]+['null','NaN']]        .sum(axis=1,skipna=True)
    
    raw_describe.columns = [x+'_raw' 
                              for x in list(raw_describe)]
    
    raw_describe['variable_harmonised'] =                             [harmonise_dict.get(clean_header(x)) 
                         for x in raw_describe['variable_raw']]

Help on function generate_ids in adruk_tools.adr_functions:

adruk_tools.adr_functions.generate_ids = generate_ids(dataframe1, file, database, tablename, matching, spark, pcol='PERSON_ID', log=None, year='99')
    WHAT IT IS: Pyspark function
    WHAT IT DOES: - Creates ids suitable for the person spine and data spines in adr.
    USE: This function can be used to generate ids suitable for the person_id.
    AUTHOR: Benjamin Marshall-Sheen
    DATE: 19/01/2021
          :PARAMETERS:
    * dataframe1 = spark dataframe chosen as 1
        `(datatype = dataframe name, no string)`, e.g. dataframe1
    * file =  The name of the file being written in the new column
          * database - the name of the databae in Hive i.e. adruk
          * tablename - the name of the table in hive i.e. adr_aede_person_spine
          * matching = a list with your chosen column i.e. pupilreferencematchinganoynymous.
          * year - the year of the file i.e. split from file name school_census11 = 11
    * log = a pandas dataframe to append a log to. Default value = None
        `(datatype = pandas dataframe)`, e.g. engineering_log
    AUTHOR: Benjamin Marshall-Sheen
    DATE: 19/01/2021
          
          :EXAMPLE:
         ctas_data, sgss_data, log = update_hive(dataframe1 = df, file = name of file, databsae = hive database, tablelname = hive table name, log = log)

Help on function make_test_df in adruk_tools.adr_functions:

adruk_tools.adr_functions.make_test_df = make_test_df(session_name)
    WHAT IT IS: Function
    WHAT IT DOES: creates a dataframe with several columns of different data types for testing purposes. Intentionally includes various errors, e.g. typos.
    RETURNS: spark dataframe
    
    AUTHOR: Johannes Hechler
    DATE: 27/08/2019
    VERSION: 0.1
    
    :PARAMETERS:
      :session_name = name of the spark session to use:
        `(datatype = session name, unquoted)`, e.g. spark
    
    :EXAMPLE:
    >>> make_test_df(spark)

Help on function save_sample in adruk_tools.adr_functions:

adruk_tools.adr_functions.save_sample = save_sample(dataframe, sample_size, filepath, na_variables=[])
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

Help on function session_large in adruk_tools.adr_functions:

adruk_tools.adr_functions.session_large = session_large()
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

Help on function session_medium in adruk_tools.adr_functions:

adruk_tools.adr_functions.session_medium = session_medium()
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

Help on function session_small in adruk_tools.adr_functions:

adruk_tools.adr_functions.session_small = session_small()
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

Help on function session_xl in adruk_tools.adr_functions:

adruk_tools.adr_functions.session_xl = session_xl()
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

Help on function spark_glob in adruk_tools.adr_functions:

adruk_tools.adr_functions.spark_glob = spark_glob(host, directory)

Help on function spark_glob_all in adruk_tools.adr_functions:

adruk_tools.adr_functions.spark_glob_all = spark_glob_all(host, directory)
    # returns files in directory

Help on function unzip_to_csv in adruk_tools.adr_functions:

adruk_tools.adr_functions.unzip_to_csv = unzip_to_csv(file_path, file_name, destination_path)
    :WHAT IT IS: PYSPARK FUNCTION
    
    :WHAT IT DOES: unzips a .csv.gz file into cdsw from a chosen location,
    puts the resulting unzipped csv into a user defined destination folder, 
    and then deletes the original file from cdsw.
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


