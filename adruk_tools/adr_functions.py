import os
import random
import pydoop.hdfs as pdh
import pandas as pd
import pathlib
import yaml as Y

from pyspark.sql import (SparkSession,
                         types as T,
                         functions as F,
                         window as W)

# ONS packages
import de_utils.catalog_utils._catalog_utils as cu
import de_utils.spark_utils._spark_utils as su
import adruk_tools.uuid as uuid



def extract_salt(filepath:str,
                 project_name:str, 
                 instance_value:str):
  """
  reads in the master SALT file on HDFS and extracts 1 value
  
  language
  --------
  Python
    
  returns
  -------
  salt value
  
  return type
  -----------
  string
  
  author
  ------
  Alex Anthony, johannes hechler
    
  date
  ------
  14/02/2024
  
  version
  -------
  0.0.2

  change log
  -------
  0.0.1 > 0.0.2 : add parameter filepath to specify different file
  
  dependencies
  ------------
  hdfs_to_pandas()
  
  parameters
  ----------
  filepath = full path to file holding salts
  `(datatype = string)`, e.g. '/dapsen/landing_zone/adruksalts/salts.csv'
  project_name = what value to filter the 'project' column on
  `(datatype = string)`, e.g. 'NEED'
   instance_value = what value to filter the 'instance' column on
  `(datatype = string)`, e.g. 'wave1'

  example
  -------
  >>> extract_salt( filepath = '/myfiles/myfile.csv',
                    project_name = 'GUIE',
                    instance_value = 'bjdbfs')

  """

  
  salts = hdfs_to_pandas(filepath)


  salt = (salts[(salts['project'] == project_name) &
               (salts['instance'] == instance_value)]
               ['salt'].values[0])
  
  return salt 




def write_hive_table(database: str, table_name: str, dataset, table_properties: dict):
    """
    :LANGUAGE: pyspark

    :WHAT IT DOES: writes a hive table in orc format that includes mandatory
    and optional properties

    :RETURNS: a hive table

    :PARAMETERS:
    * database = hive database name
      `(datatype = string)`, e.g. 'adruk'
    * table_name = hive table name
      `(datatype = string)`, e.g. 'dwp_analysis'
    * dataset = dataset we want to write to hive
      `(datatype = spark dataframe)`, e.g. dwp_analysis
    * table_properties = a dictionary containing mandatory and optional properties
      - properties are set using key argument name-value pairs.
      - property names are case sensitive
      - mandatory properties: 'project', 'description', 'tags'
      `(datatype = dictionary)`

    :AUTHOR: Silvia Bardoni
    :DATE: 18/05/2022
    :VERSION: 0.0.1

    :EXAMPLE:
    >>> write_hive_table(database = 'adruk,
                          table_name = 'my_analysis',
                          dataset = spark_dataframe,
                          table_properties = {'project' : 'dwp',
                                              'description' : 'text',
                                              'tags' : 'dwp, tax, delete',
                                              'retention' : 'delete by end 2022'})
    """

    # list of mandatory properties. if any are missing the function fails
    expected_properties = ['project', 'description', 'tags']

    # check that all mandatory properties have been defined
    if not all(item in table_properties.keys() for item in expected_properties):
        raise ValueError('Some mandatory properties are missing')

    # write dataframe to hive
    table_path = f"{database}.{table_name}"
    dataset.write.format('orc').saveAsTable(table_path, mode='overwrite')

    # add properties to hive table
    cu.set_table_properties(database, table_name, **table_properties)


def pydoop_read(file_path):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: reads in small dataset from HDFS without the need for
    a spark cluster
    :RETURNS: un-parsed, unformatted dataset
    :OUTPUT VARIABLE TYPE: bytes


    :AUTHOR: Johannes Hechler
    :DATE: 28/09/2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: not all parsing functions accept this output.
    pd.read_excel() does, pd.read_csv() does not

    :PARAMETERS:
    * file_path = full path to file to import
        `(datatype = string)`, e.g. '/dapsen/workspace_zone/my_project/sample.csv'

    :EXAMPLE:
    >>> pydoop_read(file_path = '/dapsen/workspace_zone/my_project/sample.csv')
    """

    # read in file from HDFS
    with pdh.open(file_path, "r") as f:
        data = f.read()
        f.close()

    return data


def hdfs_to_pandas(file_path: str,
                   **kwargs) -> pd.DataFrame():
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: reads in small csv dataset from HDFS without the need for a
  spark cluster
  :RETURNS: dataframe
  :OUTPUT VARIABLE TYPE: pandas


  :AUTHOR: Johannes Hechler
  :DATE: 02/10/2023
  :VERSION: 0.0.2
  :CHANGE FROM 0.0.1: accepts all pandas.read_csv() arguments
  :KNOWN ISSUES: only works on .csv files

  :PARAMETERS:
  * file_path = full path to file to import
      `(datatype = string)`, e.g. '/dapsen/workspace_zone/my_project/sample.csv'
  * kwargs = any further parameters you want to pass to pd.read_csv()

  :EXAMPLES:
  >>> hdfs_to_pandas(file_path = '/dapsen/workspace_zone/my_project/sample.csv')

  >>> hdfs_to_pandas(file_path = '/dapsen/workspace_zone/my_project/sample.csv',
                      sep = '|')
  """

  # read in file from HDFS
  with pdh.open(file_path, "r") as f:
    data = pd.read_csv(f, **kwargs)
    f.close()

  return data


def pandas_to_hdfs(dataframe: pd.DataFrame(), 
                   write_path: str,
                   **kwargs):
  """
  :LANGUAGE: Python
  :WHAT IT DOES: write a pandas dataframe to HDFS without the need for
  a spark cluster
  :RETURNS: N/A
  :OUTPUT VARIABLE TYPE: N/A
  :NOTES: never writes out the dataframe index

  :AUTHOR: Johannes Hechler
  :DATE: 02/10/2023
  :VERSION: 0.0.2
  :CHANGE FROM 0.0.1: accepts all pandas.DataFrame.to_csv() arguments
  :KNOWN ISSUES: None

  :PARAMETERS:
  * dataframe = pandas dataframe you want to write to HDFS
      `(datatype = dataframe, no string)`, e.g. my_data
  * write_path = full destination file path including extension
      `(datatype = string)`, e.g. '/dapsen/workspace_zone/my_project/sample.csv'
  * kwargs = any further parameters you want to pass to 
    ... pd.DataFrame.to_csv()

  :EXAMPLES:
  >>> pandas_to_hdfs( dataframe = my_data,
                      write_path = '/dapsen/workspace_zone/my_project/sample.csv')

  >>> pandas_to_hdfs( dataframe = my_data,
                        write_path = '/dapsen/workspace_zone/my_project/sample.csv',
                        sep = '|')
    """

  # write file from HDFS
  with pdh.open(write_path, "wt") as f:
    dataframe.to_csv(f,
                     index = False,
                     **kwargs)
    f.close()




def column_recode(dataframe, column_to_recode, recode_dict, non_matching_value):
    """
    :LANGUAGE: pyspark
    :WHAT IT DOES:
    * Recodes values in a column based on user defined dictionary
    * Any columns value that dont match keys in the recode_dict are given
    * the non_matching_value

    :AUTHOR: Johannes Hechler, Silvia Bardoni
    :DATE: 26/10/2022
    :VERSION: 0.0.1

    :PARAMETERS:
    * dataframe = spark dataframe
    * column_to_recode (str)  = name of column to recode
    * recode_dict (dict) = dictionary containing values to be replaced
          and their replacement value
    * non_matching_value (string) = value given to any column values that are
        not found in the recode_dict

    :RETURNS:
    * dataframe

    :EXAMPLE:

    :SAMPLE INPUT

    +-----+
    | name|
    +-----+
    |  Nat|
    |  Tom|
    |  Nat|
    |    5|
    |    9|
    |  Tom|
    +-----+
    >>> recode_df = column_recode(dataframe, 'name', {'Nat' : 'N', 'Tom' :'T'}, 'None')

    :SAMPLE OUTPUT:

    +-----+
    | name|
    +-----+
    |    N|
    |    T|
    |    N|
    | None|
    | None|
    |    T|
    +-----+
    """

    if not isinstance(dataframe.schema[column_to_recode].dataType, T.StringType):
        raise TypeError("Column nust be a string")

    # Start constructing SQL query
    sql_string = f'{"CASE "}'

    # For each kew value pair in the recode_dictionary, add a separate CASE clause
    for key, value in recode_dict.items():
        sql_string += f"WHEN {column_to_recode} = '{key}' THEN '{value}' "

    # Finish by add the ELSE clause
    sql_string += f"ELSE '{non_matching_value}' END"

    dataframe = dataframe.withColumn(column_to_recode, F.expr(sql_string))

    return dataframe


def equalise_file_and_folder_name(path):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: renames a .csv file to what their folder is called

    :NOTES: only works if the file is in only 1 partition

    :AUTHOR: Johannes Hechler
    :DATE: 04/10/2021
    """

    path_parts = path.split("/")  # identify folder levels in path
    path_new = (
        "/".join(path_parts[:-1] + [path_parts[-2]]) + ".csv"
    )  # construct the file name from the folder name, and add the file extension
    pdh.rename(path, path_new)  # do the actual renaming


def update_file_with_template(file_path,
                              template,
                              join_variable,
                              drop_from_template=None,
                              keep_before_join=None,
                              keep_after_join=None):
    """
    :WHAT IT IS: python function
    :WHAT IT DOES:
    * tries to update a file, if it exists, with information from a template.
    Else it writes out the template in its place.
    :RETURNS: updated input file, or template.
    :OUTPUT TYPE: csv

    :AUTHOR: hard-coded by David Cobbledick, function by Johannes Hechler
    :DATE: 15/10/2021
    :UPDATED June 2022 Nathan Shaw

    :ASSUMPTIONS:
    * files have headers
    * both datasets have the join variable under the same name

    :PARAMETERS:
      :file_path = full path to the file that you want to update:
        `(datatype = string)`,
        e.g. '/home/cdsw/test_file.csv'
      :template = name of the pandas dataframe that you want to update from:
        `(datatype = dataframe name, unquoted)`, e.g. template_df
      :join_variable = name(s) of the variable to join input file and template on:
        `(datatype = string)`, e.g. 'nino'
      :drop_from_template = name(s) of the variable to drop from the template upon load:
        `(datatype = list of string)`, e.g. ['nino']
        Optional. Defaults to None.
      :keep_before_join = name(s) of the variable to keep in input file:
        `(datatype = list of string)`, e.g. ['sex']
        Optional. Defaults to None.
      :keep_after_join = name(s) of the variable to keep after update has taken place:
        `(datatype = list of string)`, e.g. ['Age', 'Occupation']
        Optional. Defaults to None.

    :EXAMPLE:
    >>> update_file_with_template(file_path = '/home/cdsw/test_file.csv',
                                  template = good_order,
                                  join_variable = 'nhs_number',
                                  drop_from_template = ['Name'],
                                  keep_before_join = ['Age'],
                                  keep_after_join = ['Sex', 'Surname', 'Nino'])
    """
    # Check file is a csv
    if pathlib.Path(file_path).suffix != ".csv":
        raise ValueError('Function can only be used with CSV file.')

    # Check if the file actually exists, and if it does then...
    if os.path.exists(file_path):

        # read in the file to update and convert to pandas
        file_to_update = pd.read_csv(file_path)

        # NB both datasets have to be a dataframe (i.e. have more than 1 column)
        # Check first and turn into data frame if needed.
        if not isinstance(template, pd.DataFrame):
            template = template.to_frame()

        if not isinstance(file_to_update, pd.DataFrame):
            file_to_update = file_to_update.to_frame()

        # remove unneeded columns from template
        if drop_from_template is not None:
            template = template.drop(drop_from_template, axis=1)
        else:
            template = template[join_variable]

        # Keep only columns of interest
        if keep_before_join is not None:
            file_to_update = file_to_update[keep_before_join]

        # join the file onto the template.
        # keeps only records with values that exist in the template's join variable.
        # NB can lead to duplication if the join column isn't unique in either dataset.
        # Double check again that both datasets are still dataframes.
        if not isinstance(template, pd.DataFrame):
            template = template.to_frame()

        if not isinstance(file_to_update, pd.DataFrame):
            file_to_update = file_to_update.to_frame()

        updated_file = pd.merge(template,
                                file_to_update,
                                on=join_variable,
                                how='left'
                                )

        # Once input file updated, keep only required variables
        if keep_after_join is not None:
            updated_file = updated_file[keep_after_join]

        # write the updated file back to CDSW
        updated_file.to_csv(file_path, index=False)

        print("File updated")

    # ... and if there is no such file yet then save the template in its place
    else:
        # Need to add _template into the filepath to differentiate
        directory, filename = os.path.split(file_path)
        new_filename = filename.replace('.csv', '_template.csv')
        new_file_path = os.path.join(directory, new_filename)

        # Write to CDSW
        template.to_csv(new_file_path, index=False)

        print("Template written")


def session_small():
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: creates spark cluster with these parameters.
    Designed for simple data exploration of small survey data.
        * 1g of memory
        * 3 executors
        * 1 core
        * Number of partitions are limited to 12,
        which can improve performance with smaller data

    :RETURNS: spark cluster
    :OUTPUT VARIABLE TYPE: spark cluster

    :NOTES:
    * This session is similar to that used for DAPCATS training
    * It is the smallest session that is realistically used

    :AUTHOR: DAPCATS
    :DATE: 2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: None

    :EXAMPLE:
    >>> session_small()
    """

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


def unzip_to_csv(file_path, file_name, destination_path):
    """
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
    """

    # separate file name from the extentions
    # (e.g. 'file_name.csv.gz' will become 'file_name')
    file_trunc_name = file_name.split(".")[0]

    # file in imported into cdsw, unzipped,
    # put into destination folder and removed from cdsw
    os.system(
        f"hdfs dfs -cat {file_path}/{file_trunc_name}.csv.gz | gzip -d |"
        f"hdfs dfs -put - {destination_path}/{file_trunc_name}.csv"
    )


def make_test_df(session_name):
    """
    :WHAT IT IS: Function
    :WHAT IT DOES: creates a dataframe with several columns of different data types
    for testing purposes. Intentionally includes various errors, e.g. typos.
    :RETURNS: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 27/08/2019
    :VERSION: 0.1

    :PARAMETERS:
      * session_name = name of the spark session to use
        `(datatype = session name, unquoted)`, e.g. spark

    :EXAMPLE:
    >>> make_test_df(spark)

    """

    columns = [
        "strVar",
        "numVar",
        "strNumVar",
        "postcode",
        "postcodeNHS",
        "dob",
        "name",
    ]  # set up variable names
    values = [
        (
            "A",
            1,
            "1",
            "KT1 9AR",
            'ZZ99"3CZ',
            "1999-01-03",
            "MR Name",
        ),  # create data for both variables, row by row
        (
            " A",
            2,
            "2",
            "PO4 9HJ",
            "PO4 9HJ",
            "UNK",
            "MRS NAME",
        ),
        (
            "A ",
            3,
            "3",
            "QO4 9HJ",
            "ZZ994QZ",
            "1999/01/02",
            "MISS name",
        ),  # dob: only plausible date in list, show that to_date() has an
        # inbuilt plausibility check
        (
            " A ",
            4,
            "4",
            "SO10 9-K",
            "ZZ994UZ",
            "2019-10-15",
            "ms naMe",
        ),  # test if hyphens get removed
        (
            "",
            5,
            "5",
            "E$1 0SM",
            "ZZ997RZ",
            "2019-10-16",
            "MSTR   NaMe ",
        ),  # test if postcodes of correct format but with illegal characters are rejected
        (
            None,
            6,
            "6",
            "Q4 2WQ",
            None,
            "1999/01/42",
            "DR  naME  ",
        ),  # test postcode sectors with 1 letters, 1 numbers
        (
            "null",
            7,
            "7",
            "ZZ99 3WZ",
            "KE1    6HD",
            "1999/01/42",
            "PROF name",
        ),  # test NHS generic postcode
        (
            "NaN",
            13,
            "14",
            "OE1    4KQ",
            "ZZ9 94E",
            "1999/01/42",
            "   PROF NAME",
        ),  # to test if full duplicates get removed
        (
            "NaN",
            14,
            "15",
            "oe1 4KQ",
            "  ZZ994E",
            "1999/01/42",
            "   SIR   NaMe",
        ),  # to test if full duplicates get removed
        (
            "EN4 8XH",
            15,
            "16",
            "EN4 8XH",
            "  ZZ99  4E  ",
            "1999/01/42",
            "  MR name"
        ),  # to test if empty rows get removed
        (None, None, None, None, None, None, None),
    ]

    return session_name.createDataFrame(
        values, columns
    )  # create pyspark dataframe with variables/value from above


def anonymise_ids(df, id_cols, prefix = None):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: hashs a column, or unique combination (permutations) of columns.

    Creates a lookup with unique hashed values (SHA256) for all
    combinations (permutations) of columns entered as id_cols (incouding NULLs).
    Optional, hard coded prefix can be added. Output is a new
    table with each columns under id_cols and the new hashed id.
    This provides a method to join back to the original dataset.

    :WHY IT DOES IT: to anonymise one or more ID variables in ADRUK projects

    :RETURNS: dataframe with columns
    * columns from id_cols
    * hashed ID (unique values only) , called 'adr_id' if column doesnt exist,
    renamed 'adr_new' if column exists.

    :OUTPUT VARIABLE TYPE: spark dataframe. new ID column = string type

    :AUTHOR: Nathan Shaw
    :DATE: Feb2022

    :PARAMETERS:

      * df = spark dataframe with ID you want derive random IDs from
        `(datatype = dataframe name, no string)`, e.g. PDS

      * id_cols = column(s) to turn into randomised new ID
        `(datatype = list of strings)`, e.g. ['age', 'name']

      * prefix (optional) = String to add as a prefix
        Default value = None
        `(datatype = String)`, e.g. '2022'


    :EXAMPLES:

    :SAMPLE INPUT 1: General examples

    +-----+---+----+
    | name| ID| age|
    +-----+---+----+
    | John|AA3|  23|
    |  Tom|AA8|  32|
    |Alice|AA4|  44|
    | John|AA3|  61|
    |  Tom|AA8|  32|
    |Alice|AA4|  44|
    |Alice|   |  44|
    +-----+---+----+


    >>> anonymise_ids(df = df,
                      id_cols = ['ID'],
                      prefix = None)

    :SAMPLE OUTPUT:

    +----+--------------------+
    |  ID|              adr_id|
    +----+--------------------+
    | AA3|b2866ef626d6d8260...|
    | AA8|486340e9cc0f04a04...|
    | AA4|2651e7dd70ff9ed3c...|
    |null|                null|
    +----+--------------------+


    >>> anonymise_ids(df = df,
                      id_cols = ['ID', 'age'],
                      prefix = None)

    :SAMPLE OUTPUT:

    +----+---+--------------------+
    |  ID|age|              adr_id|
    +----+---+--------------------+
    | AA3| 23|8ecc1648c3cd33f91...|
    | AA8| 32|7a1b295804c9effcf...|
    | AA4| 44|1c920b721a1f094fb...|
    | AA3| 61|6664b717b66fa3234...|
    |null| 44|71ee45a3c0db9a986...|
    +----+---+--------------------+


    >>> anonymise_ids(df = df,
                      id_cols = ['ID'],
                      prefix = '2022')

    :SAMPLE OUTPUT:

    +----+--------------------+
    |  ID|              adr_id|
    +----+--------------------+
    | AA3|2022b2866ef626d6d...|
    | AA8|2022486340e9cc0f0...|
    | AA4|20222651e7dd70ff9...|
    |null|                null|
    +----+--------------------+

    >>> anonymise_ids(df = df,
                      id_cols = ['ID', 'age'],
                      prefix = '2022')

    :SAMPLE OUTPUT:

    +----+---+--------------------+
    |  ID|age|              adr_id|
    +----+---+--------------------+
    | AA3| 61|20226664b717b66fa...|
    | AA8| 32|20227a1b295804c9e...|
    | AA4| 44|20221c920b721a1f0...|
    |null| 44|202271ee45a3c0db9...|
    | AA3| 23|20228ecc1648c3cd3...|
    +----+---+--------------------+

    :SAMPLE INPUT 2: Example with column adr_id

    +-----+-------+----+
    | name| adr_id| age|
    +-----+-------+----+
    | John|AA3    |  23|
    |  Tom|AA8    |  32|
    |Alice|AA4    |  44|
    | John|AA3    |  61|
    |  Tom|AA8    |  32|
    |Alice|AA4    |  44|
    +-----+-------+----+

    >>> anonymise_ids(df = df,
                      id_cols = ['adr_id'],
                      prefix = '2022')

    :SAMPLE OUTPUT 2:

    +------+--------------------+
    |adr_id|          adr_id_new|
    +------+--------------------+
    |   AA3|2022b2866ef626d6d...|
    |   AA8|2022486340e9cc0f0...|
    |   AA4|20222651e7dd70ff9...|
    +------+--------------------+

    """

    # Check inputs
    # id_cols must be passed as list
    if type(id_cols) != list:
        raise TypeError("id_cols must be a list")

    # Prefix must be passed as string
    if prefix is not None:
        if type(prefix) != str:
            raise TypeError("prefix must be a string")

    # Build lookup table
    # Create new column name for hashed values
    if "adr_id" in df.columns:
        adr_id_column = "adr_id_new"
    else:
        adr_id_column = "adr_id"

    # Filter dataframe to id_cols
    df = df.select(id_cols)

    # Keep only unique permutations of the combined columns above.
    # This provides a mapping back to the original data
    df = df.drop_duplicates()

    # Create unique column based on id_cols
    # which in turn allows a unique hased value to be created
    # NB concat_ws doesnt ignore NULL values compared to concat
    if len(id_cols) > 1:
        df = df.withColumn("id_cols_concat", 
                           F.concat_ws("_", 
                                       *id_cols)
                          )
    else:
        df = df.withColumn("id_cols_concat", F.col(*id_cols))

    # create universally unique values
    # making sure column is of string type
    df = df.withColumn(adr_id_column, 
                       uuid.make_pyspark_uuids()
    )

    # Prefix string if selected
    if prefix is not None:

        df = df.withColumn(adr_id_column,
                           F.concat(F.lit(prefix), 
                                    F.col(adr_id_column)
                                   )
                          )

    # Tidy up
    df = df.drop(F.col("id_cols_concat"))

    return df



def complex_harmonisation(df, log=None):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES:
    * where harmonisation leads to duplicate named variables within a dataset,
    this function harmonised to a single variable
    * a multiple record (_mr) flag is generated as an additional column to indicate
    if there is discrepancy in values for harmonised variables

    :USE: used in 05c_aggregate_hive_tables.py
    :AUTHOR: David Cobbledick
    :DATE: 08/01/2021

    :PARAMETERS:
    * :df = the dataframe to standardise:
        `(datatype = dataframe name, no string)`, e.g. ashe_data
    * :log = name of an existing list to record engineering steps in, if any exists.
      Default value = None:
        `(datatype = list)`, e.g. engineering_log


    :EXAMPLE:
    >>> complex_harmonisation(df = my_dataframe, log = engineering_log)

    """

    # create pandas dataframe with 1 column that lists all the spark dataframe's columns
    "LEFTOVER: why make a dataframe if next thing you make it a list? "
    dup_cols = pd.DataFrame({"dup_cols": df.columns})

    # make a list of column names that appear repeatedly
    dup_cols = list(
        (
            dup_cols[dup_cols.duplicated(["dup_cols"], keep=False)].drop_duplicates()[
                "dup_cols"
            ]
        )
    )

    # for each column that appears more than once...
    for col in dup_cols:

        # add to dataframe columns named after the existing columns,
        # an index number and a special string
        df = df.toDF(
            *[
                column_name + "<<>>" + str(column_name)
                for index_number, column_name in enumerate(df.columns)
            ]
        )

        # record columns in this new dataframe that have the same name as
        # the current column when the above renaming is removed
        # dup_cols_raw = [x for x in df.columns if x.startswith(col)]
        dup_cols_raw = [x for x in df.columns if x.split("<<>>")[0] == col]

        # create a boolean column, named after the current column with a
        # 'multiple record' suffix, that flags where values of the identified
        # duplicate columns are different
        " LEFTOVER: this assumes only 2 duplicate columns are found each time "
        df = df.withColumn(
            col + "_mr", F.col(dup_cols_raw[0]) != F.col(dup_cols_raw[1])
        )

        # make a new spark dataframe by selecting only non-duplicated
        # columns from the original dataframe
        # add a new column, named after the current duplicate column,
        # with all values None
        harmonised_df = (
            df.select([column for column in df.columns if column not in dup_cols_raw])
            .withColumn(col, F.lit(None))
            .limit(0)
        )

        # ???
        harmonised_df = harmonised_df.toDF(
            *[
                column.split("<<>>")[0] if column.split("<<>>")[0] == col else column
                for column in harmonised_df.columns
            ]
        )

        # inner loop: for each recorded duplicate column ...
        for col_raw in dup_cols_raw:

            # make a copy of the original dataframe, without the duplicate column
            temp_df = df.drop(col_raw)

            # ???
            temp_df = temp_df.toDF(
                *[
                    x.split("<<>>")[0] if x.split("<<>>")[0] == col else x
                    for x in temp_df.columns
                ]
            )

            # then add the dataframe to the dataframe with only non-duplicated column,
            # and remove duplicate rows
            harmonised_df = harmonised_df.unionByName(temp_df).dropDuplicates()

        # ???
        df = harmonised_df.toDF(*[x.split("<<>>")[0] for x in harmonised_df.columns])

    # if the user specified an engineering log, then record which columns were marked
    # as duplicates, then return dataframe1, datafram2 and the log
    if log is not None:
        log.append(f"made {col} reflect _mr when duplicated")
        "LEFTOVER DATAFRAME1 DATAFRAME2 NOT IN CODE"
        # return dataframe1, dataframe2, log

    # if there is no log, then only return the dataframe
    else:
        return df


def complex_standardisation(df, gender):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES:
    * tidies and standardises name, postcode and sex columns.
    Use only for ASHE and AEDE data.
    * recodes sex columns to 1, 2, 3 for male, female, other
    * removes titles from name columns, trims and upper-cases them
    * removes bad values from postcode columns, trims and upper-cases them

    :AUTHOR: David Cobbledick
    :DATE: 08/01/2021
    :USE: used in 05c_aggregate_hive_tables.py

    :NOTES:
    * This can be adapted to suit data and processing requirements
    * The examples below show application for standardising sex,
    name and postcode variables
    * the function should only be used on ASHE and maybe AEDE data.
    It uses hard-coded column names and regex definitions that will not
    be universally true.
    * assumes name columns are called either of these:
    'FORENAME', 'MIDDLENAMES', 'SURNAME'
    * assumes postcode columns are called either of these:
    'POSTCODE', 'HOMEPOSTCODE', 'WORKPOSTCODE'


    :PARAMETERS:
    * :df = the dataframe to standardise:
        `(datatype = dataframe name, no string)`, e.g. ashe_data
    * :gender = names of columsn that contain gender data that you want
      to standardise:
        `(datatype = list of string)`, e.g. ['sex', 'gender']


    :EXAMPLE:
    >>> complex_standardisation(df = my_dataframe, gender = ['sex'])

    """

    # ======================================================================
    """ Standardises gender"""
    # ======================================================================
    # identify which expected gender columns are actually in data
    sex_cols = [column for column in df.columns if column in gender]

    # if there are any gender columns, then define what male,
    # female and other values look like ...
    """LEFTOVER: these IF clauses look redundant.
       If there are no columns then the loop below will not error"""
    if len(sex_cols) != 0:

        "LEFTOVER: this doesn't account for different cases"
        male_regex = "(?i)^m$"
        female_regex = "(?i)^f$"
        other_regex = "(?i)^N$|(?i)^u$|0|9"
        # gender_null_regex = "N"

        # ... and recode them to standard values
        for column in sex_cols:

            df = df.withColumn(column, F.regexp_replace(F.col(column), male_regex, "1"))
            df = df.withColumn(
                column, F.regexp_replace(F.col(column), female_regex, "2")
            )
            df = df.withColumn(
                column, F.regexp_replace(F.col(column), other_regex, "3")
            )

    # ===================================================================
    # ===================================================================
    """ Standardises name columns"""
    # ===================================================================
    # identify any name columns in dataframe. assumes name columns are
    # called one of 3 options.
    name_cols = [x for x in df.columns if x in ["FORENAME", "MIDDLENAMES", "SURNAME"]]

    # if any such columns were found, then define what personal titles are called and
    if len(name_cols) != 0:

        clean_name_regex = (
            "|".join(
                ["^Mr.$", "^Mrs.$", "^Miss.$", "^Ms.$", "^Mx.$", "^Sir.$", "^Dr.$"]
            )
            + "|[^ A-Za-z'-]"
        )

        # ... in each identified column ...
        for column in name_cols:

            # ... make all values upper case ...
            df = df.withColumn(column, F.upper(F.col(column)))

            " LEFTOVER: surely the regex won't work when values are all upper case "
            # ... remove leading / trailing whitespace and remove previously defined
            # patterns
            df = df.withColumn(
                column, F.trim(F.regexp_replace(F.col(column), clean_name_regex, ""))
            )

            # ... trim whitespace again (sic!) and replace any instances of
            # ' +' with just a space
            df = df.withColumn(
                column, F.trim(F.regexp_replace(F.col(column), " +", " "))
            )

    # ======================================================================
    # ======================================================================
    """ Standardises postcode columns"""
    # ======================================================================
    # identify any postcode columns in dataframe. assumes name columns
    # are called one of 3 options.
    postcode_cols = [
        x for x in df.columns if x in ["POSTCODE", "HOMEPOSTCODE", "WORKPOSTCODE"]
    ]

    # if there are any postcode columns at all then...
    if len(postcode_cols) != 0:

        "LEFTOVER: this looks fishy, especially case-wise"
        # define what bad values look like
        postcode_regex = r"[^A-za-z0-9]|[_]|[\^]"

        # ... go through all columns and ...
        for column in postcode_cols:

            # ... trim leading/trailing whitepsace,
            # then remove instances of bad values defines above ...
            df = df.withColumn(
                column, F.trim(F.regexp_replace(F.col(column), postcode_regex, ""))
            )

            # ... and make all values upper case
            df = df.withColumn(column, F.upper(F.col(column)))

    # ===================================================================

    return df


class Lookup:
    """
    :WHAT IT IS: python class
    :WHAT IT DOES: represents a lookup table

    :AUTHOR: Nathan Shaw, Johannes Hechler
    :DATE: March 2022

    :ASSUMPTIONS:
    * Key, Values can only be string type
    * If anonymise_ids function is used (i.e. dataset_value is None) then assumes
    hashed value column is called adr_id. This wont be the case if adr_id already
    existed in the dataset, in that case it will be called adr_id_new.

    :PARAMETERS:
      :key = column name for keys
        `(datatype = string)`, e.g. 'ids'
      :value = column name for values
        `(datatype = string)`, e.g. 'hashed_ids'
      :source (default None) = provide a lookup table if it already exists.
        `(datatype = spark dataframe name, unquoted)`, e.g. input
    """

    def __init__(self, key, value, source=None):
        """
        :WHAT IT IS: Class method (python function)
        :WHAT IT DOES: Initiates lookup class
        * If source provided, checks for unique values
        * If source provided, filters source to key and value columns

        :AUTHOR: Nathan Shaw, Johannes Hechler
        :DATE: March 2022

        :EXAMPLE:
        >>> lookup = Lookup(key = 'key', value = 'value')
        """

        self.key = key
        self.value = value
        self.source = source

        # Check key, value are strings
        try:
            assert isinstance(self.key, str)
        except AssertionError:
            raise TypeError(f"{self.key} column must be a string")

        try:
            assert isinstance(self.value, str)
        except AssertionError:
            raise TypeError(f"{self.value} column must be a string")

        # checks for existing lookup and runs basic tests and filtering
        if self.source is not None:

            # key and value in lookup
            if self.key not in self.source.columns:
                raise NameError(f"{self.key} column not in exisiting lookup")

            if self.value not in self.source.columns:
                raise NameError(f"{self.value} column not in exisiting lookup")

            # is key unique if exisiting lookup provided
            if self.source.select(self.key).distinct().count() != self.source.count():
                raise ValueError(f"{self.key} column doesnt contain unique value")

            # Filter source lookup to key and value column if not already so
            if len(self.source.columns) > 2:
                self.source = self.source.select(F.col(self.key), F.col(self.value))

    def create_lookup_source(self, cluster):
        """
        :WHAT IT IS: Class method (python function)
        :WHAT IT DOES: Creates a empty lookup, and assigns this to the
        source parameter in the class instance.
        * required schema is created aswell.

        :AUTHOR: Nathan Shaw, Johannes Hechler
        :DATE: March 2022

        :ASSUMPTIONS:
        key and values must be string type.

        :PARAMETERS:
          :cluster = spark cluster
            `(datatype = spark cluster, unquoted)`, e.g. spark

        :EXAMPLE:
        >>> lookup.create_lookup_source(cluster = spark)

        """

        # Create schema as required to initialise empty spark data frame
        schema = T.StructType(
            [
                T.StructField(f"{self.key}", T.StringType(), True),
                T.StructField(f"{self.value}", T.StringType(), True),
            ]
        )

        # Schema here as related to this method, not class as whole
        # Updates source flag in __init__

        self.source = cluster.createDataFrame([], schema).select(self.key, self.value)

        # Returning self to allow chaining of methods
        return self

    def add_to_lookup(self, 
                      dataset, 
                      dataset_key, 
                      dataset_value = None,
                     new_value_prefix = None):
        """
        :WHAT IT IS: Class method (python function)
        :WHAT IT DOES: Adds a dataset into a lookup.
        * Dataset key must be specified, and will be renamed to the lookup key column name
        * If dataset value is not given, one is created using anonymise_ids function.
        This will be renamed to the lookup value column name
        * Data types must match before data can be appended although the name of the columns
        can differ between lookup and dataframe
        * Only new keys from dataset are added to the lookup


        :AUTHOR: Nathan Shaw, Johannes Hechler
        :DATE: March 2022

        :ASSUMPTIONS:
        If anonymise_ids function is used (i.e. dataset_value is None) then assumes
        hashed value column is called adr_id.
        This wont be the case if adr_id already existed
        in the dataset, in that case it will be called adr_id_new.

        :PARAMETERS:
          :dataset = spark dataframe to append
            `(datatype = spark dataframe, unquoted)`, e.g. dataframe
          :dataset_key = key column name in the dataset
            `(datatype = string)`, e.g. 'key'
          :dataset_value (default None) = value column name in the dataset, if one exists
            `(datatype = string)`, e.g. 'value'
          :new_value_prefix (default None) = string to place before each newly created value. Only relevant if dataset_value = None
            `(datatype = string)`, e.g. 'value'

        :EXAMPLE:
        >>> lookup.add_to_lookup(dataset = input,
                                 dataset_key = 'ids',
                                 dataset_value = 'numVar')

        """

        # undercheck checks on dataset
        # -----------------------------

        # Dataset key checks
        try:
            assert isinstance(dataset_key, str)
        except AssertionError:
            raise TypeError(f"{dataset_key} column must be a string")

        # column in lookup
        if dataset_key not in dataset.columns:
            raise NameError(f"{dataset_key} column not in dataset")

        # Datset value checks
        if dataset_value is not None:

            try:
                assert isinstance(dataset_value, str)
            except AssertionError:
                raise TypeError(f"{dataset_value} column must be a string")

            # value in lookup
            if dataset_value not in dataset.columns:
                raise NameError(f"{dataset_value} column not in dataset")

            if dataset.select(dataset_key).distinct().count() != dataset.count():
                raise ValueError(f"{dataset_key} column doesnt contain unique value")

        # Check the lookup has a source
        if self.source is None:
            raise ValueError(
                "Lookup doesnt have a source. Use create_lookup_source_method"
            )

        # Find keys in dataset that dont exist in the lookup
        # ---------------------------------------------------

        # Remove key values from dataset that already exist using a left anti join
        # An anti join returns values from the left relation that has no match with
        # the right. It is also referred to as a left anti join.

        # NOTE: Adding eqNUllSafe method to ensure nulls get matched correctly.
        # By default, null == null returns null and so in an example with
        # two dataframes each containing one null value, they both get joined.
        # Using the eqNullSafe method ensures they get
        # matched in equality. See null semantinces for more details
        # https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html

        dataset = dataset.join(
            self.source,
            on = self.source[self.key].eqNullSafe(dataset[dataset_key]),
            how = "leftanti",
        )

        # Create value column for these keys if none provided
        # ----------------------------------------------------

        # NOTE: assumes anonymise_ids created adr_id not adr_id_new
        if dataset_value is None:
            dataset = anonymise_ids(dataset, 
                                    [dataset_key],
                                   prefix = new_value_prefix)
            dataset_value = "adr_id"

        # Match columns between lookup and dataset
        # -----------------------------------------

        # Filter dataset to key and value
        dataset = dataset.select(F.col(dataset_key), F.col(dataset_value))

        # Both lookup and dataset only have two columns by this point
        # so can simply rename
        dataset = dataset.withColumnRenamed(dataset_key, self.key).withColumnRenamed(
            dataset_value, self.value
        )

        # Check schema's match
        # ---------------------

        if dataset.schema != self.source.schema:
            raise ValueError(
                """ERROR: schemas dont match or source doesnt exist,
                      append cannot take place"""
            )

        # Update lookup: Overwriting source
        # ----------------------------------

        # Append dataset to lookup
        self.source = self.source.union(dataset)

        # Returning self to allow chaining of methods
        return self

    def remove_from_lookup(self, keys_to_remove):
        """
        :WHAT IT IS: Class method (python function)
        :WHAT IT DOES: Removes rows from a lookup based on a tuple of keys

        :AUTHOR: Nathan Shaw, Johannes Hechler
        :DATE: March 2022

        :PARAMETERS:
          :keys_to_remove = a tuple of the keys indicating which rows to remove
            `(datatype = tuple)`, e.g. (1,2,3)

        :EXAMPLE:
        >>> lookup.remove_from_lookup(keys_to_remove = (1,2,3))

        """

        # undercheck checks on dataset keys
        # ----------------------------

        try:
            assert isinstance(keys_to_remove, tuple)
        except AssertionError:
            raise TypeError(f"{keys_to_remove} parameter must be a tuple")

        # remove records from lookup
        # ---------------------------

        # get key column name from look
        key_column_name = self.key

        # note by defualt NULLS get dropped in SQL where statement so need to
        # explicitly state they are required.
        self.source = self.source.filter(
            F.expr(
                f"{key_column_name} NOT IN {keys_to_remove} OR {key_column_name} IS NULL"
            )
        )

        # Returning self to allow chaining of methods
        return self

def open_yaml_config(file_path):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: Reads in a .yaml config file from CDSW
    :RETURNS: Dictionary

    :AUTHOR: Jonathan Lewis
    :DATE: 13/01/2023
    :VERSION: 0.0.1
    :KNOWN ISSUES: N/A

    :PARAMETERS:
    * file_path = full path to file to import
        `(datatype = string)`, e.g. '/home/cdsw/your_repo/your_yaml_config.yaml'

    :EXAMPLE:
    >>> settings = open_yaml_config(file_path = '/home/cdsw/your_repo/your_yaml_config.yaml')
    """

    # Read YAML file from CDSW
    with open(file_path, "r") as f:
      try:
        config = Y.safe_load(f)
      except Y.YAMLError as error:
          raise error
        
    return config
  
def read_and_stack(cluster, files_to_stack, delim = ',', end_slice = False):
  """
  From a list of Tab delimited .txt files reads each of them into a dataframe and stacks them
  independently of their schema
    
  Parameters
  ----------
  cluster: spark session
  files_to_stack (lst): a list of files to stack
  delim (Str): specify the delimiter in the csv files you are reading, default comma
  end_slice (Boolean): Default is False, this step was required for GUIE 3.1 and 3.2, but not 3.3
       
  Returns
  -------  
  Spark DataFrame containing the stacked files
  
  Author
  ----------
  Silvia Bardoni, Nathan Shaw

  Notes
  ----------
  Uses a function backported from spark > 3 into de_utils that allows to
  stack datasets with different schemas, and assign missing column values to None
  
  """
  
  # Storing loaded dataframes into a list so they can be merged.
  dataframes = []
  for file in files_to_stack:
      # Mark what file is being processed
      filename = os.path.basename(file)
  
      print(f'Reading {filename}')
  
      dataframe = cluster.read.option('header', 'true')\
                  .option('inferSchema', 'true')\
                  .option('delimiter', delim)\
                  .csv(file)

      # Make all column names lower case so columns of the same
      # name always get stacked

      for column in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(column, column.lower())

      # All column names need the final 6 characters to be removed which are
      # always preceded by an underscore. Instead of using a slice,
      # I have split the string based on the final _ incase
      # in the future, the portion of string to be cut is not 6 characters.
      # 05/10/23 - added setting to toggle this off, not required for wave 3.3 for example 
      if end_slice == True:
        dataframe = dataframe.toDF(*(c.rsplit('_', 1)[0] for c in dataframe.columns))

      # Add column to record input file name
      dataframe = dataframe.withColumn("source_file", F.lit(filename))

      dataframes.append(dataframe)
      
  # Stack dataset:
  # Uses a function backported from spark > 3 into de_utils, we can easily
  # stack datasets with different schemas, and assign missing column values to None
  
  stacked_datasets = su.union_by_name(dfs = dataframes,
                                   allow_missing_cols = True,
                                   fill = None)
  return stacked_datasets
