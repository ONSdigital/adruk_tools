import os
import random
import pydoop.hdfs as pdh
import pandas as pd
import pathlib

from pyspark.sql import SparkSession
from pyspark.context import SparkContext as sc
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W
import de_utils.catalog_utils._catalog_utils as cu


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


def make_dummy_ninos(cluster):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: provides dummy National Insurance Numbers of various
    types and quality, for testing functions on
    :RETURNS: dataframe with 25 rows and these columns
    * index - simply a row number
    * ninos - dummy values
    * description - summary what type of NINO a value represents
    :OUTPUT VARIABLE TYPE: DataFrame[index: int, nino: string, description: string]


    :AUTHOR: Johannes Hechler
    :DATE: 22/02/2022
    :VERSION: 0.0.1


    :PARAMETERS:
    * cluster = active spark cluster
        `(datatype = cluster name, not string)`, e.g. spark

    :EXAMPLE:
    >>> make_dummy_ninos( cluster = spark)

    :FULL OUTPUT:

    +-----+------------+--------------------+
    |index|        nino|         description|
    +-----+------------+--------------------+
    |    1|   AB123456A|full, valid, clea...|
    |    2|   ab123456a|full, valid, all ...|
    |    3|   Aa123456a|full, valid, mixe...|
    |    4|      AB1234|valid, truncated,...|
    |    5|   AB123457A|full, valid, clea...|
    |    6|   AB123457B|full, valid, clea...|
    |    7|   AB123458E|full, invalid fin...|
    |    8|         bob|not nino, lower case|
    |    9|         BOB|not nino, upper case|
    |   10|      bob123|not nino, lower case|
    |   11|      BOB123|not nino, upper case|
    |   12|      boB123|not nino, mixed case|
    |   13|      BOb123|not nino, mixed case|
    |   14|  AB1234TEMP|    valid, temporary|
    |   15|   BT123456A|full, valid, clea...|
    |   16|   bt123456a|full, valid, all ...|
    |   17|   Bt123456a|full, valid, mixe...|
    |   18|      BT1234|valid, truncated,...|
    |   19|   BT123457A|full, valid, clea...|
    |   20|   BT123457B|full, valid, clea...|
    |   21|   BT123458E|full, invalid fin...|
    |   22|  AB123 456A|full, valid, inte...|
    |   23|  AB123456A |full, valid, exte...|
    |   24| AB123 456A |full, valid, inte...|
    |   25|        null|       missing value|
    +-----+------------+--------------------+
    """

    # what are the columns called, and what type are they?
    fields = [
        T.StructField("index", T.IntegerType(), True),
        T.StructField("nino", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
    ]

    # create a schema that spark understands
    schema = T.StructType(fields)

    # what data to put into the dataframe. row-wise.
    contents = [
        (1, "AB123456A", "full, valid, cleaned, unique"),
        (2, "ab123456a", "full, valid, all lower case"),
        (3, "Aa123456a", "full, valid, mixed case"),
        (4, "AB1234", "valid, truncated, valid"),
        (5, "AB123457A", "full, valid, cleaned, not unique"),
        (6, "AB123457B", "full, valid, cleaned, not unique"),
        (7, "AB123458E", "full, invalid final character, cleaned, unique"),
        (8, "bob", "not nino, lower case"),
        (9, "BOB", "not nino, upper case"),
        (10, "bob123", "not nino, lower case"),
        (11, "BOB123", "not nino, upper case"),
        (12, "boB123", "not nino, mixed case"),
        (13, "BOb123", "not nino, mixed case"),
        (14, "AB1234TEMP", "valid, temporary"),
        (15, "BT123456A", "full, valid, cleaned, unique, Northern Ireland"),
        (16, "bt123456a", "full, valid, all lower case, Northern Ireland"),
        (17, "Bt123456a", "full, valid, mixed case, Northern Ireland"),
        (18, "BT1234", "valid, truncated, valid, Northern Ireland"),
        (19, "BT123457A", "full, valid, cleaned, not unique, Northern Ireland"),
        (20, "BT123457B", "full, valid, cleaned, not unique, Northern Ireland"),
        (
            21,
            "BT123458E",
            "full, invalid final character, cleaned, unique, Northern Ireland",
        ),
        (22, "AB123 456A", "full, valid, internal white space, unique"),
        (23, " AB123456A ", "full, valid, external white space, unique"),
        (24, " AB123 456A ", "full, valid, internal and external white space, unique"),
        (25, None, "missing value"),
    ]

    # create the actual dataframe
    return cluster.createDataFrame(contents, schema=schema)


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


def hdfs_to_pandas(file_path):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: reads in small csv dataset from HDFS without the need for a
    spark cluster
    :RETURNS: dataframe
    :OUTPUT VARIABLE TYPE: pandas


    :AUTHOR: Johannes Hechler
    :DATE: 19/11/2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: only works on .csv files

    :PARAMETERS:
    * file_path = full path to file to import
        `(datatype = string)`, e.g. '/dapsen/workspace_zone/my_project/sample.csv'

    :EXAMPLE:
    >>> pydoop_read(file_path = '/dapsen/workspace_zone/my_project/sample.csv')
    """

    # read in file from HDFS
    with pdh.open(file_path, "r") as f:
        data = pd.read_csv(f)
        f.close()

    return data


def pandas_to_hdfs(dataframe, write_path):
    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: write a pandas dataframe to HDFS as .csv without the need for
    a spark cluster
    :RETURNS: N/A
    :OUTPUT VARIABLE TYPE: N/A

    :AUTHOR: Johannes Hechler
    :DATE: 09/11/2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: None

    :PARAMETERS:
    * dataframe = pandas dataframe you want to write to HDFS
        `(datatype = dataframe, no string)`, e.g. my_data
    * write_path = full destination file path including extension
        `(datatype = string)`, e.g. '/dapsen/workspace_zone/my_project/sample.csv'

    :EXAMPLE:
    >>> pandas_to_hdfs( dataframe = my_data,
                        write_path = '/dapsen/workspace_zone/my_project/sample.csv')
    """

    # write file from HDFS
    with pdh.open(write_path, "wt") as f:
        dataframe.to_csv(f, index=False)
        f.close()


def cull_columns(cluster, old_files, reference_columns, directory_out):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES:
    * reads in one or more HDFS csv files in turn
    * removes any columns not listed in a reference
    * write table back out

    :AUTHOR: Johannes Hechler
    :DATE: 04/10/2021
    """

    for wrong_dataset in old_files:
        print(directory_out + wrong_dataset.split("/")[-1])
        # read in dataset
        dataset = (
            cluster.read.option("header", "true")  # Yes headers are required
            .option("inferSchema", "True")  # Yes do infer the schema
            .csv(wrong_dataset)
        )

        # cleaning: make sure all column names are upper case,
        # just like the reference list
        for column in dataset.columns:
            dataset = dataset.withColumnRenamed(column, column.upper())

        # identify which columns in current datasets are also on list of approved columns
        columns_allowed = [
            column for column in dataset.columns if column in reference_columns
        ]

        # keep only agreed variables, write back out to HDFS
        dataset.select(*columns_allowed).coalesce(1).write.csv(
            directory_out + wrong_dataset.split("/")[-1],
            sep=",",  # set the seperator
            header="true",  # Set a header
            mode="overwrite",
        )  # overwrite is on


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


def session_medium():
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: creates spark cluster with these parameters.
    Designed for analysing survey or synthetic datasets.
    Also used for some Production pipelines based on survey and/or
    smaller administrative data.
        * 6g of memory
        * 3 executors
        * 3 cores
        * Number of partitions are limited to 18,
        which can improve performance with smaller data

    :RETURNS: spark cluster
    :OUTPUT VARIABLE TYPE: spark cluster

    :USE CASE:
      * Developing code in Dev Test
      * Data exploration in Production
      * Developing Production pipelines on a sample of data
      * Running smaller Production pipelines on mostly survey data

    :AUTHOR: DAPCATS
    :DATE: 2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: None

    :EXAMPLE:
    >>> session_medium()
    """

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
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: creates spark cluster with these parameters.
    Designed for running Production pipelines on large administrative data,
    rather than just survey data. Will often develop using a smaller session
    then change to this once the pipeline is complete.
        * 10g of memory
        * 5 executors
        * 1g of memory overhead
        * 5 cores, which is generally optimal on larger sessions
        * Number of partitions are limited to 18,
        which can improve performance with smaller data

    :RETURNS: spark cluster
    :OUTPUT VARIABLE TYPE: spark cluster

    :NOTES:
    * for production pipelines on administrative data
    * Cannot be used in Dev Test, as 9 GB limit per executor

    :AUTHOR: DAPCATS
    :DATE: 2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: None

    :EXAMPLE:
    >>> session_large()
    """

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
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: creates spark cluster with these parameters.
    Designed for the most complex pipelines, with huge administrative data sources
    and complex calculations. Uses a large amount of resource on the cluster,
    so only use when running Production pipelines
        * 20g of memory
        * 12 executors
        * 2g of memory overhead
        * 5 cores, using too many cores can actually cause worse performance
        on larger sessions

    :RETURNS: spark cluster
    :OUTPUT VARIABLE TYPE: spark cluster

    :NOTES:
    * use for large, complex pipelines in Production on mostly administrative data
    * Do not use for development purposes;
    use a smaller session and work on a sample of data or synthetic data

    :EXAMPLE USE:
    * Three administrative datasets of around 300 million rows
    * Significant calculations,
    including joins and writing/reading to many intermediate tables

    :AUTHOR: DAPCATS
    :DATE: 2021
    :VERSION: 0.0.1
    :KNOWN ISSUES: None

    :EXAMPLE:
    >>> session_xl()
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
        * generates base property for object,
        i.e. reads in the specified file from HDFS into a pandas dataframe
        :AUTHOR: Johannes Hechler
        :DATE: 09/02/2021
        :VERSION: 0.1
        """

        with pdh.open(path, "r") as f:
            self.content = pd.read_json(f)
            f.close()

    def whole(self):
        """
        :WHAT IT IS: Python method for objects of class 'manifest'
        :WHAT IT DOES:
        * generates property 'whole',
        i.e. information about the overall delivery, as a pandas dataframe with 1 row
        :AUTHOR: Johannes Hechler
        :DATE: 09/02/2021
        :VERSION: 0.1
        """

        return self.content.iloc[0]

    def parts(self, variable):
        """
        :WHAT IT IS: Python method for objects of class 'manifest'
        :WHAT IT DOES:
        * generates property 'parts',
        i.e. information about the individual files included in a delivery,
        as a pandas dataframe with as many rows as there are files
        :AUTHOR: Johannes Hechler
        :DATE: 09/02/2021
        :VERSION: 0.1
        """

        return pd.DataFrame(list(self.content[variable]))


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


def save_sample(dataframe, sample_size, filepath, na_variables=[]):
    """
    :WHAT IT IS: PYSPARK FUNCTION

    :WHAT IT DOES:
    * draws as user-specified number of records from the top of a spark dataframe
    * saves them to a selected HDFS location in csv format

    :RETURNS: nothing in memory; writes out a comma-separated file
    :OUTPUT VARIABLE TYPE: not applicable

    :TESTED TO RUN ON: spark dataframe from covid test and trace dataset

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2021
    :VERSION: 0.0.2
    :CHANGE FROM PREVIOUS VERSION: uses pydoop for writing out sample
    instead of spark cluster
    :KNOWN ISSUES: requires package pydoop

    :PARAMETERS:
    * dataframe = spark dataframe
        `(datatype = dataframe name, no string)`, e.g. ctas_data
    * sample_size = how many rows to take from dataset. Default values = 20.
        `(datatype = numeric)`, e.g. 20
    * filepath = the directory and filename for the file to be written to.
        `(datatype = string)`, e.g. "/dapsen/workspace_zone/adruk/sample.csv"
          * na_variables = if you want to exclude records with missing data
          from the sample, you can specify the names of columns to check for missingness.
          Records are removed if ANY of the selected variables is missing.
          Optional. Default value = []
        `(datatype = list of strings)`, e.g. ['age', 'sex]

    :EXAMPLE:
    >>> save_sample( dataframe = pii_data,
                     sample_size = 20,
                     filepath = '/dapsen/workspace_zone/my_project/sample.csv)))
    """

    # removes records with missing values in the chosen columns, if any were chosen
    dataframe = dataframe.na.drop(subset=na_variables, how="any")

    # draws the sample and converts it to a pandas dataframe
    results = dataframe.limit(sample_size).toPandas()

    # write sample to the chosen HDFS file_path in comma-separate format.
    pandas_to_hdfs(dataframe=results, write_path=filepath)


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


def anonymise_ids(df, id_cols, prefix=None):
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
        df = df.withColumn("id_cols_concat", F.concat_ws("_", *id_cols))
    else:
        df = df.withColumn("id_cols_concat", F.col(*id_cols))

    # Hash the data in the specified column using SHA256,
    # making sure column is of string type
    df = df.withColumn(
        adr_id_column, F.sha2(F.col("id_cols_concat").cast("string"), 256)
    )

    # Prefix string if selected
    if prefix is not None:

        df = df.withColumn(adr_id_column,
                           F.concat(F.lit(prefix), F.col(adr_id_column)))

    # Tidy up
    df = df.drop(F.col("id_cols_concat"))

    return df


def generate_ids(session, df, id_cols, start_year, id_len=None):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: recodes a column, or combinations of columns,
    into a unique random numerical values.

    Creates unique random numerical values for all unique combinations
    (permutations) of columns entered under id_cols and start_year,
    with start_year added as a prefix to the generated numerical
    values. Output is a new table with columns under id_cols,
    allowing a join back to the original dataset, and the new numerical id.
    Additional option to create numerical id of specified length.

    :WHY IT DOES IT: to anonymise ID variables in ADRUK projects

    :RETURNS: dataframe with 2 or more columns, mapping old and new IDs
    * old ID (unique combinations of columns in id_cols and start_year,
    displayed in seperate columns)
    * new ID (unique values only) , called 'adr_id'

    :OUTPUT VARIABLE TYPE: spark dataframe. new ID column = string type

    :AUTHOR: hard-coded by David Cobbledick, function by Johannes Hechler
    :DATE: 2020
    :VERSION: 0.0.1
    :KNOWN ISSUES:
      * input dataset must not have existing column called 'adr_id'
      * the parameter 'start_year' is actually used as part of the parameter
      'id_cols'. That means that the function distinguishes not just distinct
      values of the id columns, but distinct combinations of id columns and
      start_year. If you don't want this start_year must be a column with only
      1 value. start_year is a compulsory parameter, so if you only want to
      distinguish distinct values of the id_cols then create an auxiliary
      column with just 1 distinct value to use as start_year.

    :PARAMETERS:
      * session = name of active spark cluster
        `(datatype = cluster name, no string)`, e.g. spark

      * df = spark dataframe with ID you want derive random IDs from
        `(datatype = dataframe name, no string)`, e.g. PDS

      * id_cols = column(s) to turn into randomised new ID
        `(datatype = list of strings)`, e.g. ['year', 'name']

      * start_year = name of additional column to prefix (in the clear) to
        the anonymised ids.
        NB if several columns are supplied, only the first is used.
        NB this column is actually used with the id_cols, i.e. if start_year has
        several distinct values it potentially inflates the number of ids identified
        by the function
        `(datatype = list of strings)`, e.g. ['year']

      * id_len = set uniform length of ID values (ignoring length of start_year values)
      if required. Pads out values with leading zeroes if needed.
      Default value = None, i.e. accept different lengths
        `(datatype = numeric)`, e.g. 9


    :EXAMPLES:

    :SAMPLE INPUT:

    +-----+---+----+
    | name| ID| age|
    +-----+---+----+
    | John|AA3|  23|
    |  Tom|AA8|  32|
    |Alice|AA4|  44|
    | John|AA3|  61|
    |  Tom|AA8|  32|
    |Alice|AA4|  44|
    +-----+---+----+

    >>> generate_ids(sessions = spark,
                      df = AEDE,
                      id_cols = ['name'],
                      start_year = ['age'],
                      id_len = 9)

    :SAMPLE OUTPUT:
    +-----+-----------+
    | name|     adr_id|
    +-----+-----------+
    |Alice|44277926008|
    | John|23534343875|
    | John|61534343875|
    |  Tom|32580421606|
    +-----+-----------+

     :SAMPLE INPUT:

    +-----+---+----+
    | name| ID| age|
    +-----+---+----+
    | John|AA3|  23|
    |  Tom|AA8|  32|
    |Alice|AA1|  44|
    | Matt|AA6|  61|
    |  Tom|AA8|  32|
    |Alice|AA4|  44|
    +-----+---+----+

    >>> generate_ids(sessions = spark,
                      df = AEDE,
                      id_cols = ['name', 'ID'],
                      start_year = ['age'],
                      id_len = 9)

    :SAMPLE OUTPUT:
    +-----+---+-----------+
    | name| ID|     adr_id|
    +-----+---+-----------+
    |  Tom|AA8|32974159768|
    |Alice|AA1|44074421659|
    |Alice|AA4|44248294836|
    | Matt|AA6|61683839080|
    | John|AA3|23683839080|
    +-----+---+-----------+
    """

    # ==========================================================================
    """CHECK INPUTS AND PREPARE INPUT DATA"""
    # ==========================================================================

    # check that the ID columns were passed as a list, and if not, make it one
    if type(id_cols) != list:
        id_cols = [id_cols]

    # check that the columns expressing which period an ID first appeared were
    # passed as a list, and if not, make it one
    if type(start_year) != list:
        start_year = [start_year]

    # reduce dataframe to only the ID and the Year columns. then remove records
    # where the same ID was used more than once in the year it was first used.
    df = df.select(id_cols + start_year)
    df = df.drop_duplicates()

    # count how many IDs ( = people) are left, i.e. how many need a new ID generated
    n_persons = df.count()

    # ==========================================================================
    """CREATE RANDOM VALUES FOR NEW IDs"""
    # ==========================================================================

    # if you don't care whether your numbers will have a specific length of digits
    if id_len is None:
        id_list = random.sample(
            range(n_persons), n_persons  # how many numbers to generate
        )  # how many of those numbers to pick

    # if you want your numbers to have a specific lenght. NB sometimes the numbers
    # will shorter - these values are padded out later
    # generates numbers up to 10 to a chosen power.
    # using range() means there are no duplicates in the numbers that get sampled
    # from, i.e. sampling is without replacement
    # abs() is a safeguard in case users passed a negative value
    else:
        id_list = random.sample(range(1 * 10 ** abs(id_len)), n_persons)

    # turn the base Python list into a spark dataframe
    list_df = session.createDataFrame(id_list, T.IntegerType())

    # change the default column name to 'adr_id'
    list_df = list_df.withColumnRenamed("value", "adr_id")

    # ==========================================================================
    """MAIN DATASET: GIVE EACH OLD ID VALUE A UNIQUE
    BUT UNRELATED NUMBER TO LATER JOIN ON"""
    # ==========================================================================
    # make a new, purely auxiliary columm called 'instance'.
    # For now populated with the number 1, to be used in a calculation,
    # then later deleted
    df = df.withColumn("instance", F.lit(1))

    # define a window function specification that...
    w = (
        W.Window.partitionBy(
            "instance"
        )  # for each unique value in the 'instance' column...
        .orderBy(
            id_cols
        )  # ... and ordered by the column of ID values created earlier ...
        .rangeBetween(W.Window.unboundedPreceding, 0)
    )  # ... add as many to the previous group's value as there are records in
    # the current groups

    # apply the window specification - essentially makes a (non-unique) ranking,
    # where each group's rank number is the previous group's number, plus as the
    # number of times that the current ID value appears in the data
    df = df.withColumn("cum_sum", F.sum("instance").over(w))

    # remove the auxiliary 'instance' column from the main dataframe
    df = df.drop("instance")

    # ==========================================================================
    """NEW ID DATASET: GIVE EACH NEW ID VALUE A
    UNIQUE BUT UNRELATED NUMBER TO LATER JOIN ON"""
    # ==========================================================================
    # make a new, auxiliary column called 'instance' in the auxiliary dataframe
    # that holds the numbers created for use as IDs
    list_df = list_df.withColumn("instance", F.lit(1))

    # define a window function specification that is the same as for the
    # main dataframe but...
    w = (
        W.Window.partitionBy("instance")
        .orderBy("adr_id")  # ... orders by the newly created ID values
        .rangeBetween(W.Window.unboundedPreceding, 0)
    )

    # apply the window specification - essentially makes a (non-unique) ranking,
    # where each group's rank number is the previous group's number,
    # plus as the number of times that the current ID value appears in the data
    list_df = list_df.withColumn("cum_sum", F.sum("instance").over(w))

    # remove the auxiliary 'instance' column from the main dataframe
    list_df = list_df.drop("instance")

    # ==========================================================================
    """ADD NEW ADR_ID VALUES TO MAIN DATAFRAME"""
    # ==========================================================================
    # join the dataframe with the adr_id column onto the main dataframe
    # keeps only records whose cum_sum value exists in both dataframes
    # NB this by definition never creates duplicate records because the
    # linkage variable 'cum_sum' is unique in the adr_id dataframe
    df = df.join(list_df, on="cum_sum", how="inner")

    # remove auxiliary 'cum_sum' column
    df = df.drop("cum_sum")

    # ==========================================================================
    """WHERE ADR_ID VALUES ARE NOT OF DESIRED
    LENGTH PAD THEM OUT WITH LEADING ZEROES"""
    # ==========================================================================
    # if you don't care how many digits your new ID values ought to have...
    if id_len is None:
        n_characters = str(
            len(str(n_persons))
        )  # how many digits are in the number of records of the main
        # dataframe - turn that from numeric into string

    # if you want the ID values to have a specific length
    else:
        n_characters = str(
            id_len
        )  # simply turn from numeric to string the number of digits you want
        # to have in your new ID values

    # overwrite the existing 'adr_id' column in the main dataframe, that turns
    # the numeric values to string, and adds leading zeros if they're shorter
    # than the selected number of digits
    # "%0" means 'potentially start with leading zeroes'
    # n_characters means 'if the original value isn't this long already
    # "d" : unclear what it does but without it spark throws a memory error
    df = df.withColumn("adr_id", F.format_string("%0" + n_characters + "d", "adr_id"))

    # ==========================================================================
    """ADD DELIVERY PERIOD TO ADR_ID"""
    # ==========================================================================
    # overwrite the new ID column with a version of itself that has the delivery
    # period added in front
    df = df.withColumn("adr_id", F.concat(F.col(start_year[0]), F.col("adr_id")))

    # remove from the main dataframe the (first) column used to specify the period
    # an original ID value was added
    # NB while start_year must be a list, at this point it only uses the
    # first element anyway.
    # Ought to just make it a string to avoid user confusion.
    df = df.drop(start_year[0])

    # ==========================================================================
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


def spark_glob(host, directory):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: lists names of files or subdirectories in a given directory
    :RETURNS: list of file / directory names
    :OUTPUT VARIABLES TYPE: list of strings
    :NOTES: expects an existing connection to a spark cluster

    :AUTHOR: David Cobbledick
    :DATE: 2020
    :VERSION: 0.1


    :PARAMETERS:
    * :host = a valid CDSW user name. not necessarily that of the current users:
        `(datatype = string)`, e.g. 'hechlj'
    * :directory = for name of the directory to check:
        `(datatype = string)`, e.g. '/dapsen/landing_zone/hmrc/self_assessment/2017/v1'


    :EXAMPLE:
    >>> spark_glob(host = 'hechlj',
                    directory = '/dapsen/landing_zone/hmrc/self_assessment/2017/v1')

    """

    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI(host), Configuration())

    status = fs.listStatus(Path(directory))

    files = [str(fileStatus.getPath()) for fileStatus in status]

    return files


def spark_glob_all(host, directory):
    """
    :WHAT IT IS: pyspark function
    :WHAT IT DOES: lists names of files or subdirectories in a given directory,
    and all subdirectories
    :RETURNS: list of file / directory names
    :OUTPUT VARIABLES TYPE: list of strings
    :NOTES:
    * expects an existing connection to a spark cluster
    * expect the package `adruk_tools` including the function `spark_glob` to be installed

    :AUTHOR: David Cobbledick
    :DATE: 2020
    :VERSION: 0.1


    :PARAMETERS:
    * :host = a valid CDSW user name. not necessarily that of the current users:
        `(datatype = string)`, e.g. 'hechlj'
    * :directory = for name of the directory to check:
        `(datatype = string)`, e.g. '/dapsen/landing_zone/hmrc/self_assessment/2017/v1'


    :EXAMPLE:
    >>> spark_glob_all(host = 'hechlj',
                    directory = '/dapsen/landing_zone/hmrc/self_assessment/2017/v1')

    """

    files = spark_glob(host, directory)  # list all functions in selected directory

    for file in files:
        if len(files) == len(set(files)):  # ... if no file has been listed twice...
            files.extend(
                spark_glob(host, file)
            )  # then add the contents of the current file or directory file to the list.
            # NB this only makes a difference for directories.
        else:
            break

    files = list(set(files))  # deduplicate list of files

    return files


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

    def add_to_lookup(self, dataset, dataset_key, dataset_value=None):
        """
        :WHAT IT IS: Class method (python function)
        :WHAT IT DOES: Adds a dataset into a lookup.
        * Dataset key must be specified, and will be renamed to the lookup key column name
        * If dataset value is not given, one is created using anonymis_ids function.
        This will be renamed to the lookup value column name
        * Schemas must match before data can be appended
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
            on=self.source[self.key].eqNullSafe(dataset[dataset_key]),
            how="leftanti",
        )

        # Create value column for these keys if none provided
        # ----------------------------------------------------

        # NOTE: assumes anonymise_ids created adr_id not adr_id_new
        if dataset_value is None:
            dataset = anonymise_ids(dataset, [dataset_key])
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
