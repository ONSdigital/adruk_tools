import pyspark.sql.functions as F
import pandas as pd


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
