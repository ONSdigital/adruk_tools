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


def missing_count(*args):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: counts missing values per column for any number of input matrices.
    :RETURNS: pandas dataframe
    :OUTPUT VARIABLES TYPE: string, numeric
    :NOTES: won't work if any columns are of array type

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Amy Mayer, amended by Johannes Hechler
    :DATE: 19/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * args (Pyspark dataframe): Dataframes for analysis
        `(datatype = dataframe names, no string, no list)`, e.g. PDS, HESA, WSC


    :EXAMPLE:
    >>> missing_count(PDS, HESA, WSC)
    """

    pd.set_option("display.html.table_schema", True)

    # Create empty Pandas dataframe to store results
    missing_file = pd.DataFrame([])

    for count, i in enumerate(args):  # for each dataframe, do this:
        count_nulls = i.select(
            *(
                F.sum(
                    F.col(c)
                    .isin(  # for each column, sum together all values that....
                        ["", "NULL", "NAN", "NA", "UNK"]
                    )
                    .cast("int")  # ... are either of these values...
                )
                # turn the True/False values into 1/0 so they can be summed up.
                + F.sum(
                    # Then add to that sum...
                    F.col(c).isNull().cast("int")).alias(c) for c in i.columns))

        # ... rename the resulting number after the variable it refers to

        count_nulls = count_nulls.toPandas()

        # Extract counts from count_nulls dataframe to a list
        count_nulls_list = list(count_nulls.iloc[0])

        # Create new list expressing counts as percentages
        count_all = i.count()
        count_nulls_percent = [j / count_all * 100 for j in count_nulls_list]

        # Create list of file numbers
        file_nums = [count + 1] * len(i.columns)

        # Append counts, percentages and file number lists to missing_file dataframe
        missing_file = missing_file.append(
            pd.DataFrame(
                list(zip(count_nulls_list, count_nulls_percent, file_nums)),
                index=i.columns,
                columns=["Count_Missing", "Percentage_Missing", "File"],
            )
        )
    return missing_file


def missing_by_row(dataset, *args):

    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: counts how many rows have got different numbers of selected
    variables missing
    :RETURNS: frequency table
    :OUTPUT VARIABLE TYPE: string, numeric

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 19/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe
        `(datatype = 1 dataframe name, no string)`, e.g. PDS
    * args = variables to include in count
        `(datatype = strings, no list)`, e.g. 'forename', 'surname'

    :EXAMPLE:
    >>> missing_by_row(PDS,
                       'forename_clean',
                       'middle_name_clean',
                       'surname_clean',
                       'date_of_birth',
                       'sex',
                       'postcode')

    """

    return (
        dataset.select(*args)
        .withColumn(
            "fields_missing",  # create new column called 'fields_missing'
            sum(  # make column equal the sum of missing fields of the chosen variables
                  # NB this MUST use base Python sum(), NOT pyspark.sql.functions.sum!
                [F.when(F.col(x).isNull(), 1).otherwise(0) for x in [*args]]
            ),
        )
        .groupby("fields_missing")
        .count()
    )  # count frequencies of missing number of fields


def unique_function(*args):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: Return unique values per column for any number of input matrices.
    Indicates suitability of each column for use as an identifier key in linkage.
    :RETURNS: pandas dataframe of analysis results
    :OUTPUT VARIABLE TYPE: string, numeric, boolean

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Amy Mayer, amended by Johannes Hechler and Dave Beech
    :DATE: 19/12/2019
    :VERSION: 0.0.2


    :PARAMETERS:
    * args = spark dataframes for analysis
        `(datatype = dataframe names, no strings, no list)`, e.g. PDS, HESA


    :EXAMPLE:
    >>> unique_function(PDSraw, PDSclean)
    """

    pd.set_option("display.html.table_schema", True)

    # Create an empty dataframe to store results
    unique_file = pd.DataFrame([])
    for count, i in enumerate(args):  # go through each dataframe and do this:

        # Count distinct values and send to Pandas dataframe
        i.persist()  # make the current dataframe stay in the executors for the next
        # few calculations to prevent repeated reading in, to speed up processing
        distinct_count = [
            i.select(F.countDistinct(c)).collect()[0][0] for c in i.columns
        ]  # for each column count the number of distinct value and save in a list

        # Count non-empty cells and send to Pandas dataframe
        count_not_nulls = i.select(
            *(F.sum(F.col(c).isNotNull().cast("int")).alias(c) for c in i.columns)
        )
        count_not_nulls = count_not_nulls.toPandas()

        # Extract counts from dataframe to a list
        count_not_nulls_list = list(count_not_nulls.iloc[0])

        i.unpersist()  # allow current dataframe to be removed from executors' memory.
        # The calculations where a stable memory allocation helped is over now.

        # Create boolean list to show if column could be used as unique identifier
        # (ie does the number of unique values = number of non-nulls)
        identifier = [j == k for j, k in zip(distinct_count, count_not_nulls_list)]

        # Create list of percentages of unique values
        distinct_percent = [
            (j / k) * 100 for j, k in zip(distinct_count, count_not_nulls_list)
        ]

        # Calculate percentage of unique values where total includes nulls
        input_dataset_count = i.count()
        distinct_standardised = [
            (x / input_dataset_count) * 100 for x in distinct_count
        ]

        # Calculate the average number of non-null entries per distinct value
        count_per_distinct = [
            (y / z) for y, z in zip(count_not_nulls_list, distinct_count)
        ]

        # Create list of file numbers
        file_nums = [(count + 1)] * len(i.columns)

        # Append file_nums, distinct_count, distinct_percent, distinct_normalised,
        # count_per_distinct, and identifier to unique_file dataframe
        unique_file = unique_file.append(
            pd.DataFrame(
                list(
                    zip(
                        file_nums,
                        distinct_count,
                        distinct_percent,
                        distinct_standardised,
                        count_per_distinct,
                        identifier,
                    )
                ),
                index=i.columns,
                columns=[
                    "File",
                    "Distinct_Values_Count",
                    "Distinct_Values_%",
                    "Distinct_%_Standardised",
                    "Count_Per_Distinct",
                    "Use_As_Identifier",
                ],
            )
        )

    return unique_file
