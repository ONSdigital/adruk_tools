from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
import numpy as np


def describe(df, describe_type):
    """Provides summary statistics for a spark data frame.

    Creates a pandas data frame of summary statistics based on the describe type
    provided - FOR EACH COLUMN??? Describe types are as follows:

    **sum**

    | Provides a count of the values in any numeric columns.

    **positive**

    Parameters
    ----------
    df : spark dataframe
        The dataframe to be analysed.

    describe_type : str
        A string to select the describe type *(sum, positive, negative, zero,
        null, nan, unique, blank, mean, stddev, min, max).*

    Returns
    -------
    pandas dataframe
        A pandas dataframe with summary statistics.

    Example
    -------
    >>> describe(df, 'sum')

    Notes
    -----
    Built by Silvia Bardoni and Nathan Shaw and based on previous code by David
    Cobbledick

    :KNOWN ISSUES: This will not work if there is a boolean datatype.    #TO CHECK!!
    """

    # Valid describe types
    valid_describe_types = ['sum', 'positive', 'negative', 'zero', 'null', 'nan',
                            'unique', 'blank', 'mean', 'stddev', 'min', 'max']

    if describe_type not in valid_describe_types:
        raise ValueError(
            f"Invalid describe type, valid values are: {valid_describe_types}"
        )

    # Create spark dataframe based on provided describe_type
    if describe_type == 'sum':
        out = df.groupBy().sum()
        out = out.toDF(*[x.replace("(", ")").split(")")[1] for x in out.columns])

    if describe_type == 'positive':
        out = df.select([F.count(F.when(df[c] > 0, True)).alias(c) for c in df.columns])

    if describe_type == 'negative':
        out = df.select([F.count(F.when(df[c] < 0, True)).alias(c) for c in df.columns])

    if describe_type == 'zero':
        out = df.select([F.count(F.when(df[c] == 0, True)).alias(c) for c in df.columns])

    if describe_type == 'null':
        out = df.select(
            [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
        )

    if describe_type == 'nan':
        out = df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns])

    if describe_type == 'unique':
        out = df.select([F.col(c).cast(T.StringType()).alias(c) for c in df.columns])
        out = out.agg(*(F.countDistinct(F.col(c)).alias(c) for c in out.columns))

    if describe_type == 'blank':
        out = df.select([F.count(F.when(df[c] == "", True)).alias(c) for c in df.columns])

    if describe_type == 'mean':
        out = df.groupBy().mean()
        out = out.toDF(*[x.replace("(", ")").split(")")[1] for x in out.columns])

    if describe_type == 'stddev':
        out = df.select([F.stddev(F.col(c)).alias(c) for c in df.columns])

    if describe_type == 'min':
        out = df.select([F.min(F.col(c)).alias(c) for c in df.columns])

    if describe_type == 'max':
        out = df.select([F.max(F.col(c)).alias(c) for c in df.columns])

    # Convert spark dataframe to pandas dataframe and update column names
    out = out.toPandas().transpose().reset_index()
    out.columns = ["variable", describe_type]

    return out


def mode_describe(df):
    """
    :WHAT IT IS: PYSPARK FUNCTION

    :WHAT IT DOES: This looks at the boolean values and provides information
    on the positive boolean figure.
    :RETURNS: Pandas dataframe with a boolean calculation.
    :OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset
    as a pandas dataframe.
    :TESTED TO RUN ON: test data in adruk.test.QA

    :AUTHOR: David Cobbledick
    :DATE: 01/12/2020
    :VERSION: 0.0.1
    :KNOWN ISSUES: This only works on boolean data.

    :PARAMETERS:
    * df = the dataframe that you are calling this on.
    """

    out = [
        (
            df.groupBy(column)
            .count()
            .sort(F.col("count"), ascending=False)
            .withColumn("variable", F.lit(column))
            .limit(1)
            .drop("count")
            .withColumnRenamed(column, "mode")
        )
        for column in df.columns
    ]

    out = reduce(F.DataFrame.unionAll, out)
    out = out.toPandas()
    return out


def special_describe(df, regex_dict):
    """
    :WHAT IT IS: PYSPARK FUNCTION

    :WHAT IT DOES: This looks at the value of the regex matches in the data.
    :RETURNS: Pandas dataframe with a regex match to count the values of the match.
    :OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset
    as a pandas dataframe.
    :TESTED TO RUN ON: test data in adruk.test.QA

    :AUTHOR: David Cobbledick
    :DATE: 01/12/2020
    :VERSION: 0.0.1
    :KNOWN ISSUES: This requires a regex_dict to be set up.

    :PARAMETERS:
    * df = the dataframe that you are calling this on.
    """

    out_dict = {}
    for k, v in regex_dict.items():
        out = df.select(
            [F.count(F.when(df[c].rlike(v), True)).alias(c) for c in df.columns]
        )
        out = out.toPandas().transpose().reset_index()
        out.columns = ["variable", k]
        out_dict[k] = out

    out_dfs = list(out_dict)
    out = out_dict.get(out_dfs[0])

    if len(out_dfs) > 1:
        for out_df in out_dfs[1:]:
            out = out.merge(out_dict.get(out_df), on="variable", how="inner")

    return out


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
    axis_=0,
    fillna_=0
):
    """
    :WHAT IT IS: PYSPARK FUNCTION
    :WHAT IT DOES: This function extends all of the functions listed in parameters
    to apply on a dataset.
    :RETURNS: Pandas dataframe with information on the data in the specified dataframe.
    :OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset as
    a pandas dataframe.
    :TESTED TO RUN ON: test data in adruk.test.describe

    :AUTHOR: David Cobbledick
    :DATE: 01/12/2020
    :VERSION: 0.0.1
    :KNOWN ISSUES: There are multiple columns that require boolean and some that do not
    work on boolean data.

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
                                    length_max_=False,
                                    percent_=False,
                                    pandas_=True,
                                    axis_=1,
                                    fillna_=0)
    """

    # default output and determines numeric columns - so that numeric methodologies are
    # only applied to the numeric columns where they are relevant
    # the base output file is 'out' to which all other measurment output is merged

    count = df.count()
    column_count = len(df.columns)

    out = pd.DataFrame(
        {
            "variable": [x[0] for x in df.dtypes],
            "type": [x[1] for x in df.dtypes],
            "total_columns": column_count,
            "total_rows": count,
        }
    )

    numeric_types = ["int", "bigint", "smallint", "double", "float", "decimal"]

    numeric_columns = list(out[out["type"].isin(numeric_types)]["variable"])

    out_columns = ["variable", "type", "total_rows", "total_columns"]

    # option to trim whitespace

    if trim_ is True:
        df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])

    # application of measure sub functions depending on user arguments

    if sum_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["sum"] = None
        else:
            sum_df = sum_describe(df.select(numeric_columns))
            out = out.merge(sum_df, on="variable", how="left")
        out_columns.append("sum")

    if positive_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["positive"] = None
        else:
            positive_df = positive_describe(df.select(numeric_columns))
            out = out.merge(positive_df, on="variable", how="left")

    if negative_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["negative"] = None
        else:
            negative_df = negative_describe(df.select(numeric_columns))
            out = out.merge(negative_df, on="variable", how="left")

    if zero_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["zero"] = None
        else:
            zero_df = zero_describe(df.select(numeric_columns))
            out = out.merge(zero_df, on="variable", how="left")

    if null_ is True or active_columns_ is True or count_ is True or all_ is True:
        null_df = null_describe(df)
        out = out.merge(null_df, on="variable", how="left")

    if active_columns_ is True or all_ is True:
        null_column_count = out[out["total_rows"] == out["null"]].shape[0]
        active_column_count = column_count - null_column_count
        out["active_columns"] = active_column_count
        out["null_columns"] = null_column_count
        out_columns.append("active_columns")
        out_columns.append("null_columns")

    if count_ is True or all_ is True:
        out["count"] = out["total_rows"] - out["null"]
        out_columns.append("count")

    if (active_columns_ is True or count_ is True) and null_ is False and all_ is False:
        out = out.drop(["null"], axis=1)

    if nan_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["NaN"] = None
        else:
            nan_df = nan_describe(df.select(numeric_columns))
            out = out.merge(nan_df, on="variable", how="left")

    if unique_ is True or all_ is True:
        unique_df = unique_describe(df)
        out = out.merge(unique_df, on="variable", how="left")
        out_columns.append("unique")

    if (special_ is True or all_ is True) and special_dict_ is not False:
        special_df = special_describe(df, special_dict_)
        out = out.merge(special_df, on="variable", how="left")

    if blank_ is True or all_ is True:
        blank_df = blank_describe(df)
        out = out.merge(blank_df, on="variable", how="left")

    if mean_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["mean"] = None
        else:
            mean_df = mean_describe(df.select(numeric_columns))
            out = out.merge(mean_df, on="variable", how="left")
        out_columns.append("mean")

    if stddev_ is True or all_ is True:
        if len(numeric_columns) == 0:
            out["stddev"] = None
        else:
            stddev_df = stddev_describe(df.select(numeric_columns))
            out = out.merge(stddev_df, on="variable", how="left")
        out_columns.append("stddev")

    if min_ is True or range_ is True or all_ is True:
        min_df = min_describe(df)
        out = out.merge(min_df, on="variable", how="left")
        if min_ is True or all_ is True:
            out_columns.append("min")

    if max_ is True or range_ is True or all_ is True:
        max_df = max_describe(df)
        out = out.merge(max_df, on="variable", how="left")
        if max_ is True or all_ is True:
            out_columns.append("max")

    if range_ is True or all_ is True:
        range_df = out[out["type"].isin(numeric_types)].reset_index(drop=True)
        range_df["range"] = range_df["max"] - range_df["min"]
        range_df = range_df[["variable", "range"]]
        out = out.merge(range_df, on="variable", how="left")
        out_columns.append("range")
        if min_ is False and all_ is False:
            out = out.drop(["min"], axis=1)
        if max_ is False and all_ is False:
            out = out.drop(["max"], axis=1)

    if mode_ is True or all_ is True:
        mode_df = mode_describe(df)
        out = out.merge(mode_df, on="variable", how="left")
        out_columns.append("mode")

    # if any measure of value length is selected in user arguments, a dataframe of
    # value lengths in the principle data frame is created and analysed

    if (
        length_mean_ is True
        or length_stddev_ is True
        or length_mode_ is True
        or length_min_ is True
        or length_max_ is True
        or length_range_ is True
        or all_ is True
    ):

        df = df.na.fill("")
        length_df = df.select(df.columns)
        for col in df.columns:
            length_df = length_df.withColumn(col + "_l", F.length(df[col]))
            length_df = length_df.drop(col)
        length_df = length_df.toDF(*[x[:-2] for x in length_df.columns])

    if length_mean_ is True or all_ is True:
        mean_length_df = mean_describe(length_df)
        mean_length_df.columns = ["variable", "length_mean"]
        out = out.merge(mean_length_df, on="variable", how="left")
        out_columns.append("length_mean")

    if length_stddev_ is True or all_ is True:
        stddev_length_df = max_describe(length_df)
        stddev_length_df.columns = ["variable", "length_stddev"]
        out = out.merge(stddev_length_df, on="variable", how="left")
        out_columns.append("length_stddev")

    if length_min_ is True or length_range_ is True or all_ is True:
        min_length_df = min_describe(length_df)
        min_length_df.columns = ["variable", "length_min"]
        out = out.merge(min_length_df, on="variable", how="left")
        if length_min_ is True or all_ is True:
            out_columns.append("length_min")

    if length_max_ is True or length_range_ is True or all_ is True:
        max_length_df = max_describe(length_df)
        max_length_df.columns = ["variable", "length_max"]
        out = out.merge(max_length_df, on="variable", how="left")
        if length_max_ is True or all_ is True:
            out_columns.append("length_max")

    if length_range_ is True or all_ is True:
        out["length_range"] = out["length_max"] - out["length_min"]
        out_columns.append("length_range")
        if length_min_ is False and all_ is False:
            out = out.drop(["length_min"], axis=1)
        if length_max_ is False and all_ is False:
            out = out.drop(["length_max"], axis=1)

    if length_mode_ is True or all_ is True:
        mode_length_df = mode_describe(length_df)
        mode_length_df.columns = ["length_mode", "variable"]
        out = out.merge(mode_length_df, on="variable", how="left")
        out_columns.append("length_mode")

    # dynamically orders output columns depemnding on user arguments

    fixed_columns = [
        "variable",
        "type",
        "total_rows",
        "total_columns",
        "active_columns",
        "null_columns",
        "sum",
        "mean",
        "stddev",
        "min",
        "max",
        "range",
        "mode",
        "unique",
        "length_mean",
        "length_stddev",
        "length_min",
        "length_max",
        "length_range",
        "length_mode",
    ]

    fixed_columns = [x for x in fixed_columns if x in out_columns]
    out = out.fillna(np.nan)

    if percent_ is True:
        percent_columns = [x for x in list(out) if x not in fixed_columns]

        for column in percent_columns:
            out[column + "_%"] = [(x / count) * 100 for x in out[column].astype(float)]

    out_columns = [x for x in list(out) if x not in fixed_columns]
    out_columns = fixed_columns + sorted(out_columns)

    out = out[out_columns]

    # fills na depending on user argument

    if fillna_ is not False:
        out = out.fillna(fillna_)

    # orientates output depending on user argument

    if axis_ == 0:
        out = out.transpose().reset_index()
        out.columns = ["summary"] + (list(out.iloc[0])[1:])
        out = out.iloc[1:].reset_index(drop=True)

    return out
