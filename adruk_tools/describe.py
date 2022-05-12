from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
import numpy as np

# Sample docstring from one function.
# Keeping here for reference
# Could be improved

"""
:WHAT IT IS: PYSPARK FUNCTION

:WHAT IT DOES: This looks at the negative values in the dataset and counts
them by column.
:RETURNS: Pandas dataframe with a numerical sum showing negative.
:OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset.
:TESTED TO RUN ON: test data in adruk.test.QA

:AUTHOR: David Cobbledick
:DATE: 01/12/2020
:VERSION: 0.0.1
:KNOWN ISSUES: This will not work if there is a boolean datatype.

:PARAMETERS:
* df = the dataframe that you are calling this on.
"""

# not used in extended describe - do we keep? 
# -------------------------------------------


def pandas_describe(df):

    out = df.describe()
    out = out.toPandas().transpose().reset_index()
    out.columns = ["variable"] + (list(out.iloc[0])[1:])
    out = out.iloc[1:]
    return out

############################
#add else if type_describe is wrong 

def describe(df, describe_type):
  """
  :WHAT IT IS: PYSPARK FUNCTION

  :WHAT IT DOES: This looks at the describe_type values in the dataset and counts them by column.
  :RETURNS: Pandas dataframe with a numerical sum showing negative.
  :OUTPUT VARIABLE TYPE: Out 1: Information on the data you have in the dataset.
  :TESTED TO RUN ON:  see above df   ###CHANGE 

  :AUTHOR: Silvia Bardoni
  :DATE: 10/05/2022
  :VERSION: 0.0.1
  :KNOWN ISSUES: This will not work if there is a boolean datatype.    #TO CHECK!!

  :PARAMETERS:
  * df = the dataframe that you are calling this on.
  * describy_type : the statistic of interest (i.e. 'sum' or 'mean' or 'positive')
  """
  
  
  if describe_type == "sum":
      """
      :WHAT IT DOES: This provides a count of the values in any numeric columns.
      """
      out = df.groupBy().sum()
      out = out.toDF(*[x.replace("(", ")").split(")")[1] for x in out.columns])


  if describe_type == "positive":
      """
      :WHAT IT DOES: This looks at the positive values in the dataset and counts
      """     
      out = df.select([F.count(F.when(df[c] > 0, True)).alias(c) for c in df.columns])
            
    
  if describe_type == "negative":
      """
      :WHAT IT DOES: This looks at the negative values in the dataset and counts
      """      
      out = df.select([F.count(F.when(df[c] < 0, True)).alias(c) for c in df.columns])
        
    
  if describe_type =='zero':
      """
       :WHAT IT DOES: This looks at the false values in the dataset and counts
       them by column.
      """      
      out = df.select([F.count(F.when(df[c] == 0, True)).alias(c) for c in df.columns])
      
  
  if describe_type =='null':
      """
       :WHAT IT DOES: This looks at the null values in the dataset and counts
        them by column.
      """     
      out = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
      

  if describe_type =='nan':
      """
       :WHAT IT DOES: This looks at the NaN values in the dataset and counts
        them by column.
      """     
      out = df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns])
      
  
  if describe_type =='unique':
      """
       :WHAT IT DOES: This looks at the unique values in the dataset and counts
        them by column.
      """     
      out = df.select([F.col(c).cast(T.StringType()).alias(c) for c in df.columns])
      out = out.agg(*(F.countDistinct(F.col(c)).alias(c) for c in out.columns))
                

  if describe_type == "blank":
      """
      :WHAT IT DOES: This looks at the blank values in the dataset and counts
      """      
      out = df.select([F.count(F.when(df[c] == "", True)).alias(c) for c in df.columns])
                   

  if describe_type == "mean":
      """
      :WHAT IT DOES: This looks at the mean values in the dataset and counts
      """      
      out = df.groupBy().mean()
      out = out.toDF(*[x.replace("(", ")").split(")")[1] for x in out.columns])
      
      
  if describe_type == "stddev":
      """
      :WHAT IT DOES: This looks at the stddev values in the dataset and counts
      """      
      out = df.select([F.stddev(F.col(c)).alias(c) for c in df.columns])
      
      
  if describe_type == "min":
      """
      :WHAT IT DOES: This looks at the minimum values in the dataset and counts
      """      
      
      out = df.select([F.min(F.col(c)).alias(c) for c in df.columns])
      
      
  if describe_type == "max":
      """
      :WHAT IT DOES: This looks at the maximum values in the dataset and counts
      """      
      out = df.select([F.max(F.col(c)).alias(c) for c in df.columns])
      
                   
                
  out = out.toPandas().transpose().reset_index()
  out.columns = ["variable", describe_type]
  return out

########################
#Testing section  with ad-hoc dataframe for 

import adruk_tools.adr_functions as adr
spark = adr.session_small()

df = pd.DataFrame({
    "col1": ['A', 'A', None, 'C', ''],
    "col2": [1, 2, 2, None, -2],
    "col3": [15, 0, -6, -5, 10], 
})
df = spark.createDataFrame(df)
df.show() 


result_sum= describe(df, 'sum') 
result_sum
result_positive = describe(df, 'positive')
result_positive
result_negative = describe(df, 'negative')
result_negative
result_zero = describe(df, 'zero')
result_zero
result_null = describe(df, 'null')
result_null
result_nan = describe(df, 'nan')
result_nan
result_unique = describe(df,'unique')
result_unique
result_blank = describe(df,'blank')
result_blank
result_mean = describe(df,'mean')
result_mean
result_stddev = describe(df,'stddev')
result_stddev
result_max = describe(df,'max')
result_max
result_min = describe(df,'min')
result_min



###############################
# update references to functions in here
# eg

# sum_df = sum_describe(df.select(numeric_columns)) becomes
# sum_df = describe(df.select(numeric_columns), "sum")


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
