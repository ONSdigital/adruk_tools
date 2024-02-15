from pyspark.sql import (functions as F,
                         types as T,
                         window as W)

def deduplicate_ordered(dataframe, 
                        subset:list, 
                        order:list):
  """
  language
  --------
  pyspark
  
  what it does
  -------------
  * drops duplicates from a spark dataframe, but doesn't incur the 
  ... non-determinism of .dropDuplicates()
  
  returns
  -------
  deduplicated, ordered dataframe
  
  return type
  -------------
  spark dataframe

  author
  --------------
  Johannes Hechler
  
  version
  -------
  0.0.1
  
  date
  ----------------
  10/01/2024

  notes
  -------------
  * based on DAPCATS tip: 
  ... https://gitlab-app-l-01/DAP_CATS/troubleshooting/python-troubleshooting/-/blob/master/window_drop_duplicates.ipynb


  parameters
  --------------
  :dataframe = dataframe to deduplicate:
    `(datatype = spark dataframe)`, e.g. my_duplicated_dataframe
  :subset = names of columns to keep unique combinations of:
    `(datatype = list of strings)`, e.g. ['age', 'sex']
  :order = names of the variables to sort by, and their 
  ...respective sort order as either F.desc() or F.asc():
    `(datatype = list of column objects)`, e.g. [F.desc('income'), F.asc('date')]


  example
  ----------
  >>> deduplicate_ordered(dataframe = my_duplicated_dataframe, 
                          subset =  ['age', 'sex'], 
                          order = [F.desc('income'), 
                                      F.asc('date')])
  """
  # partition data by subset, 
  # then number the records in each partition
  # ... in the order of order_by
  dataframe = dataframe.withColumn("row_number", 
                                   F.row_number().\
                                   over(W.Window.partitionBy(subset).\
                                        orderBy(*order)
                                       )
                                  )

  # filter by rank, then remove auxiliary ranking column
  return dataframe.filter(F.col("row_number") == 1).drop('row_number')

def clean_header(column):
  """
  Cleans column headers by trimming leading/trailing whitespace, replace spaces 
  with underscores and makes everything lowercase. 
  
  language
  --------
  python
    
  returns
  -------
  cleaned column headers 
  
  return type
  -----------
  string
  
  author
  ------
  Alex Anthony
    
  date
  ------
  21/08/2023  
  
  version
  -------
  0.0.1
    
  dependencies
  ------------
  none
  
  parameters
  ----------
  columns = the name of the column header that are to be cleaned
  `(datatype = string)`.

  example
  -------
  >>> clean_header('column_name')
  
  """
  return column.strip().replace(' ','_').lower()

  



def remove_whitespace(column):
    """
    :LANGUAGE: pyspark

    :WHAT IT DOES: removes all whitespace from values in a column object of type string
    :RETURNS: column object with cleaned string values
    :OUTPUT VARIABLE TYPE: spark column object

    :AUTHOR: Johannes Hechler
    :DATE: 28/02/2022
    :VERSION: 0.0.1
    :DEPENDENCIES: pyspark.sql.functions must be present, prefixed as F

    :PARAMETERS:
      : dataset = name of spark string type column to clean:
        `(datatype = string)`, e.g. 'messy_column'

    :EXAMPLE:
    >>> remove_whitespace('messy_column')
    """

    return F.regexp_replace(F.col(column), r"\s+", "")


def clean_nino(column):
    """
    :LANGUAGE: pyspark

    :WHAT IT DOES: in a column object of type string
    * removes all whitespace from values (includes trimming)
    * makes all values upper case
    :RETURNS: column object with cleaned string values
    :OUTPUT VARIABLE TYPE: spark column object

    :AUTHOR: Johannes Hechler
    :DATE: 28/02/2022
    :VERSION: 0.0.1
    :DEPENDENCIES:
    * library pyspark.sql.functions must be loaded, prefixed as F
    * function remove_whitespace() must be present in the same library

    :PARAMETERS:
      : dataset = name of spark string type column to clean:
        `(datatype = string)`, e.g. 'messy_nino_column'

    :EXAMPLE:
    >>> clean_nino('messy_nino_column')
    """

    return F.upper(remove_whitespace(column))


def concatenate_columns(dataset, variable_new, variables, sep=" "):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: concatenates selected dataframe columns by selected seperator
    and adds it to the source dataframe under a selected name
    :RETURNS: dataframe with the concatenation as 1 additional variable.
    All original variable remain unchanged.
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframes

    :AUTHOR: Johannes Hechler
    :DATE: 12/12/2019
    :VERSION: 0.0.2


    :PARAMETERS:
    * dataset = spark dataframe whose variable to concatenate:
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables = list of names of variables to concatenate:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']
    * variable_new = how to call new, concatenated variable:
        `(datatype = string)`, e.g. 'names_all'
    * sep = separator between variable strings. Default = space:
        `(datatype = string)`, e.g. ','

    :EXAMPLE:
    >>> concatenate_columns(dataset = SomeDataFrame,
                            variable_new = 'address_concatenated',
                            variables = ['ADDRESS_LINE1', 'ADDRESS_LINE2'],
                            sep = ' ')
    """

    return dataset.withColumn(
        variable_new,  # create new variable, named as user specified
        F.concat_ws(sep, *variables),
    )  # fill variable with concatenation of selected variables, separated with
    # the chosen string




def name_split(data, strings_to_split, separator):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: splits strings into segments based on a rule the users provides.
    :RETURNS: spark dataframe with as many additional variables as there are strings in
    the longest input variable. all original variables unchanged.
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 11/10/2019
    :VERSION: 0.0.1


    :PARAMETERS:
      : data = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : strings_to_split = variables whose strings to split:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']
      : separator = what characters to split by:
        `(datatype = string or regular expression)`, e.g. '-' or [a-z]

    :EXAMPLE:
    >>> name_split(data = PDS,
                   strings_to_split = ['forename_clean', 'middle_name_clean',
                   'surname_clean'],
                   separator = '-')
    """

    for string in strings_to_split:
        # count how many sub-names any field will have at most,
        # i.e. how many new variables are needed
        nCols = (
            data.withColumn("split", F.split(data[string], separator))
            .withColumn("howManyNames", F.size("split"))
            .agg({"howManyNames": "max"})
            .collect()[0]["max(howManyNames)"]
        )

        # split a variable and create new variables for sub-names
        for part in range(
            0, nCols
        ):  # go through each group of sub-name names in turn and...
            data = data.withColumn(
                string + "_" + str(part + 1),
                F.split(data[string], separator).getItem(part),
            )  # ...assign them to a new variable.
            # part+1 so the new variables start with 1, not 0

    return data


def name_split_array(data, strings_to_split, separator, suffix=""):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: splits strings into segments based on a rule the users provides.
    :RETURNS: spark dataframe with 1 additional variable per input variable.
    All original variables unchanged.
    :OUTPUT VARIABLE TYPE: array

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Dave Beech, Johannes Hechler
    :DATE: 11/11/2019
    :VERSION: 0.0.2


    :PARAMETERS:
      : data = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : strings_to_split = variables whose strings to split:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']
      : separator = what characters to split by:
        `(datatype = string or regular expression)`, e.g. '-' or [a-z]

    :EXAMPLE:
    >>> name_split_array(data = PDS,
                   strings_to_split = ['forename_clean', 'middle_name_clean',
                   'surname_clean'],
                   separator = '-',
                   suffix = '_split')
    """

    # return data.select(['*'] + [F.split(F.col(s),
    # separator).alias(s + suffix) for s in strings_to_split])
    for column in strings_to_split:
        data = data.withColumn(column + suffix, F.split(F.col(column), separator))

    return data



def clean_postcode(dataset, variables, spaces):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: cleans and standardises strings, letting users specify how many
    internal spaces to keep. Intended for postcodes.
    :RETURNS: spark dataframe with original variables cleaned and overwritten.

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables = list of variables to clean
        `(datatype = list of strings)`, e.g. ['postcode', 'postcodeNHS']
    * spaces = list of variables to clean
        `(datatype = list of strings)`, e.g. ['postcode', 'postcodeNHS']

    :EXAMPLE:
    >>> clean_postcode(dataset = testDF,
                        variables = ['postcodeNHS', 'postcode'],
                        spaces = 2)

    """

    """
  ACTUAL CLEANING
  1. cycle through each selected variable in turn
  2. overwrite it with a version that (in this order)...
  2.1 ... replaces anything not character (of either case), numeric or a white space
  removed...
  2.2 ... reduces the length of any remaining spaces to what the user specified...
  2.3 ... trims preceeding and trailing white space...
  2.4 ... makes everything that remains upper case
  """
    for variable in variables:
        dataset = dataset.withColumn(
            variable,
            F.upper(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(variable, r"[^a-zA-Z0-9\s]", ""),
                        r"\s+",
                        " " * spaces,
                    )
                )
            ),
        )

    return dataset


def postcode_pattern(test_df, test_postcodes):

    """
    :WHAT IT IS: Function
    :WHAT IT DOES: tests if the values in a given variable follow the format of UK
    postcodes. Excludes non-numeric and non-alphabetical characters except horizontal
    spaces.
    :RETURNS: copy of the original dataframe, with a new variable called 'fitPattern',
    which is TRUE where a value fits the format, FALSE if it's not,
    and NULL if the test variable is NULL.

    :TESTED ON: UK Postcode Directory
    :FALSE NEGATIVES: catches all postcodes in UK Postcode Directory
    :FALSE POSITIVES: tests if a letter or number should be in a certain place,
    but not if that PARTICULAR letter/number should be there.
    If postcodes never use a certain character in a certain place that uses other
    characters, it doens't catch it.
    Rejects any format that isn't in the UK Postcode Directory.
    Designed to reject anything that isn't a number or letter, or not in the right place.
    But cannot test completely.
    :FULL LIST OF ACCEPTED FORMATS: see below

    :AUTHOR: Johannes Hechler
    :DATE: 27/08/2019
    :VERSION: 0.1


    :PARAMETERS:
      * test_df = name of dataframe that holds variable to test
        `(datatype = dataframe name, no string)`, e.g. PDS
      * test_variable = name of variable to match against reference
        `(datatype = string)`, e.g. 'postcode'


    :EXAMPLE:
    >>> postcode_pattern(testDF, 'postcode')

    """

    patterns = (
        "^([Gg][Ii][Rr] ?0[Aa]{2})$|^([Nn][Pp][Tt] ?[0-9]"
        "[AaBbD-Hd-hJjLlNnP-Up-uW-Zw-z]{2})$|"
        "^([A-Pa-pR-Ur-uWwYyZz][0-9]{1,2} ?[0-9][AaBbD-Hd-hJjLlNnP-Up-uW-Zw-z]{2})$|"
        "^([A-Pa-pR-Ur-uWwYyZz][A-Ha-hK-Yk-y][0-9]{1,2} ?[0-9]"
        "[AaBbD-Hd-hJjLlNnP-Up-uW-Zw-z]{2})$|"
        "^([A-Pa-pR-Ur-uWwYyZz][0-9][A-Za-z] ?[0-9][AaBbD-Hd-hJjLlNnP-Up-uW-Zw-z]{2})$|"
        "^([A-Pa-pR-Ur-uWwYyZz][A-Ha-hK-Yk-y][0-9][A-Za-z] ?[0-9]"
        "[AaBbD-Hd-hJjLlNnP-Up-uW-Zw-z]{2})$")

    return test_df.withColumn("fitsPattern", F.col(test_postcodes).rlike(patterns))


def postcode_split(dataset, variable, suffix, connection):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: checks if a string or parts of it follow postcode format and saves
    them in separate variables
    :RETURNS: spark dataframe with 4 new variables (postcode unit, postcode sector,
    postcode district, postcode area), 1 for each possible postcode part.
    Where a part doesn't follow a postcode pattern, the value is NULL/None.
    New variables are named after the source variable with an optional suffix.
    Original variable remains unchanged.

    :NOTES:
    * fails if input postcodes have no internal space (unless they are 7 characters long),
    or more than one. So the codes should first be cleaned.
    * the format check looks purely if a code has no characters/digits in the wrong
    places, as specified in the PAF Programmer's Guide.
    There is no check against any reference
    lookups like the National Statistics Postcode Lookup.
    * format requirements are summarised in PAF Programmer's Guide
    * Original method and SQL code developed by DWP in Oracle SQL.
    Adapted to Hive SQL and wrapped into a pyspark function in ONS.

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/19/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variable = variable to check and break into components
        `(datatype = 1 string)`, e.g. 'postcode'
    * suffix = what suffix to add to the names of the new, component variables.
        To distinguish them if more than 1 variable is broken into components.
        `(datatype = 1 string)`, e.g. '_domicile'
    * connection = name of the spark cluster to use for SQL computations
        `(datatype = 1 object name, no string)`, e.g. spark

    :EXAMPLE:
    >>> postcode_split(dataset = PDS,
                        variable = 'postcode',
                        suffix = '',
                        connection = session)

    """
    otherVars = ",".join([var for var in dataset.columns if var != variable])
    dataset.createOrReplaceTempView(
        "dataset"
    )  # register dataframe as spark table so SQL functions can find it
    return (
        connection.sql(
            """
  SELECT {1}, {0},  postcode_unit_validated, postcode_sector_validated,
  postcode_district_validated, postcode_area_validated FROM
  (SELECT *,
        CASE WHEN substr(postcode_format,1,5) = 'AANAN'
            AND substr(postcode_unit_validated,4,1) IN ('I','L','O','Q','Z')
            THEN NULL
            WHEN substr(postcode_format,1,4) = 'ANAN'
            AND substr(postcode_unit_validated,1,3) IN ('I','L','O','Q','Z')
            THEN NULL
            WHEN postcode_format IN ('AAN','AN','ANA','AANN')
            THEN NULL
            ELSE postcode_sector_unvalidated
       END AS postcode_sector_validated,
       CASE WHEN substr(postcode_format,1,4) = 'AANA'
            AND substr(postcode_unit_validated,4,1) IN ('I','L','O','Q','Z')
            THEN NULL
            WHEN substr(postcode_format,1,3) = 'ANA'
            AND substr(postcode_unit_validated,1,3) IN ('I','L','O','Q','Z')
            THEN NULL
            ELSE postcode_district_unvalidated
       END AS postcode_district_validated,
       CASE WHEN substr(postcode_format,1,2) = 'AA'
            THEN substr(postcode_unit_validated,1,2)
            WHEN substr(postcode_format,1,1) = 'A'
            THEN substr(postcode_unit_validated,1,1)
            ELSE NULL
      END AS postcode_area_validated
      FROM
      (
      SELECT *,
       translate(regexp_replace(upper(postcode_unit_validated), '\\s', ''),
                             'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                             'AAAAAAAAAAAAAAAAAAAAAAAAAANNNNNNNNNN') AS postcode_format,
       regexp_replace(
       CASE WHEN instr(postcode_unit_validated,' ') > 0
            THEN substr(postcode_unit_validated,1,instr(postcode_unit_validated,' ')+1)
            ELSE regexp_replace(substr(postcode_unit_validated,1,5),'\\s','')
       END,
       '\\s',
       '') AS postcode_sector_unvalidated,
       CASE WHEN instr(postcode_unit_validated,' ') > 0
            THEN substr(postcode_unit_validated,1,instr(postcode_unit_validated,' ')-1)
            ELSE regexp_replace(substr(postcode_unit_validated,1,4),'\\s','')
       END AS postcode_district_unvalidated
       FROM
            (SELECT *,
                CASE WHEN substr({0},1,2) IN ('XX','OO','ZZ')
               THEN NULL
               WHEN substr({0},1,1) IN ('Q','V','X')
               THEN NULL
               WHEN substr({0},2,1) IN ('I','J','Z')
               THEN NULL
               ELSE {0}
            END AS postcode_unit_validated
            FROM dataset
            ) A
            ) B
            ) C
      """.format(
                variable, otherVars
            )
        )
        .withColumnRenamed(
            "postcode_unit_validated", "postcode_unit_validated" + suffix
        )
        .withColumnRenamed(
            "postcode_sector_validated", "postcode_sector_validated" + suffix
        )
        .withColumnRenamed(
            "postcode_district_validated", "postcode_district_validated" + suffix
        )
        .withColumnRenamed(
            "postcode_area_validated", "postcode_area_validated" + suffix
        )
    )