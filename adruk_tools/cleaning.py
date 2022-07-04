import pyspark.sql.functions as F
#import cleaning_postcode_prep


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


def clean_names(dataset, variables):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: removes non-alphabetical characters (not case sensistive)
    from selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables added
    as new variables ending in '_clean'. All original variables unchanged
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframe
    :RUN TIME: 20-row test dataframe - 3s; full deaths registrations 2017 - 4s

    :AUTHOR: Johannes Hechler
    :DATE: 11/09/2019
    :VERSION: 0.0.1


    :PARAMETERS:
      : dataset = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : variables = list of variables to clean:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']

    :EXAMPLE:
    >>> clean_names(PDS, ['family_names', 'first_given_name'])

    """

    for variable in variables:  # loop over chosen variables one by one and...
        dataset = dataset.withColumn(
            variable + "_clean", F.upper(F.trim(F.regexp_replace(variable,
                                                                 "[^a-zA-Z]", "")))
        )  # remove anything not a character (of either case), then trim whitespace,
        # then make all upper case. save that as a new variable,
        # named after the input variable, with the suffix '_clean'

    return dataset


def clean_names_part(dataset, variables):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: removes illegal characters (anything not alphabetical or whitespace,
    not case-sensistive) from selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables overwritten
    with cleaned versions.
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframe
    :RUN TIME: 20-row test dataframe - 3s; full deaths registrations 2017 - 4s

    :AUTHOR: Johannes Hechler
    :DATE: 11/09/2019
    :VERSION: 0.0.3


    :PARAMETERS:
      : dataset = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : variables = list of variables to clean:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']

    :EXAMPLE:
    >>> clean_names(PDS, ['family_names', 'first_given_name'])

    """

    for variable in [
        name
        for name, dtype in dataset.select(*variables).dtypes
        if "array<string>" not in dtype
    ]:  # loop over chosen variables one by one and...
        dataset = dataset.withColumn(
            variable, F.upper(F.trim(F.regexp_replace(variable, r"[^a-zA-Z\s-]", "")))
        )  # remove anything not a character (of either case) or any length of whitespace,
        # then trim whitespace before the first/after the last character,
        # then make all upper case. save that as a new variable, named after the input
        # variable, with the suffix '_clean'

    for variable in [
        name
        for name, dtype in dataset.select(*variables).dtypes
        if "array<string>" in dtype
    ]:  # loop over chosen variables one by one and...
        dataset = dataset.withColumn(
            variable,
            F.split(
                F.upper(
                    F.trim(
                        F.regexp_replace(
                            F.concat_ws(
                                "@",  # concatenate the array elements with an '@'
                                F.col(variable),
                            ),
                            r"[^a-zA-Z\s-@]",  # remove anything not like these
                            "",
                        )  # replace it with this
                    )  # END TRIM
                ),  # END UPPER
                "@",
            ))

    return dataset


def clean_names_full(dataset, variables):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: removes non-alphabetical characters (not case sensistive) from
    selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables overwritten
    with cleaned versions.
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframe
    :RUN TIME: 20-row test dataframe - 3s; full deaths registrations 2017 - 4s

    :AUTHOR: Johannes Hechler
    :DATE: 11/09/2019
    :VERSION: 0.0.3


    :PARAMETERS:
      : dataset = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : variables = list of variables to clean:
        `(datatype = list of strings)`, e.g. ['forename', 'surname']

    :EXAMPLE:
    >>> clean_names_full(PDS, ['family_names', 'first_given_name'])

    """

    for variable in [
        name
        for name, dtype in dataset.select(variables).dtypes
        if "array<string>" not in dtype
    ]:  # loop over chosen variables one by one and...
        dataset = dataset.withColumn(
            variable, F.upper(F.trim(F.regexp_replace(variable, "[^a-zA-Z]", "")))
        )  # remove anything not a character (of either case), then trim whitespace,
        # then make all upper case. save that as a new variable,
        # named after the input variable, with the suffix '_clean'

    for variable in [
        name
        for name, dtype in dataset.select(variables).dtypes
        if "array<string>" in dtype
    ]:  # loop over chosen variables one by one and...
        dataset = dataset.withColumn(
            variable,
            F.split(
                F.upper(
                    F.trim(
                        F.regexp_replace(
                            F.concat_ws(
                                "@",  # concatenate the array elements with an '@'
                                F.col(variable),
                            ),
                            "[^a-zA-Z@]'`",  # remove anything not like these
                            "",
                        )  # replace it with this
                    )
                ),
                "@",
            )
        )

    return dataset


def array_to_columns(dataset, arrays, new_column_names, number_of_columns):
    """
    data_test = array_to_columns(dataset = data_test,
                              arrays = ['forename_clean', 'surname_clean'],
                              number_of_columns = 0)
    """

    for array, new_column_name in zip(arrays, new_column_names):
        for index in range(1, number_of_columns + 1):
            dataset = dataset.withColumn(
                new_column_name + "_" + str(index), F.col(array).getItem(index - 1)
            )

    return dataset


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


def concatenate_distinct(dataset, variable_new, variables_to_combine, separator):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: concatenates selected dataframe columns by selected separator and
    adds it to the source dataframe under a selected name
    :RETURNS: dataframe with the concatenation as 1 additional variable.
    All original variable remain unchanged.
    :OUTPUT VARIABLE TYPE: string

    :TESTED TO RUN ON: spark dataframes

    :AUTHOR: Johannes Hechler
    :DATE: 30/07/2020
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe whose variable to concatenate:
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variable_new = what to call new, concatenated variable:
        `(datatype = string)`, e.g. 'names_all'
    * variables_to_combine = names of variables to concatenate:
        `(datatype = list of string)`, e.g. ['surname', 'surname_former',
        'surname_preferred']
    * separator = what to put in between concatenated values:
        `(datatype = string)`, e.g. ' '

    :EXAMPLE:
    >>> concatenate_distinct( variables_to_combine = ['forename_clean',
                                                      'forename_clean',
                                                      'middle_name_clean'],
                              dataset = data,
                              variable_new = 'all_names',
                              separator = ' ')
    """

    return dataset.withColumn(
        variable_new,
        F.concat_ws(separator, F.array_distinct(F.array(variables_to_combine))),
    )


def date_recode(dataset, variable_input, variable_output, date_format="yyyy/MM/dd"):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: breaks date variable into day, month, year and saves them as
    separate variables. User specifies the format of the input date, and the name of
    the output variables. If that's the same as the input variable it gets overwritten.
    :RETURNS: spark dataframe with 4 additional variables (full date, day, month, year).
    Variable names are as specified by user, plus suffixes '_day', '_month', '_year'.
    :OUTPUT VARIABLE TYPE:
    * full date: date
    * day, month, year: int
    * works on variables in formats: date, datestamp, string


    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe with date variable to recode
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variable_input = variable to convert to date format and break into components
        `(datatype = 1 string)`, e.g. 'date_of_birth'
    * variable_output = what to call the output variable
        `(datatype = 1 string)`, e.g. 'dob'
    * date_format = what format the input variable records dates, as expressed by
    the to_date() function. Example: 2019/12/17 is expressed as 'yyyy/MM/dd'.
      * Default value = 'yyyy/MM/dd'.
      * If variable is not of type string (e.g. date or datestamp) this gets ignored,
      even if it specifies the wrong format.
        `(datatype = 1 string)`, e.g. 'yyyy/MM/dd'

    :EXAMPLE:
    >>> date_recode(dataset = PDS,
                    variable_input = 'date_of_birth',
                    variable_output = 'date_of_birth',
                    date_format = 'yyyy/MM/dd')

    """

    dataset = dataset.withColumn(
        variable_output, F.to_date(F.col(variable_input), format=date_format)
    )  # convert timestamp to date format
    dataset = dataset.withColumn(
        variable_output + "_year", F.year(dataset[variable_output])
    )  # extract year into separate column
    dataset = dataset.withColumn(
        variable_output + "_month", F.month(dataset[variable_output])
    )  # extract year into separate column
    dataset = dataset.withColumn(
        variable_output + "_day", F.dayofmonth(dataset[variable_output])
    )  # extract year into separate column

    return dataset


def LAlookup(
    test_df,
    test_variables,
    reference_table,
    reference_variable,
    LA_code_variable,
    connection,
):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: takes a selected variable, finds its values in a selected
    reference file and returns corresponding values of another variable in that reference
    :RETURNS: copy of the original dataframe with the returned values in a new variable
    named after the input variable and the joined-on variable.
    :NOTES:
    * built for postcodes; works for other variables but beware of the inbuilt cleaning
    * the code cleans both the lookup and the reference variable (removes spaces,
    trims whitespace, makes all upper case)
    * even if no matching value is found the original variable is overwritten with a
    cleaned version

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE:
    :VERSION: 0.0.1

    :PARAMETERS:
    * test_df = spark dataframe containing the variable to look up in reference
        `(datatype = dataframe name, no string)`, e.g. PDS
    * test_variable = name of variable to look up in reference
        `(datatype = string)`, e.g. 'postcode'
    * reference_table = database and table name of reference table to use
        `(datatype = string)`, e.g. 'postcode_directory.onspd_may_2018_uk_std'
    * reference_variable = name of reference variable to find test_variable values in
        `(datatype = string)`, e.g. 'pcd'
    * LA_code_variable = name of variable in reference file to join onto test_df
        `(datatype = string)`, e.g. 'postcode'
    * connection = name of spark cluster to use
        `(datatype = cluster name, no string)`, e.g. spark

    :EXAMPLE:
    >>> lookup(test_df = testDF,
              test_variable = 'postcode',
              reference_table = 'postcode_directory.onspd_may_2018_uk_std',
              reference_variable = 'pcd',
              LA_code_variable = 'oa11',
              connection = spark).show()
    """

    reference = connection.sql(
        "SELECT {0}, {1} FROM {2}".format(
            reference_variable, LA_code_variable, reference_table
        )
    )  # pull the reference variable into a spark dataframe to use
    reference = reference.withColumn(
        reference_variable,
        F.upper(F.trim(F.regexp_replace(reference_variable, r"[^a-zA-Z0-9][\s]*", ""))),
    )  # clean reference value, remove illegal characters and white space
    for test_variable in test_variables:
        test_df = test_df.withColumn(
            test_variable,
            F.upper(F.trim(F.regexp_replace(test_variable, r"[^a-zA-Z0-9][\s]*", ""))),
        )  # clean postcode variable; ensure it has the same format as reference codes
        test_df = (
            test_df.join(
                F.broadcast(reference),
                on=(test_df[test_variable] == reference[reference_variable]),
                how="left",
            )
            .withColumnRenamed(LA_code_variable, LA_code_variable + "_" + test_variable)
            .drop(reference_variable)
        )  # join reference to test dataframe, where test variable matches any reference
        # variable value within accepted levenshtein distance.
        # This will leave a new, merged-on variable that is Null for test values that
        # weren't matched. remove reference variable, it was only used for linking,
        # don't need the variable itself.

    return test_df  # remove reference variable(s) that were joined in;
    # repartition to improve processing speed


def make_missing_none(data, columns):
    """
    :WHAT IT IS: pyspark function

    :WHAT IT DOES: recodes different ways of expressing missingness into explicit
    NULL/None values, in all variables of a spark dataframe
    :RETURNS: spark dataframe with all variables recoded

    :AUTHOR: Johannes Hechler
    :DATE: 11/05/2020
    :VERSION: 0.0.2
    :CHANGES FROM PREVIOUS VERSION: can now select columns to clean


    :PARAMETERS:
      * dataset = spark dataframe
        `(datatype = dataframe name, no string)`, e.g. PDS
      * columns = list of strings
        `(datatype = list of strings])`, e.g. ['forename', 'surname']

    :EXAMPLE:
    >>> make_missing_none(PDS, columns = ['forename', 'surname'])

    """

    for column in columns:  # for each variable in turn, do this following:
        data = data.withColumn(
            column,  # overwrite the column with a copy of itself where values are
            # recoded like this...
            F.when(
                data[column].isin(
                    ["", "NULL", "NAN", "NA", "UNK"]
                ),  # if they are any of these string...
                None,
            ).otherwise(
                data[column]
            ),  # ... recode that value to NULL/None... else leave them unchanged
        )
    for column in [
        column
        for column, datatype in data.dtypes
        if datatype in ("float", "double", "string")
    ]:  # now look only at variables of these types and...
        data = data.withColumn(
            column,  # overwrite the column with a copy of itself where values are
            # recoded like this...
            F.when(
                F.isnan(data[column]), None  # ... if a value is NaN (not a number)...
            ).otherwise(
                data[column]
            ),  # ...turn it to NULL/None... else leave it unchanged
        )

    return data


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


def NHS_postcode_recode(datasetNHS, variables_old, variables_new, connection):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: recodes variables saying if a corresponding value in another variable
    is a valid NHS postcode and if it refers to the UK, an EU country, or elsewhere in
    the world
    :RETURNS: spark dataframe with new, recoded variables added (as many as input
    variables), named after the input variables. Original variables unchanged.
    :NOTES:
    * function uses custom NHS-postcode lookup file that was ingested into DAP.
    It was built by Data Architecture Division for a specific task and is not regularly
    updated.
    * pulls in another user-defined function to clean postcodes before lookup.
    Other teams may not have access to the function library.
    * we hard-coded the definition of what postcodes are UK and EU.
    If EU membership changes this will be outdated.
    * non-EU postcodes are defined as any reference codes that are in neither definition.
    Anything not found in those 3 definitions is classified as not a NHS postcode

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * datasetNHS = spark dataframe with postcode variables
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables_old = variables to check for NHS postcodes
        `(datatype = list of strings)`, e.g. ['postcode1', 'postcode2']
    * variables_new = what to call the new variables that say what kind of
        NHS postcode was found if any
        `(datatype = list of strings)`, e.g. ['NHS1', 'NHS2']
    * connection = name of spark cluster to use for connecting to NHS postcode lookup
        `(datatype = 1 object name, no string)`, e.g. spark


    :EXAMPLE:
    >>> NHS_postcode_recode(datasetNHS = testDF,
                            variables_old = ['postcode1', 'postcode2'],
                            variables_new = ['NHS1', 'NHS2'],
                            connection = spark)

    """

    NHSlookup = connection.sql(
        "SELECT postcode FROM nhs_postcodes_country_lookup.nhs_country_postcodes_std"
    )  # import lookup of NHS postcodes to countries
    NHSlookup = cleaning_postcode_prep.clean_postcode(
        dataset=NHSlookup,  # clean imported reference postcodes to ensure they are
        # in the same format as the postcodes to be checked
        variables=["postcode"],
        spaces=0,
    ).toPandas()
    UK = [
        "ZZ993CZ",
        "Z993CZ",
        "ZZ993GZ",
        "ZZ991WZ",
        "ZZ992WZ",
        "ZZ993WZ",
        "ZZ993VZ",
    ]  # define which postcodes refer to the UK
    EU = [
        "ZZ994QZ",
        "ZZ994GZ",
        "ZZ994LZ",
        "ZZ992CZ",
        "ZZ994YZ",
        "ZZ995VZ",
        "ZZ994ZZ",
        "ZZ994HZ",
        "ZZ994MZ",
        "ZZ994EZ",
        "ZZ992DZ",
        "ZZ994UZ",
        "ZZ996AZ",
        "ZZ995XZ",
        "ZZ994FZ",
        "ZZ997LZ",
        "ZZ994BZ",
        "ZZ994RZ",
        "ZZ994XZ",
        "ZZ993AZ",
        "ZZ997RZ",
        "ZZ997SZ",
        "ZZ992EZ",
        "ZZ995BZ",
        "ZZ994JZ",
        "ZZ995YZ",
        "ZZ995UZ",
    ]  # define which postcodes refer to non-UK EU countries
    NONEU = [
        element for element in list(set(NHSlookup.postcode)) if element not in UK + EU
    ]  # define all other postcodes as non-EU countries

    """
  ACTUAL RECODE
  1. loop through original variables in turn (NB uses not their name but index number
  so we can also refer to their corresponding new variable name later)
  2. create new variable...
  3. ...that's simply a cleaned version of the corresponding old variable...
  4. ... then overwrite that new variable with a recode of itself like this...
  4.1 ... if it's in the UK list, make the record 'UK'
  4.2 ... if it's in the EU list, make the record 'EU'
  4.3 ... if it's in the non-EU list, make the record 'NONEU'
  4.4 ... if the value is in neither, say that it's no NHS postcode at all
  """
    for a in range(len(variables_old)):
        datasetNHS = datasetNHS.withColumn(
            variables_new[a],
            F.upper(
                F.trim(F.regexp_replace(variables_old[a], r"[^a-zA-Z0-9][\s]*", ""))
            ),
        ).withColumn(
            variables_new[a],
            F.when(F.col(variables_new[a]).isin(UK), F.lit("UK"))
            .when(F.col(variables_new[a]).isin(EU), F.lit("EU"))
            .when(F.col(variables_new[a]).isin(NONEU), F.lit("NONEU"))
            .otherwise(F.lit("NONHSPOSTCODE")),
        )

    return datasetNHS


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


def rename_columns(dataset, variable_names_old, variable_names_new):
    """
    :WHAT IT IS: FUNCTION

    :WHAT IT DOES: renames columns in a spark dataframe according to a user-defined
    lookup
    :RETURNS: spark dataframe with columns renamed. Columns not in the lookup are
    unchanged.
    :OUTPUT VARIABLE TYPE: function does not affect variable types, only names.

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe
        `(datatype = 1 dataframe name, no string)`, e.g. PDS
    * variable_names_old = what the old variables are called
        `(datatype = list of strings)`, e.g. ['gender', 'fname']
    * variable_names_new = how to call the new variables
        `(datatype = list of strings)`, e.g. ['sex', 'forename']
      * provide in order corresponding to old names
      * every listed old name MUST have a new value assigned.
      If variable should NOT be renamed, assign None


    :EXAMPLE:
    >>> rename_columns(dataset = PDS,
                      variable_names_old = ['gender', 'fname'],
                      variable_names_new = ['sex', 'forename'])

    """
    variable_lookup = dict(
        zip(variable_names_old, variable_names_new)
    )  # turns old and new variable names into dictionary
    variable_lookup = {
        key: value for key, value in variable_lookup.items() if value
    }  # filter lookup: remove keys with empty values
    for (
        old_name,
        new_name,
    ) in variable_lookup.items():  # Loop over key-value pairs of dict
        dataset = dataset.withColumnRenamed(old_name, new_name)  # Rename

    return dataset


def sex_recode(dataset, variable_input, variable_output):
    """
    :WHAT IT IS: pyspark function

    :WHAT IT DOES: recodes different sets of values to 1, 2, 3 or None/NULL
    :RETURNS: spark dataframe with recode saved into new variable, or original
    variable overwritten
    :OUTPUT VARIABLE TYPE: int
    :NOTES:
    * recode values are hardcoded into the function like this:
    * [1, 'MALE', 'M'] becomes 1
    * [2, 'FEMALE', 'F'] becomes 2
    * [0, 3, 'I']  becomes 3
    * these values were chosen purely because they appeared in the test data used.
    If your data uses any other codes, the function fails.
    * the function expects strings to be in upper case: 'MALE' recodes to 1,
    but 'male' gets ignored and becomes NULL/None.

    :TESTED TO RUN ON: spark dataframe

    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * dataset = spark dataframe that holds the variable to be recoded
        `(datatype = 1 dataframe name, no string)`, e.g. PDS
    * variable_input = name of the variable to recode
        `(datatype = 1 string)`, e.g. 'gender'
    * variable_output = what to call the recoded variable
        `(datatype = 1 string)`, e.g. 'sex_recoded'

    :EXAMPLE:
    >>> sex_recode(dataset = PDS,
                  variable_input = 'sex',
                  variable_output = 'sex_recoded')

    """

    male = [1, "MALE", "M"]  # define what values to recode to 1
    female = [2, "FEMALE", "F"]  # define what value to recode to 2
    other = [0, 3, "I"]  # define what values to recode to 3

    """
  ACTUAL RECODE
  1. create new variable of chosen name (if name is the same as the input variable
  it gets overwritten)
  2. assign values as defined above
  3. if the original value is in neither definition, make in None/NULL
  """
    dataset = dataset.withColumn(
        variable_output,
        F.when(F.col(variable_input).isin(male), 1)
        .when(F.col(variable_input).isin(female), 2)
        .when(F.col(variable_input).isin(other), 3)
        .otherwise(None),
    )

    return dataset


def space_to_underscore(df):

    """
    :WHAT IT IS: pyspark function

    :WHAT IT DOES: replaces spaces with underscores
    :RETURNS: spark dataframe with no column names containing spaces
    :AUTHOR: Sophie-Louise Courtney
    :DATE: 02/03/2021
    :VERSION: 0.0.1


    :PARAMETERS:
    * df = spark dataframe
        `(datatype = dataframe name, not string)`, e.g. ESC
    """

    return df.replace(" ", "_")


def title_remove(dataset, variables):
    """
    :WHAT IT IS: pyspark function

    :WHAT IT DOES: removes preceeding/trailing white space from strings,
    then makes them upper-case, then removes selected titles at the very start
    :RETURNS: spark dataframe with selected variable cleaned, trimmed, made upper case.
    Other variables unchanged
    :OUTPUT VARIABLE TYPE: string
    :NOTES:
    * title list is hard-coded into the function.
    * It is based on what we found in census data
    * list of titles removed: DAME, DR, MR, MSTR, LADY, LORD, MISS, MRS, MS, SIR, REV,
    plus any amount of whitespace behind it
    * according to census data, we needn't for spelling out the acronyms
    (e.g. mister, professor)
    * no need for longer list (e.g. as in Wikipedia::English honorifics)
    * the function cleans the data before removing titles (trims whitespace
    make everything upper case), but more cleaning should be done beforehand,
    e.g. remove dots


    :TESTED TO RUN ON: spark dataframe
    :RUN TIME: 20-row test dataframe - 3s; full PDS stock 2019 - 2s

    :AUTHOR: Johannes Hechler
    :DATE: 25/09/2019
    :VERSION: 0.0.1


    :PARAMETERS:
    * data = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables = variables to remove titles from:
        `(datatype = list of strings)`, e.g. ['firstname', 'secondname']

    :EXAMPLE:
    >>> title_remove(dataset = PDS,
                    variables = ['first_given_name', 'family_name', 'other_given_names'])
    """

    title_filter = (r"^DAME\s+|^DR\s+|^MR\s+|^MSTR\s+|^LADY\s+|^LORD\s+|"
                    r"^MISS\s+|^MRS\s+|^MS\s+|^SIR\s+|^REV\s+")

    for variable in variables:  # loop over selected variables and...
        dataset = dataset.withColumn(
            variable,
            F.regexp_replace(F.trim(F.upper(F.col(variable))), title_filter, ""),
        )  # ... make them upper-case, trim predeeding/trailing whitespace,
        # then remove any of the defines titles

    return dataset
