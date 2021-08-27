Help on function LAlookup in adruk_tools.cleaning:

adruk_tools.cleaning.LAlookup = LAlookup(test_df, test_variables, reference_table, reference_variable, LA_code_variable, connection)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: takes a selected variable, finds its values in a selected reference file and returns corresponding values of another variable in that reference
    :RETURNS: copy of the original dataframe with the returned values in a new variable named after the input variable and the joined-on variable.
    :NOTES:
    * built for postcodes; can work for other variables but beware of the inbuilt cleaning:
    * the code cleans both the lookup and the reference variable (removes spaces, trims whitespace, makes all upper case)
    * even if no matching value is found the original variable is overwritten with a cleaned version
    
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

Help on function NHS_postcode_recode in adruk_tools.cleaning:

adruk_tools.cleaning.NHS_postcode_recode = NHS_postcode_recode(datasetNHS, variables_old, variables_new, connection)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: recodes variables saying if a corresponding value in another variable is a valid NHS postcode and if it refers to the UK, an EU country, or elsewhere in the world
    :RETURNS: spark dataframe with new, recoded variables added (as many as input variables), named after the input variables. Original variables unchanged.
    :NOTES:
    * function uses custom NHS-postcode lookup file that was ingested into DAP. It was built by Data Architecture Division for a specific task and is not regularly updated.
    * pulls in another user-defined function to clean postcodes before lookup. Other teams may not have access to the function library.
    * we hard-coded the definition of what postcodes are UK and EU. If EU membership changes this will be outdated.
    * non-EU postcodes are defined as any reference codes that are in neither definition. Anything not found in those 3 definitions is classified as not a NHS postcode
    
    :TESTED TO RUN ON: spark dataframe
    
    :AUTHOR: Johannes Hechler
    :DATE: 17/12/2019
    :VERSION: 0.0.1
    
    
    :PARAMETERS:
    * datasetNHS = spark dataframe with postcode variables
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables_old = variables to check for NHS postcodes
        `(datatype = list of strings)`, e.g. ['postcode1', 'postcode2']
    * variables_new = what to call the new variables that say what kind of NHS postcode was found if any
        `(datatype = list of strings)`, e.g. ['NHS1', 'NHS2']
    * connection = name of spark cluster to use for connecting to NHS postcode lookup
        `(datatype = 1 object name, no string)`, e.g. spark
        
    
    :EXAMPLE:
    >>> NHS_postcode_recode(datasetNHS = testDF,
                            variables_old = ['postcode1', 'postcode2'],
                            variables_new = ['NHS1', 'NHS2'],
                            connection = spark)

Help on function array_to_columns in adruk_tools.cleaning:

adruk_tools.cleaning.array_to_columns = array_to_columns(dataset, arrays, new_column_names, number_of_columns)
    data_test = array_to_columns(dataset = data_test, 
                              arrays = ['forename_clean', 'surname_clean'], 
                              number_of_columns = 0)

Help on function clean_names in adruk_tools.cleaning:

adruk_tools.cleaning.clean_names = clean_names(dataset, variables)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: removes non-alphabetical characters (not case sensistive) from selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables added as new variables ending in '_clean'. All original variables unchanged
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

Help on function clean_names_full in adruk_tools.cleaning:

adruk_tools.cleaning.clean_names_full = clean_names_full(dataset, variables)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: removes non-alphabetical characters (not case sensistive) from selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables overwritten with cleaned versions.
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

Help on function clean_names_part in adruk_tools.cleaning:

adruk_tools.cleaning.clean_names_part = clean_names_part(dataset, variables)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: removes illegal characters (anything not alphabetical or whitespace, not case-sensistive) from selected string variables
    :RETURNS: spark dataframe with cleaned version of selected variables overwritten with cleaned versions.
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

Help on function clean_postcode in adruk_tools.cleaning:

adruk_tools.cleaning.clean_postcode = clean_postcode(dataset, variables, spaces)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: cleans and standardises strings, letting users specify how many internal spaces to keep. Intended for postcodes.
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

Help on function concatenate_columns in adruk_tools.cleaning:

adruk_tools.cleaning.concatenate_columns = concatenate_columns(dataset, variable_new, variables, sep=' ')
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: concatenates selected dataframe columns by selected seperator and adds it to the source dataframe under a selected name
    :RETURNS: dataframe with the concatenation as 1 additional variable. all original variable remain unchanged.
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

Help on function concatenate_distinct in adruk_tools.cleaning:

adruk_tools.cleaning.concatenate_distinct = concatenate_distinct(dataset, variable_new, variables_to_combine, separator)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: concatenates selected dataframe columns by selected separator and adds it to the source dataframe under a selected name
    :RETURNS: dataframe with the concatenation as 1 additional variable. all original variable remain unchanged.
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
        `(datatype = list of string)`, e.g. ['surname', 'surname_former', 'surname_preferred']
    * separator = what to put in between concatenated values:
        `(datatype = string)`, e.g. ' '
    
    :EXAMPLE:
    >>> concatenate_distinct( variables_to_combine = ['forename_clean', 
                                                      'forename_clean', 
                                                      'middle_name_clean'], 
                              dataset = data, 
                              variable_new = 'all_names',
                              separator = ' ')

Help on function date_recode in adruk_tools.cleaning:

adruk_tools.cleaning.date_recode = date_recode(dataset, variable_input, variable_output, date_format='yyyy/MM/dd')
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: breaks date variable into day, month, year and saves them as separate variables. User specifies the format of the input date, and the name of the output variables. If that's the same as the input variable it gets overwritten.
    :RETURNS: spark dataframe with 4 additional variables (full date, day, month, year). Variable names are as specified by user, plus suffixes '_day', '_month', '_year'.
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
    * date_format = what format the input variable records dates, as expressed by the to_date() function. Example: 2019/12/17 is expressed as 'yyyy/MM/dd'.
      * Default value = 'yyyy/MM/dd'.
      * If variable is not of type string (e.g. date or datestamp) this gets ignored, even if it specifies the wrong format.
        `(datatype = 1 string)`, e.g. 'yyyy/MM/dd'
    
    :EXAMPLE:
    >>> date_recode(dataset = PDS,
                    variable_input = 'date_of_birth',
                    variable_output = 'date_of_birth',
                    date_format = 'yyyy/MM/dd')

Help on function make_missing_none in adruk_tools.cleaning:

adruk_tools.cleaning.make_missing_none = make_missing_none(data, columns)
    WHAT IT IS: FUNCTION
    
    WHAT IT DOES: recodes different ways of expressing missingness into explicit NULL/None values, in all variables of a spark dataframe
    RETURNS: spark dataframe with all variables recoded
    
    TESTED TO RUN ON: spark dataframe
    
    AUTHOR: Johannes Hechler
    DATE: 11/05/2020
    VERSION: 0.0.2
    CHANGES FROM PREVIOUS VERSION: can now select columns to clean
    
    
    :PARAMETERS:
      : dataset = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
      : columns = list of strings:
        `(datatype = list of strings])`, e.g. ['forename', 'surname']
    
    :EXAMPLE:
    >>> make_missing_none(PDS, columns = ['forename', 'surname'])

Help on function make_test_df in adruk_tools.cleaning:

adruk_tools.cleaning.make_test_df = make_test_df(session_name)
    WHAT IT IS: PySpark Function
    WHAT IT DOES: creates a dataframe with several columns of different data types for testing purposes. Intentionally includes various errors, e.g. typos.
    RETURNS: spark dataframe
    
    AUTHOR: Johannes Hechler, Ben Marshall-Sheen
    DATE: 17/12/2020
    VERSION: 0.2
    CHANGES FROM PREVIOUS VERSION: added boolean variable
    
    :PARAMETERS:
      :session_name = name of the spark session to use:
        `(datatype = session name, unquoted)`, e.g. spark
    
    :EXAMPLE:
    >>> make_test_df(spark)

Help on function name_split in adruk_tools.cleaning:

adruk_tools.cleaning.name_split = name_split(data, strings_to_split, separator)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: splits strings into segments based on a rule the users provides.
    :RETURNS: spark dataframe with as many additional variables as there are strings in the longest input variable. all original variables unchanged.
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
                   strings_to_split = ['forename_clean', 'middle_name_clean', 'surname_clean'],
                   separator = '[\s|-]+')

Help on function name_split_array in adruk_tools.cleaning:

adruk_tools.cleaning.name_split_array = name_split_array(data, strings_to_split, separator, suffix='')
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: splits strings into segments based on a rule the users provides.
    :RETURNS: spark dataframe with 1 additional variable per input variable. all original variables unchanged.
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
                   strings_to_split = ['forename_clean', 'middle_name_clean', 'surname_clean'],
                   separator = '[\s|-]+',
                   suffix = '_split')

Help on function postcode_pattern in adruk_tools.cleaning:

adruk_tools.cleaning.postcode_pattern = postcode_pattern(test_df, test_postcodes)
    WHAT IT IS: Function
    WHAT IT DOES: tests if the values in a given variable follow the format of UK postcodes. Excludes non-numeric and non-alphabetical characters except horizontal spaces.
    RETURNS: copy of the original dataframe, with a new variable called 'fitPattern', which is TRUE where a value fits the format, FALSE if it's not, and NULL if the test variable is NULL.
    TESTED TO RUN ON: Spark dataframes
    TESTED FOR PERFORMANCE ON: UK Postcode Directory
    FALSE NEGATIVES: catches all postcodes in UK Postcode Directory
    FALSE POSITIVES: tests if a letter or number should be in a certain place, but not if that PARTICULAR letter/number should be there. If postcodes never use a certain character in a certain place that uses other characters, it doens't catch it. rejects any format that isn't in the UK Postcode Directory. Designed to reject anything that isn't a number or letter, or not in the right place. But cannot test completely.
    FULL LIST OF ACCEPTED FORMATS: see below
    
    AUTHOR: Johannes Hechler
    DATE: 27/08/2019
    VERSION: 0.1
    
    
    :PARAMETERS:
      :test_df = name of dataframe that holds variable to test:
        `(datatype = dataframe name, no string)`, e.g. PDS
      :test_variable = name of variable to match against reference:
        `(datatype = string)`, e.g. 'postcode'
    
    
    :EXAMPLE:
    >>> postcode_pattern(testDF, 'postcode')

Help on function postcode_split in adruk_tools.cleaning:

adruk_tools.cleaning.postcode_split = postcode_split(dataset, variable, suffix, connection)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: checks if a string or parts of it follow postcode format and saves them in separate variables
    :RETURNS: spark dataframe with 4 new variables (postcode unit, postcode sector, postcode district, postcode area), 1 for each possible postcode part. Where a part doesn't follow a postcode pattern, the value is NULL/None. New variables are named after the source variable with an optional suffix. Original variable remains unchanged.
    
    :NOTES:
    * fails if input postcodes have no internal space (unless they are 7 characters long), or more than one. So the codes should first be cleaned.
    * the format check looks purely if a code has no characters/digits in the wrong places, as specified in the PAF Programmer's Guide. There is no check against any reference lookups like the National Statistics Postcode Lookup.
    * format requirements are summarised in PAF Programmer's Guide
    * Original method and SQL code developed by DWP in Oracle SQL. Adapted to Hive SQL and wrapped into a pyspark function in ONS.
    
    :TESTED TO RUN ON: spark dataframe
    
    :AUTHOR: Johannes Hechler
    :DATE: 17/19/2019
    :VERSION: 0.0.1
    
    
    :PARAMETERS:
    * dataset = spark dataframe
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variable = variable to check and break into components
        `(datatype = 1 string)`, e.g. 'postcode'
    * suffix = what suffix to add to the names of the new, component variables. To distinguish them if more than 1 variable is broken into components.
        `(datatype = 1 string)`, e.g. '_domicile'
    * connection = name of the spark cluster to use for SQL computations
        `(datatype = 1 object name, no string)`, e.g. spark
        
    :EXAMPLE:
    >>> postcode_split(dataset = PDS,
                        variable = 'postcode',
                        suffix = '',
                        connection = session)

Help on function rename_columns in adruk_tools.cleaning:

adruk_tools.cleaning.rename_columns = rename_columns(dataset, variable_names_old, variable_names_new)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: renames columns in a spark dataframe according to a user-defined lookup
    :RETURNS: spark dataframe with columns renamed. Columns not in the lookup are unchanged.
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
      * ever listed old name MUST have a new value assigned. If variable should NOT be renamed, assign None
    
        
    :EXAMPLE:
    >>> rename_columns(dataset = PDS,
                      variable_names_old = ['gender', 'fname'],
                      variable_names_new = ['sex', 'forename'])

Help on function sex_recode in adruk_tools.cleaning:

adruk_tools.cleaning.sex_recode = sex_recode(dataset, variable_input, variable_output)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: recodes different sets of values to 1, 2, 3 or None/NULL
    :RETURNS: spark dataframe with recode saved into new variable, or original variable overwritten
    :OUTPUT VARIABLE TYPE: int
    :NOTES:
    * recode values are hardcoded into the function like this:
    * [1, 'MALE', 'M'] becomes 1
    * [2, 'FEMALE', 'F'] becomes 2
    * [0, 3, 'I']  becomes 3
    * these values were chosen purely because they appeared in the test data used. If your data uses any other codes, the function fails.
    * the function expects strings to be in upper case: 'MALE' recodes to 1, but 'male' gets ignored and becomes NULL/None.
    
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

Help on function space_to_underscore in adruk_tools.cleaning:

adruk_tools.cleaning.space_to_underscore = space_to_underscore(df)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: replaces spaces with underscores
    :RETURNS: spark dataframe with no column names containing spaces
    TESTED TO RUN ON: spark dataframe  
    AUTHOR: Sophie-Louise Courtney
    DATE: 02/03/2021
    VERSION: 0.0.1
    
    
    :PARAMETERS:
    * df = spark dataframe

Help on function title_remove in adruk_tools.cleaning:

adruk_tools.cleaning.title_remove = title_remove(dataset, variables)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: removes preceeding/trailing white space from strings, then makes them upper-case, then removes selected titles at the very start
    :RETURNS: spark dataframe with selected variable cleaned, trimmed, made upper case. Other variables unchanged
    :OUTPUT VARIABLE TYPE: string
    :NOTES:
    * title list is hard-coded into the function.
    * It is based on what we found in census data
    * list of titles removed: DAME, DR, MR, MSTR, LADY, LORD, MISS, MRS, MS, SIR, REV, plus any amount of whitespace behind it
    * according to census data, we needn't for spelling out the acronyms (e.g. mister, professor)
    * no need for longer list (e.g. as in Wikipedia::English honorifics)
    * the function cleans the data before removing titles (trims whitespace, make everything upper case), but more cleaning should be done beforehand, e.g. remove dots
    
    
    TESTED TO RUN ON: spark dataframe
    RUN TIME: 20-row test dataframe - 3s; full PDS stock 2019 - 2s
    
    AUTHOR: Johannes Hechler
    DATE: 25/09/2019
    VERSION: 0.0.1
    
    
    :PARAMETERS:
    * data = spark dataframe:
        `(datatype = dataframe name, no string)`, e.g. PDS
    * variables = variables to remove titles from:
        `(datatype = list of strings)`, e.g. ['firstname', 'secondname']
        
    :EXAMPLE:
    >>> title_remove(dataset = PDS,
                    variables = ['first_given_name', 'family_name', 'other_given_names'])


