Help on function missing_by_row in adruk_tools.diagnostics:

adruk_tools.diagnostics.missing_by_row = missing_by_row(dataset, *args)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: counts how many rows have got different numbers of selected variables missing
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

Help on function missing_count in adruk_tools.diagnostics:

adruk_tools.diagnostics.missing_count = missing_count(*args)
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

Help on function unique_function in adruk_tools.diagnostics:

adruk_tools.diagnostics.unique_function = unique_function(*args)
    :WHAT IT IS: FUNCTION
    
    :WHAT IT DOES: Return unique values per column for any number of input matrices. Indicates suitability of each column for use as an identifier key in linkage.
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