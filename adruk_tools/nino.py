"""
WHAT IT IS: pyspark functions library
WHAT IT DOES: defines regular expressions for different types of National Insurance numbers (NINOs)
AUTHOR: Johannes Hechler, Tom Heneghan
DATE: 23/08/2021
JIRA: ONSDS-2881
NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110
"""

#---------- REGEX DEFINITIONS ------------------ "
# Valid NINO. First two letters can't be D,F,I,Q,U or V. Second letter can't be O.
# NOTE: Unallocated NINOs are still permitted i.e. prefixes BG, GB, NK, KN,TN,NT and ZZ
# are seen as valid by this regex. These seven unallocated NINOs are not used/allocated
# in practice because of the risk of confusion or error. For more info refer to the NINO rule sources.
legal_nino = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{6}[a-dA-D]$'

# temporary NINO
temp_nino = '^TN|.*TEMP.*|^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{6}[fmpFMP]$'

# truncated NINO
legal_truncated_nino = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}$'

# valid  9 character ashe NINO,flags true if valid 9 character NINO format with 14 as characters 7 and 8
valid_ashe_9char_nino = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}14[a-dA-D]$'

# valid  8 character ashe NINO,flags true if valid 8 character NINO format with 14 as characters 7 and 8
# followed by a space or no character
valid_ashe_8char_nino = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}14$|^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}14 $'


# Northern Ireland NINO. starts with BT and is followed by 4 digits (or more)
ni_string = "^[bB][tT][0-9]{4}"


# Special Nino flag, Flags true if first two characters are in: CR, MW, NC, PY, PZ, MA, JY, GY and is followed by 4 digits (or more)
special_string = "(^CR|^MW|^NC|^PY|^PZ|^MA|^JY|^GY)[0-9]{4}"


# Unallocated prefix (not illegal but not allocated): first two characters are: BG, GB, NK, KN, TN, NT, ZZ and is followed by 4 digits (or more)
# O is not allowed as second letter
unallocated_prefix = "(^BG|^GB|^NK|^KN|^TN|^NT|^ZZ|^[a-zA-Z]O)[0-9]{4}"

#Invalid final character, valid NINO but final character is not A-D
invalid_final_char='^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{6}[e-zE-Z]$'

# Invalid suffix while also being an ashe nino: final letter of NINO is not A-D
ashe_invalid_final_char = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}14[e-zE-Z]$'


# Non-ashe NINO, NINO is valid but not part of ashe sample. Last two digits can't be 14, final letter can't be E-Z,
# if no final letter must be space or 8 characters long
non_ashe_nino = '^(?![dfiquvDFIQUV])[a-zA-Z](?![dfiquvDFIQUVoO])[a-zA-Z][0-9]{4}(?!14)[0-9]{2}((?![e-zE-Z])[a-zA-Z]| |$)'

# Name-not-NINO NINO contains only letters
# future upgrade: check if there is NO numerical digit instead. currently fails on whitespace, special characters
name_not_nino = '^[a-zA-Z]*$'




#---------- FUNCTION DEFINITIONS ------------------ "
def legal(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid, full-length National Insurance numbers
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :RULES:
  * First two letters can't be D,F,I,Q,U or V. Second letter can't be O.
  * NOTE: Unallocated NINOs are still permitted i.e. prefixes BG, GB, NK, KN,TN,NT and ZZ
  * are seen as valid by this regex. These seven unallocated NINOs are not used/allocated
  * in practice because of the risk of confusion or error. For more info refer to the NINO rule sources.
  
  :NOTES:
    * the regular expression used in this function is part of the package. run `legal_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> legal('NINO')
  >>> data.withColumn('valid_nino', legal('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(legal_nino)


def temporary(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid, temporary National Insurance numbers
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `temp_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> temporary('NINO')
  >>> data.withColumn('valid_nino', temporary('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(temp_nino)


def truncated(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid, truncated National Insurance numbers
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `legal_truncated_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> truncated('NINO')
  >>> data.withColumn('valid_nino', truncated('NINO'))
  """

  import pyspark.sql.functions as F
  return F.col(column).rlike(legal_truncated_nino)


def valid_ashe_9char(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are National Insurance numbers that are
  * valid
  * of length 9
  * characters 7 and 8 are '14'
  
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean


  :NOTES:
    * the regular expression used in this function is part of the package. run `valid_ashe_9char_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> valid_ashe_9char('NINO')
  >>> data.withColumn('valid_nino', valid_ashe_9char('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(valid_ashe_9char_nino)


def valid_ashe_8char(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are National Insurance numbers that are
  * valid
  * of length 8
  * characters 7 and 8 are '14'
  
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `valid_ashe_8char_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> valid_ashe_8char('NINO')
  >>> data.withColumn('valid_nino', valid_ashe_8char('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(valid_ashe_8char_nino)


def northern_ireland(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid National Insurance numbers from Northern Ireland
  * starts with 'BT' and is followed by 4 digits (or more)
  
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `ni_string` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> northern_ireland('NINO')
  >>> data.withColumn('valid_nino', northern_ireland('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(ni_string)


def special(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid National Insurance numbers with special prefixes
  * first two characters are in: CR, MW, NC, PY, PZ, MA, JY, GY and is followed by 4 digits (or more)
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `special_string` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> special('NINO')
  >>> data.withColumn('valid_nino', special('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(special_string)


def unallocated(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column are valid National Insurance numbers with special prefixes
  * first two characters are: BG, GB, NK, KN, TN, NT, ZZ and is followed by 4 digits (or more)
  * O is not allowed as second letter
  
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `unallocated_prefix` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> unallocated('NINO')
  >>> data.withColumn('valid_nino', unallocated('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(unallocated_prefix)


def invalid_end_character(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column end in characters that National Insurance numbers don't end in
  * final character is not A-D
  
  :RETURNS: 1 spark column object
  
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
  * the regular expression used in this function is part of the package. run `invalid_final_char` to print the rule.
  * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
  * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> invalid_end_character('NINO')
  >>> data.withColumn('valid_nino', invalid_end_character('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(invalid_final_char)



def ashe_invalid_end_character(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column should qualify for the Annual Survey of Hours and Earnings but end in characters that National Insurance numbers don't end in
  * positions 7 and 8 are '14'
  * but final character is not A-D

  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `ashe_invalid_final_char` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> ashe_invalid_end_character('NINO')
  >>> data.withColumn('valid_nino', ashe_invalid_end_character('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(ashe_invalid_final_char)



def non_ashe(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column don't qualify for the Annual Survey of Hours and Earnings
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `non_ashe_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> non_ashe('NINO')
  >>> data.withColumn('valid_nino', non_ashe('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(non_ashe_nino)



def name(column):
  """
  :WHAT IT IS: pyspark function
  :WHAT IT DOES: checks if values in a column only contain letters
  :RETURNS: 1 spark column object
  :OUTPUT VARIABLES TYPE: boolean

  :NOTES:
    * the regular expression used in this function is part of the package. run `name_not_nino` to print the rule.
    * NINO rules sourced from https://en.wikipedia.org/wiki/National_Insurance_number
    * https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

  :TESTED TO RUN ON: spark dataframe

  :AUTHOR: Johannes Hechler
  :DATE: 08/09/2021
  :VERSION: 0.0.1


  :PARAMETERS:
    : column = dataframe column to check:
      `(datatype = string)`, e.g. 'NINO'        

  :EXAMPLE:
  >>> name('NINO')
  >>> data.withColumn('valid_nino', name('NINO'))
  """
  import pyspark.sql.functions as F
  return F.col(column).rlike(name_not_nino)