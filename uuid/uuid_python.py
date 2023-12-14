import pandas as pd
from uuid import uuid4


def make_python_uuids(number_values_required):
  
  """
  Creates UUIDs in python.
  
  language
  --------
  Python
  
  returns
  -------
  uuid values
  
  return type
  -----------
  list
  
  author
  ------
  Alex Anthony
  
  date
  ------
  14/12/2023
  
  version
  -------
  0.0.1
  
  parameters
  ----------
  number_values_required = state the number of uuids you wish to generate
  `(datatype = integer)`, e.g. 10

  example
  -------
  >>> make_python_uuids(number_values_required = 10)
  """

  uuids = [uuid4() for x in range(number_values_required)]
  return(uuids)

make_python_uuids(number_values_required = 10)