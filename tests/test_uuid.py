# Provide explicit file path to updated function, otherwise the old version in the
# package is referenced. At least was for me
#this is to be able to import conftest
repo_path = '/home/cdsw/adruk_tools/tests'
import sys
# map local repo so we can import local libraries
sys.path.append(repo_path)

import conftest as ct

import pyspark.sql as ps
import adruk_tools.uuid as u
import pytest

class TestUuidPython(object):

  """
  Unit tests for the function make_python_uuids()
  
  author
  ------
  johannes hechler
  
  date
  ----
  05/01/2024
  """

  ## SETUP
  #---------

  # how many UUIDs do we expect?
  n_uuids = 10
  
  # run the function and produce output to test 
  test_data = u.make_python_uuids(n_uuids)

  ## TESTS
  #---------

  #1. does the function return a list?
  def test_returns_list(self):
    assert isinstance(self.test_data, list)

  #2. is the list populated?
  def test_list_populated(self):
    assert len(self.test_data) == self.n_uuids

  #3. are all elements unique?
  def test_all_unique(self):
    assert len(set(self.test_data)) == self.n_uuids

  #4. are no elements NULL?
  def test_no_null(self):
    assert sum([element for element in self.test_data if element is None]) == 0
    

def test_uuid_pyspark(test_data):
  """tests function make_pyspark_uuid()"""
  # run the function and produce output to test 
  actual = test_data.withColumn('uuid', 
                                u.make_pyspark_uuids())
  
  assert actual.select('uuid').dropDuplicates().count() == test_data.count()
