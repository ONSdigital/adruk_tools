"""
WHAT IT IS: python script
WHAT IT DOES: Runs a set of unit tests on function cull_columns
AUTHOR: Silvia Bardoni
CREATED: 22/08/2022

"""
from importlib.machinery import SourceFileLoader

# import standard packages
import pandas as pd
from pandas.testing import assert_frame_equal

#this is to be able to import conftest
repo_path = '/home/cdsw/adruk_tools/tests'
import sys
# map local repo so we can import local libraries
sys.path.append(repo_path)

#to be able to reload the module if making changes to it
from importlib import reload
import conftest as ct

import os
import pydoop.hdfs as pdh

# import package to test function from
import adruk_tools.adr_functions as adr

# Import testing packages
import pytest
from unittest import mock




#---------------------LEFTOVER: parameters belong to test_cull_columns(), tidy this up ---------------
# Provide explicit file path to updated function, otherwise the old version in the package is referenced
adr = SourceFileLoader("adr_functions",
                       "/home/cdsw/adruk_tools/adruk_tools/adr_functions.py").load_module()


# Create a test dataset to be used throughout the tests. This is intially created in Spark
# so the function can run, before being converted to pandas to undertake assertation.

test_columns = ['name', 'id', 'age', 'bmi', 'year']

test_rows = [('Nathan', 'A', 23, 45.679, '2008'),
     ('Joanna', 'B', 63, 25.080, '2008'),
     ('Tom', 'A', 89, 99.056, '2008'),
     ('Nathan', 'C', 23, 45.679, '2008'),
     ('Nathan', 'A', 23, 45.679, '2008'),
     ('Johannes', 'E', 67, 25.679, '2009'),
     ('Nathan', 'B', 23, 45.679, '2009'),
     ('Johannes', 'E', 67, 45.679, '2009'),
     ('Nathan', None, 23, 45.679, '2009'),
     ('Nathan', 'C', 23, 45.679, None),
     (None, 'F', 89, 99.056, '2008')]

# paths in HUE, note that the culled dataframe needs to go into a different directory and will have the same name
write_path = '/dapsen/de_testing/test_dataframe.csv'
write_path_cull = '/dapsen/de_testing/cull/'

# Note that all functions start with test_ and call in the spark_context created
# in conftest.py. Required for the tests to be run.


def test_cull_columns(spark_context):
  """
  :WHAT IT IS: Python function
  :WHAT IT DOES: tests the function cull_columns in adr_functions
  Uses the the method get_sorted_data_frame to sort the PySpark DFs and
  converts PySpark DFs to pandas DFs to then use assert_frame_equal from
  pandas.testing.
  """

  input_dataset = spark_context.createDataFrame(test_rows, test_columns)

  # save into HUE
  input_dataset.coalesce(1).write.csv(
    f'{write_path}',
    sep=",",  # set the seperator
    header="true",  # Set a header
    mode="overwrite",
    )

  # cull_columns
  adr.cull_columns(cluster=spark_context,
                   old_files=[f'{write_path}'],
                   reference_columns=['NAME', 'YEAR'],
                   directory_out=f'{write_path_cull}')

  # read the culled csv
  expected_output = spark_context.read.csv(f'{write_path_cull}test_dataframe.csv', header=True)
  # sort and to Pandas
  expected_output = ct.get_sorted_data_frame(expected_output.toPandas(), ['NAME', 'YEAR'])

  real_output = pd.DataFrame([
    ['Nathan','2008'],
    ['Joanna', '2008'],
    ['Tom', '2008'],
    ['Nathan', '2008'],
    ['Nathan', '2008'],
    ['Johannes', '2009'],
    ['Nathan', '2009'],
    ['Johannes', '2009'],
    ['Nathan', '2009'],
    ['Nathan', None],
    [None, '2008']], columns=['NAME', 'YEAR'])

  # sort and to Pandas
  real_output = ct.get_sorted_data_frame(real_output, ['NAME', 'YEAR'])

  # Test equality between expected and generated outcomes

  print("columns culled")
  assert_frame_equal(expected_output, real_output, check_like=True)

  # delete dataframes from HDFS
  os.system(f'hdfs dfs -rm -r {write_path}')
  os.system(f'hdfs dfs -rm -r {write_path_cull}')


def test_hdfs_to_pandas():
    """Tests for read_csv_from_hdfs function."""

    # Use a patch to mock the result of spark.read
    # Note the order of the parameters here: self, mock_read, spark
    # The fixtures are listed at the end, and any mocks before this
    # Note that multiple mock decorators work in reverse order - the one at the top
    #   is the last listed in the function and vice versa

    @mock.patch("pyspark.sql.SparkSession.read")
    def test_hdfs_to_pandas(self, mock_read, spark):
        """Test the expected functionality."""

        # Arrange and Act
        sdf = adr.hdfs_to_pandas(spark, "filepath")

        # Assert
        mock_read.csv.assert_called_with("filepath",
                                         header=True,
                                         inferSchema=True)



def test_pandas_to_hdfs():
  """
  test for function pandas_to_hdfs
  writes a dummy dataset to HDFS,
  then checks if it arrived,
  then deletes it
  """
  #-----------
  # parameters
  #-----------

  # unclear if this directory has read/write access for everyone
  write_path = '/dapsen/de_testing/deleteme.csv'

  # make pandas df; contents are irrelevant
  dataframe = pd.DataFrame({'col1':[1,2,3],
                           'col2' : ['dummy file made for unit test',
                                     'delete it on sight',
                                     'no really, delete it.']})

  # write dataframe to HDFS
  adr.pandas_to_hdfs(dataframe = dataframe,
                      write_path = f'{write_path}' )

  # TEST: check it has been written
  assert pdh.path.exists(write_path)

  # delete dataframe from HDFS
  os.system(f'hdfs dfs -rm -r {write_path}')




def test_pydoop_read():
  """
  test for function pandas_to_hdfs
  writes a dummy dataset to HDFS,
  then checks if it arrived,
  then deletes it
  """
  # -----------
  # parameters
  # -----------

  # unclear if this directory has read/write access for everyone
  # LEFTOVER: use mocking instead
  write_path = '/dapsen/de_testing/deleteme.csv'

  # make pandas df; contents are irrelevant
  dataframe = pd.DataFrame({'col1': [1, 2, 3],
                            'col2': ['dummy file made for unit test',
                                     'delete it on sight',
                                     'no really, delete it.']})

  # write dataframe to HDFS
  adr.pandas_to_hdfs(dataframe=dataframe,
                      write_path=f'{write_path}')

  df = adr.pydoop_read(write_path)

  # TEST: check if the data is in memory
  assert df is not None

  # delete dataframe from HDFS
  os.system(f'hdfs dfs -rm -r {write_path}')



def test_column_recode(spark_context):

    """
    :WHAT IT IS: Python function
    :WHAT IT DOES: tests column_recode function in adr_functions.py
    """
    # create dataframe
    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    expected_output = adr.column_recode(
        input_dataset,
        'name', {'Nathan': 'Nat', 'Tom': 'Tomas', 'Joanna': 'Jo'}, 'Other')

    expected_output = expected_output.select(['name','year'])

    expected_output = ct.get_sorted_data_frame(expected_output.toPandas(),
                                               ['name', 'year'])

    real_output = pd.DataFrame([
                                ['Jo', '2008'],
                                ['Nat', '2008'],
                                ['Nat', '2008'],
                                ['Nat', '2008'],
                                ['Nat', '2009'],
                                ['Nat', '2009'],
                                ['Nat', None],
                                ['Other', '2008'],
                                ['Other', '2009'],
                                ['Other', '2009'],
                                ['Tomas', '2008']
                                 ],
                               columns=['name', 'year'])

    real_output = ct.get_sorted_data_frame(real_output, ['name', 'year'])

    # Test equality between expected and generated outcomes
    pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


def test_wrong_type(spark_context):
    """
    Test that the type of the recoded column is string
    """

    # Create dataframe with column type not a string
    input_dataset = spark_context.createDataFrame(test_rows, test_columns)

    try:
      with pytest.raises(TypeError) as context:
          expected_output_df = adr.column_recode(
              input_dataset,
              'age', {'Nathan': 'Nat', 'Tomas': 'Tom', 'Joanna': 2}, 'Other')
    #return expected_output_df
    except:
      assert isinstance(context.value, TypeError)
      assert str(context.value) == 'Column must be a string'


""" to updated once  correction in function """
class TestUpdateFileWithTemplate(object):
  """Unit tests for the function update_file_later. It tests xxxx"""
  # Start tests
  # -----------


  # 1 Function does what expected
  def test_update_file_standard(self):
    """Test the expected functionality."""
    input_dataset = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'James', 'Silvia', 'Johannes'],
    "Age": [1, 2, 2, 3, -2],
    "Sex": ['M', 'M', 'F', 'M', 'F'],
    "DOB": [True, False, None, True, False]})

    template = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
    "Occ": ['test1', 'test2', 'test3', 'test4', 'test5'],
    "Alive": [15, 0, -6, -5, 10],
    "Surname": ['a', 'b', None, 'c', 'd']})

    expected_output = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
    "Age": [1, 2, None , 2, 3],
    "DOB": [True, False, None , None, True],
    "Sex": ['M', 'M', None, 'F', 'M'],})

    # Arrange and Act
    input_dataset.to_csv('/home/cdsw/adruk_tools/test_data.csv', index=False)

    adr.update_file_with_template(file_path = '/home/cdsw/adruk_tools/test_data.csv',
                                  template = template,
                                  join_variable = 'Name')

    real_output = pd.read_csv('/home/cdsw/adruk_tools/test_data.csv')

    # Assert equality
    assert_frame_equal(expected_output, 
                       real_output, 
                       check_like=True)




  # 2 Check failure if input dataset isnt csv
  # ---------
  def test_update_not_csv(self):
    template = pd.DataFrame({"Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia']})
    with pytest.raises(ValueError, 
                       match = 'Function can only be used with CSV file.'):
      adr.update_file_with_template(file_path = '/home/cdsw/adruk_tools/nino.py',
                                    template = template,
                                    join_variable = 'Name')


  # 3 Check that if the file does not exist, must write template
  # ---------
  def test_update_file_does_not_exist(self,  spark_context):
    template = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
    "Occ": ['test1', 'test2', 'test3', 'test4', 'test5'],
    "Alive": [15, 0, -6, -5, 10],
    "Surname": ['a', 'b', None, 'c', 'd']})

    #expected output same as template
    expected_output = pd.DataFrame({
    "Name": ['Nathan', 'Jo', 'Jon', 'James', 'Silvia'],
    "Occ": ['test1', 'test2', 'test3', 'test4', 'test5'],
    "Alive": [15, 0, -6, -5, 10],
    "Surname": ['a', 'b', None, 'c', 'd']})

    # Arrange and Act
    adr.update_file_with_template(file_path = '/home/cdsw/adruk_tools/test_data.csv',
                                  template = template,
                                  join_variable = 'Name')

    real_output = pd.read_csv('/home/cdsw/adruk_tools/test_data_template.csv')

    # Assert equality
    assert_frame_equal(expected_output, 
                       real_output, 
                       check_like=True)
