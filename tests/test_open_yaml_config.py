"""
LANGUAGE: python
WHAT IT DOES: Runs a set of unit tests on the function open_taml_config
AUTHOR: Veronica Ferreiros Lopez
DATE: 24/02/2023
"""

import adruk_tools.adr_functions as d
import pytest


class TestOpenYamlConfig(object):
  
  """Unit tests for the function open_yaml_config. 
  It tests errors, type and values"""
  
 
  yaml_file_path = "/home/cdsw/adruk_tools/tests/test_config.yaml"
  yaml_non_existing_file_path = "/home/cdsw/adruk_tools/tests/non_existing_file.yaml"


  ## TESTS
  #---------  
    
  
  #1. Does the function raise an error when the input file does not exist?  
  def test_open_yaml_config_error(self):     
    with pytest.raises(OSError):
      d.open_yaml_config(self.yaml_non_existing_file_path) 
  
  
  
  #2. Does the function return a dictionary? 
  def test_return_open_yaml_config(self):
    assert isinstance(d.open_yaml_config(self.yaml_file_path),dict)
  
  
  
  #3. Is the dictionary empty (=empty yaml file)?  
  def test_dictionary_open_yaml_config(self):
    assert len(d.open_yaml_config(self.yaml_file_path)) != 0
  
  

  #4. Test all the elements the dictionary and check that returns the correct values.
  # Please, go to the example yaml file (test_config.yaml) to check variables and
  # values.
  def test_values_open_yaml_config(self):
    assert (d.open_yaml_config(self.yaml_file_path)["settings"]["file"]== "test/file/test.csv")
    assert (d.open_yaml_config(self.yaml_file_path)["settings"]["columns"]== ["column_1","column_2","column_3"])
    assert (d.open_yaml_config(self.yaml_file_path)["settings"]["test_data"]== True)
    assert (d.open_yaml_config(self.yaml_file_path)["settings"]["number_of_columns"]== 3)
  
  
  
 
