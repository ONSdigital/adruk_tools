class manifest:
  """
  WHAT IT IS: Python class
  WHAT IT DOES: 
  * creates an object of class 'manifest'
  * assign several methods to the object
  * designed extract data from nested dictionaries, in particular .mani files on HDFS
  AUTHOR: Johannes Hechler
  DATE: 09/02/2021
  VERSION: 0.1
  """
  # give the object 
  def __init__(self, path):
    """
    WHAT IT IS: Python method for objects of class 'manifest'
    WHAT IT DOES: 
    * generates base property for object, i.e. reads in the specified file from HDFS into a pandas dataframe
    AUTHOR: Johannes Hechler
    DATE: 09/02/2021
    VERSION: 0.1
    """
    import pydoop.hdfs as hdfs
    import pandas as pd
    
    with hdfs.open(path, "r") as f:
      self.content = pd.read_json(f)
      f.close()
        
  def whole(self): 
    """
    WHAT IT IS: Python method for objects of class 'manifest'
    WHAT IT DOES: 
    * generates property 'whole', i.e. information about the overall delivery, as a pandas dataframe with 1 row
    AUTHOR: Johannes Hechler
    DATE: 09/02/2021
    VERSION: 0.1
    """
    return self.content.iloc[0]
      
  def parts(self, variable):
    """
    WHAT IT IS: Python method for objects of class 'manifest'
    WHAT IT DOES: 
    * generates property 'parts', i.e. information about the individual files included in a delivery, as a pandas dataframe with as many rows as there are files
    AUTHOR: Johannes Hechler
    DATE: 09/02/2021
    VERSION: 0.1
    """
    import pandas as pd
    return pd.DataFrame(list( self.content[ variable ]))