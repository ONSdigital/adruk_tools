# Import testing packages
import pytest
from unittest import mock


# import package to test function from
import adruk_tools.adr_functions as adr

def test_pandas_to_hdfs():
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

