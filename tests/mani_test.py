from mani import manifest

def test_manifest():
    # read in test data
    read_path = '/ons/det_training/dummy_manifest.mani' # path to a dummy dataset in DevTest HDFS
    mani = manifest(read_path) # create a manifest object from dummy dataset

    # does the dataset reads in correctly?
    import pandas as pd  # import required package 'pandas'
    assert pd.DataFrame(list(mani.content['files']))['size'][1] == 10   # does the second file have the correct size?+
  
    # test the information about the overal delivery
    assert mani.total().files == {'name' : 'sgss.csv',
                                  'size' : 8} # is the 'files' variable a dictionary with 2 entries of specified content?

    # test the information about the individual datasets
    assert mani.parts('files').name == ['sgss.csv', 'ctas.csv']  # are both file names read in correctly from nested dictionaries?