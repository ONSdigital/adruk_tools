# import module that holds the dummy function
import dummy as d


# actual test
def test_dummy():
    assert d.dummy_function() == 'success'