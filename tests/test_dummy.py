# import module that holds the dummy function
import adruk_tools.dummy as d


# actual test
def test_dummy():
    assert d.dummy_function() == 'success'