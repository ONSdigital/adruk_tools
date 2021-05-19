import dummy as d


#@pytest.mark.skip
def test_dummy():
    assert d.dummy_function() == 'success'