from digital_land_datasette.ducky import get_bool

def test_get_bool_returns_False():
    'false'
    result = get_bool('false')
    assert result is False