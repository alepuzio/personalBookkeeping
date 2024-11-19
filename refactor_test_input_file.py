import pytest

from refactor_read_data_csv import calculate_max_length_columns


# class TestInputData: @pytest.fixture(scope="class", autouse=True)
def test_calculate_max_length_columns():
    actual = calculate_max_length_columns(my_data())
    expected = 2
    assert actual == expected


# @pytest.fixture
def my_data():
    lines = []
    for i in range(3):
        line = "row {0}, row {1}\n".format(str(i), str(i + 1))
        lines.append(line)
    return lines
