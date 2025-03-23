import pytest
from pyspark.sql import SparkSession
from utils.enrich import get_city, get_country

spark = SparkSession.builder.getOrCreate()


# @pytest.fixture
def test_main():
    assert 1 == 1


# @pytest.fixture
def test_random():
    assert 2 == 2


def test_get_country():
    # Test with valid coordinates
    result = get_country(52.3676, 4.9041)
    assert result == "Netherlands"

    # Test when no country is found (mock a failed lookup)
    result = get_country(0, 0)
    assert result == "Unknown"


def test_get_city():
    # Test with valid coordinates
    result = get_city(52.3676, 4.9041)
    assert result == "Amsterdam"

    # Test with invalid coordinates
    result = get_city(0, 0)
    assert result == ""
