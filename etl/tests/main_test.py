import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# @pytest.fixture
def test_example():
    assert 1 == 1


# @pytest.fixture
def test_example2():
    assert 1 == 1
