"""This module contains functions to enrich data with geographical
information."""

from geopy.geocoders import Nominatim
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import FloatType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

# Initialize geolocator
geolocator = Nominatim(user_agent="testing-roelof-netherlands")


def get_country(lat: float, lon: float) -> str:
    """Uses geopy to get the country of a given latitude and longitude."""
    try:
        location = geolocator.reverse((lat, lon), language="en")
        return location.raw["address"].get("country", "")
    except:
        return "Unknown"


def get_city(lat: float, lon: float) -> str:
    """Uses geopy to get the city of a given latitude and longitude."""
    try:
        location = geolocator.reverse((lat, lon), language="en")
        return location.raw["address"].get("city", "")
    except:
        return ""


def get_nearest_city(lat: float, lon: float, radius=10000) -> str:
    """Uses geopy to get the nearest city of a given latitude and longitude."""
    try:
        # Perform reverse geocoding with radius to expand the search area
        location = geolocator.reverse(
            (lat, lon), exactly_one=True, radius=radius, language="en"
        )
        address = location.raw["address"]

        # Attempt to retrieve the city name
        city = address.get("city")

        # Fallback to nearby regions if city is not available
        if not city:
            city = (
                address.get("town") or address.get("village") or address.get("hamlet")
            )

        return city if city else "Unknown"

    except Exception as e:
        return "Unknown"
