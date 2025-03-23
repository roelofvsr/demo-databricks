# Databricks notebook source
# DBTITLE 1,Setup: install Python Packages.
# MAGIC %sh
# MAGIC pip install -r requirements.txt
# MAGIC export PYTHONDONTWRITEBYTECODE=1

# COMMAND ----------

# DBTITLE 1,Restart the kernel after package installation
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,change path to root
# imports
import os
import pytest
import sys
from utils import *
from geopy.geocoders import Nominatim
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# etl path ## improvement: ingest this var so as to make it dynamic
path = "etl"

# # get path and change dir - only required for manual testing - bundle start from root 'ETL'
# notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# root_path = os.path.dirname(os.path.dirname(notebook_path))
# os.chdir(f'/Workspace{root_path}/{path}')

# COMMAND ----------

# DBTITLE 1,Test
# add color tp pytest
os.environ['PYTEST_ADDOPTS'] = "--color=yes"

# don't write to disk, less clutter
sys.dont_write_bytecode = True

# run tests
pytest.main(["./tests"])

# COMMAND ----------

# DBTITLE 1,Set config
# read in secret from KV
dbutils.secrets.list('genericsecretscope')
key=dbutils.secrets.get(scope="genericsecretscope", key="stsaskey")

# Define global parameters
url = spark.conf.get("spark.databricks.workspaceUrl")
storage_account_name = f'stroelofdev'
if url.startswith("adb-1460959937671195"):
    storage_account_name = f'stroelofprd'

storage_account_access_key = key

# Set spark config
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,Read from silver
# reading from silver
layer = "silver"
blob_container = f'assignment-msf-{layer}'
file_name = f"project_locations_{layer}.txt"

# Read silver data
df = read_from_blob_wasbs(
    storage_account_name, 
    key, 
    blob_container, 
    file_name,
    'delta')

# sanity check
df.show(5)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Gold
# Initialize geolocator
geolocator = Nominatim(user_agent="testing-roelof-netherlands")

# convert
pdf = df.toPandas()

# Apply the functions to get country and city
pdf['country'] = pdf.apply(lambda row: get_country(row['latitude'], row['longitude']), axis=1)
pdf['city'] = pdf.apply(lambda row: get_city(row['latitude'], row['longitude']), axis=1)

# Convert back to Spark DataFrame
df_enriched = spark.createDataFrame(pdf)

# sanity check
display(df)

# write to gold 
layer = "gold"
blob_container = f'assignment-msf-{layer}'
name = f"project_locations_{layer}.txt"

save_dataframe_to_blob_wasbs(
    blob_container,
    file_name,
    df_enriched,
    layer,
    storage_account_name, 
    'delta')


# COMMAND ----------


