# Databricks notebook source
import os

# etl path ## improvement: ingest this var so as to make it dynamic
path = "etl"

# get path and change dir
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
root_path = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace{root_path}/{path}')

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# read in secret from KV
dbutils.secrets.list('secretscope')
test=dbutils.secrets.get(scope="secretscope", key="stsaskey")

# COMMAND ----------

# DBTITLE 1,read gold data
# reading from gold
storage_account_name = 'stroelofdev'
storage_account_access_key = test
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

layer = "gold"
blob_container = f'assignment-msf-{layer}'
name = f"project_locations_{layer}.txt"
readFilePath = "wasbs://" + blob_container + "@" + storage_account_name + f".blob.core.windows.net/{name}"
df = spark.read.format("delta").load(readFilePath, inferSchema = True, header = True)

# sanity check
df.show(24)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,visualize
# visualize data
import folium
from folium.plugins import MarkerCluster

# Convert Spark DataFrame to Pandas DataFrame
pdf = df.toPandas()

# Create a map centered around the average latitude and longitude
map_center = [pdf['latitude'].astype(float).mean(), pdf['longitude'].astype(float).mean()]
m = folium.Map(location=map_center, zoom_start=5)

# Add points to the map
marker_cluster = MarkerCluster().add_to(m)
for idx, row in pdf.iterrows():
    folium.Marker(location=[row['latitude'], row['longitude']], popup=row['city'], tooltip=row['country']).add_to(marker_cluster)

# Display the map
display(m)
