# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


dbutils.widgets.text("storageName", "adlsproyectola2026")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_smartdata")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC constants

# COMMAND ----------

storage_name = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
catalog =  dbutils.widgets.get("catalog")
schema =  dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC paths

# COMMAND ----------

path_base = f"abfss://{container}@{storage_name}.dfs.core.windows.net/"
path_ciudad =  f"{path_base}ciudad.csv"



# COMMAND ----------

# MAGIC %md
# MAGIC structures
# MAGIC

# COMMAND ----------

ciudad_schema = StructType(fields=[
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC read source

# COMMAND ----------

df_ciudad =  spark.read.option('header', True)\
                        .schema(ciudad_schema)\
                        .csv(path_ciudad)

# COMMAND ----------

# MAGIC %md
# MAGIC save

# COMMAND ----------

df_ciudad.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ciudad")