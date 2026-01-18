# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_estudiante_transformed = spark.table(f"{catalogo}.{esquema_source}.estudiante_transformed")

# COMMAND ----------

df_transformed = df_estudiante_transformed.groupBy(col("edad")).agg(
                                                     count(col("edad")).alias("edad"),
                                                     max(col("edad")).alias("max_edad"),
                                                     min(col("edad")).alias("min_edad"),
                                                     col("edad")).agg(
                                                     count(col("sexo"=="M")).alias("sexo"),
                                                     count(col("sexo"="F")).alias("sexo"),
                                                     count(col("ciudad"="Bogota")).alias("ciudad"),
                                                     count(col("ciudad"="Medellin")).alias("ciudad"))
                                                     ).orderBy(col("race_year").desc())

# COMMAND ----------

df_transformed.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_raced_partitioned")