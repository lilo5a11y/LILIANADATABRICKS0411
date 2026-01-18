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
path_estudiante =  f"{path_base}estudiante.csv"



# COMMAND ----------

# MAGIC %md
# MAGIC structures
# MAGIC

# COMMAND ----------

estudiante_schema = StructType(fields=[ 
StructField("Tipo_de_identificacion_del_estudiante",StringType(), False),
StructField("Numero_de_Identificacion",StringType(), False),
StructField("Primer_apellido_del_estudiante",StringType(), False),
StructField("Segundo_apellido_del_estudiante",StringType(), False),
StructField("Primer_nombre_del_estudiante",StringType(), False),
StructField("Segundo_nombre_del_estudiante",StringType(), False),
StructField("Fecha_de_Nacimiento",StringType(), False),
StructField("Sexo",StringType(), False),
StructField("Edad",StringType(), False),
StructField("PESO",StringType(), False),
StructField("ciudad",StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC read source

# COMMAND ----------

df_estudiante =  spark.read.option('header', True)\
                        .schema(estudiante_schema)\
                        .csv(path_estudiante)

# COMMAND ----------

# MAGIC %md
# MAGIC save

# COMMAND ----------

df_estudiante.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.estudiante")