# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

def edad_categoria(edad):
    if edad < 18:
        return "mayor edad"
    elif 18 <= edad < 30:
        return "joven"  
    elif 30 <= edad < 50:
        return "adulto"
    else:
        return "adulto mayor"


# COMMAND ----------

def sexo_categoria(sexo):
    if sexo == "M":
        return "Masculino"
    else:
        return "Femenino"

# COMMAND ----------

edad_categoria_udf = F.udf(edad_categoria, StringType())

# COMMAND ----------

sexo_categoria_udf = F.udf(sexo_categoria, StringType())

# COMMAND ----------

df_sexo = df_estudiante.select("Sexo").distinct()
df_sexo = df_sexo.withColumn("Sexo", sexo_categoria_udf("Sexo"))
df_sexo = df_sexo.drop("Sexo")
df_edad = df_estudiante.select("Edad").distinct()
df_edad = df_edad.withColumn("Edad", edad_categoria_udf("Edad"))
df_edad = df_edad.drop("Edad")
df_estudiante = df_estudiante.withColumn("Edad", edad_categoria_udf("Edad"))
df_estudiante = df_estudiante.withColumn("Sexo", sexo_categoria_udf("Sexo"))    

df_estudiante = df_estudiante.join(df_sexo, df_estudiante.Sexo == df_sexo.Sexo, "left")
df_estudiante = df_estudiante.join(df_edad, df_estudiante.Edad == df_edad.Edad, "left")
df_edad = df_estudiante.select("Edad").distinct()
df_edad = df_edad.withColumn("Edad", edad_categoria_udf("Edad"))
df_edad = df_edad.drop("Edad")
df_estudiante = df_estudiante.withColumn("Edad", edad_categoria_udf("Edad"))
df_estudiante = df_estudiante.withColumn("Sexo", sexo_categoria_udf("Sexo"))


# COMMAND ----------

df_estudiante = df_estudiante.withColumn("edad_category", edad_udf("edad_category"))

# COMMAND ----------

df_filtered_sorted = df_joined.filter(df_races.race_year > 1978).orderBy("race_id")

# COMMAND ----------

df_mayor_edad = df_mayor_edad.withColumn(
    "years_diferences", 
    F.year(F.current_date()) - F.col("edad")
)

# COMMAND ----------

df_updated.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.estudiante_transformed")