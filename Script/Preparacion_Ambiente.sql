-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "adlsproyectola2026";

-- COMMAND ----------

DROP CATALOG IF EXISTS catalog_smartdata CASCADE;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_smartdata;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_smartdata.raw;
CREATE SCHEMA IF NOT EXISTS catalog_smartdata.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_smartdata.silver;
CREATE SCHEMA IF NOT EXISTS catalog_smartdata.golden;
CREATE SCHEMA IF NOT EXISTS catalog_smartdata.exploratory;

CREATE VOLUME IF NOT EXISTS catalog_smartdata.raw.datasets;

-- COMMAND ----------

DESCRIBE VOLUME catalog_smartdata.raw.datasets

-- COMMAND ----------

show volumes in catalog_smartdata.raw;

-- COMMAND ----------

