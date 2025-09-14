# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw_schema.raw_volume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/raw_volume/raw_data/flights")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/raw_volume/raw_data/bookings")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/raw_volume/raw_data/passengers")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_schema/raw_volume/raw_data/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.silver;
# MAGIC CREATE SCHEMA workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME bronze.bronze_volume;
# MAGIC CREATE VOLUME silver.silver_volume;
# MAGIC CREATE VOLUME gold.gold_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/bookings/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/flights/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronze_volume/airports/data/`