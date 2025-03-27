# Databricks notebook source
# DBTITLE 1,Python Imports
import ast, time

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers"

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")

dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")

dbutils.widgets.text("target_schema", "default4")
target_schema = dbutils.widgets.get("target_schema")


# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook
source_tables, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

# COMMAND ----------

print(source_tables)

# COMMAND ----------

# DBTITLE 1,Write Data to Catalog.Target_Schema.Tables
for table in source_tables:
    if table == "temp_view":
        continue  # Skip the temp view name if it's accidentally in the list

    source_table_name = f"{source_catalog}.{source_schema}.{table}"
    target_table_name = f"{source_catalog}.{target_schema}.{table}"
    print(f"attempting to read table: {source_table_name}....")
    
    df = spark.read.table(source_table_name)
    if df.count() > 0:
        df.createOrReplaceTempView("temp_view")
        spark.sql(f"""INSERT INTO {target_table_name} SELECT * FROM temp_view""")
        print(f"inserted data into table: {target_table_name}....\n")
    else:
        print(f"no data to insert into table: {target_table_name}....\n")
