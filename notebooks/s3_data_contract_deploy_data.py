# Databricks notebook source
# DBTITLE 1,Python Imports
import ast, time

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")

dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")

dbutils.widgets.text("target_schema", "default4")
target_schema = dbutils.widgets.get("target_schema")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="target_schema", value=target_schema) 

# Tables: Using a JSON widget to store multiple key-value pairs
dbutils.widgets.text(
    "source_tables", "{'customer': 'customer table description', \
    'customer1': 'customer1 table description', \
    'customer2': 'customer2 table description', \
    'my_managed_table': 'my_managed_table description', \
    'my_managed_table1': 'my_managed_table1 description', \
    'my_managed_table2': 'my_managed_table2 description'}"
)
source_tables = ast.literal_eval(dbutils.widgets.get("source_tables"))

# COMMAND ----------

# DBTITLE 1,Write Data to Catalog.Target_Schema.Tables
for table in source_tables:
    source_table_name = f"{source_catalog}.{source_schema}.{table}"
    target_table_name = f"{source_catalog}.{target_schema}.{table}"
    print(f"attempting to read table: {source_table_name}....")
    df = spark.read.table(source_table_name)
    if df.count() > 0: # then write data to target schema
        df.createOrReplaceTempView("temp_view")
        spark.sql(f"""INSERT INTO {target_table_name} SELECT * FROM temp_view""")
        print(f"inserted data into table: {target_table_name}....\n")
    else: print(f"no data to insert into table: {target_table_name}....\n")
