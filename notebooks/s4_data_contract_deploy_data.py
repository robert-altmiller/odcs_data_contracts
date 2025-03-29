# Databricks notebook source
# DBTITLE 1,Python Imports
import ast, time
import pyspark.sql.functions as F

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
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


dbutils.widgets.text("target_schema", "default_prod")
target_schema = dbutils.widgets.get("target_schema")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="target_schema", value=target_schema) 
print(f"target_schema: {target_schema}")


# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook
source_tables, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

# COMMAND ----------

# DBTITLE 1,Write Data to Catalog.Target_Schema.Tables
for table in source_tables:
    if table == "temp_view" or source_schema == target_schema:
        print(f"Skipping table '{table}': unable to process 'temp_view' or source_schema '{source_schema}' and target_schema '{target_schema}' are the same")
        continue

    source_table = f"{source_catalog}.{source_schema}.{table}"
    target_table = f"{source_catalog}.{target_schema}.{table}"

    if not spark.catalog.tableExists(target_table):
        print(f"⚠️ Skipping table '{table}': target table {target_table} does not exist.")
        continue

    print(f"🔹 Reading source table: {source_table}")
    source_df = spark.read.table(source_table)

    print(f"🔹 Reading target table: {target_table}")
    target_df = spark.read.table(target_table)

    if source_df.count() == 0:
        print(f"⚠️ No data in source table: {source_table}")
        continue

    # Ensure both tables have the same columns
    common_columns = [col for col in source_df.columns if col in target_df.columns]

    if not common_columns:
        print(f"⚠️ No common columns between source and target for table: {table}")
        continue

    # Alias DataFrames for clarity
    source_df = source_df.alias("src")
    target_df = target_df.alias("tgt")

    # Build the join condition across all common columns
    join_condition = [F.col(f"src.{c}") == F.col(f"tgt.{c}") for c in common_columns]
    print(f"Join condition: {join_condition}")

    # Perform the left outer join
    joined_df = source_df.join(target_df, on=join_condition, how="left_outer")

    # Filter for records that do not exist in the target (i.e., new)
    new_records_df = joined_df.filter(F.col(f"tgt.{common_columns[0]}").isNull())

    if new_records_df.count() > 0:
        print(f"✅ Found {new_records_df.count()} new records to insert into: {target_table}")

        # Select only source columns to insert
        new_data = new_records_df.select([F.col(f"src.{c}") for c in source_df.columns])
        new_data.createOrReplaceTempView("temp_view")

        spark.sql(f"INSERT INTO {target_table} SELECT * FROM temp_view")
        spark.catalog.dropTempView("temp_view")

        print(f"✅ Successfully inserted new records into {target_table}\n")
    else:
        print(f"ℹ️ No new records to insert into {target_table}\n")