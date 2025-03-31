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
        print(f":warning: Skipping table '{table}': target table {target_table} does not exist\n")
        continue
    print(f":small_blue_diamond: Reading source table: {source_table}")
    
    source_df = spark.read.table(source_table).limit(100)
    print(f":small_blue_diamond: Reading target table: {target_table}")
    
    target_df = spark.read.table(target_table)
    if source_df.count() == 0:
        print(f":warning: No data in source table: {source_table}")
        continue
    
    # Get target schema as a dictionary {column_name: column_type}
    target_schema_dict = {field.name: field.dataType for field in target_df.schema.fields}
    
    # Ensure both tables have the same columns
    common_columns = [c for c in source_df.columns if c in target_schema_dict]
    if not common_columns:
        print(f":warning: No common columns between source and target for table: {table}")
        continue
    
    # Cast and select only common columns
    source_df_casted = source_df.select([F.col(c).cast(target_schema_dict[c]) for c in common_columns])
    
    # Perform the left outer join to detect new records
    source_df_casted = source_df_casted.alias("src")
    target_df = target_df.alias("tgt")
    
    # Null-safe join condition using <=> (eqNullSafe)
    join_condition = [F.col(f"src.{c}").eqNullSafe(F.col(f"tgt.{c}")) for c in common_columns]
    print(f"Join condition: {join_condition}")
    
    # Perform left outer join to find new records
    joined_df = source_df_casted.join(target_df, on=join_condition, how="left_outer")
    
    # Filter for new records not in the target
    new_records_df = joined_df.filter(F.col(f"tgt.{common_columns[0]}").isNull())
    if new_records_df.count() > 0:
        print(f":white_check_mark: Found {new_records_df.count()} new records to insert into: {target_table}")
        
        # Select only source columns to insert
        new_data = new_records_df.select([F.col(f"src.{c}") for c in common_columns])
        
        # Create temp view and insert into target
        new_data.createOrReplaceTempView("temp_view")
        spark.sql(f"INSERT INTO {target_table} SELECT * FROM temp_view")
        
        spark.catalog.dropTempView("temp_view")
        print(f":white_check_mark: Successfully inserted new records into {target_table}\n")
        
        # Clean up to avoid memory issues
        del source_df
        del target_df
    else:
        print(f":information_source: No new records to insert into {target_table}\n")
