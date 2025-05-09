# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Data Notebook
# MAGIC
# MAGIC This notebook is used to copy data from the source catalog and schema provided via the parameters to a target catalog and schema defined in the data contract .yaml. Note that this is provided as a means of testing the validity of the DDL deployed in step 3 and testing data quality rules. It is not expected to be used in the production lifecycle of a normal data contract.

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Workflow Widget Parameters
# MAGIC
# MAGIC This step defines widgets and initializes variables.

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Widget Parameters
dbutils.widgets.text("user_email", dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())
user_email = dbutils.widgets.get("user_email")
print(f"user_email: {user_email}")


dbutils.widgets.text("author_folder_path", f"/Workspace/Users/{user_email}/odcs_data_contracts/notebooks/input_data")  # should be a Workspace Users folder
author_folder_path = dbutils.widgets.get("author_folder_path")
print(f"author_folder_path: {author_folder_path}")


dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


# yaml_file_path syntax --> "{source_catalog}__{source_schema}.yaml"
dbutils.widgets.text("yaml_file_path", f"{author_folder_path.split('/input_data')[0]}/data_contracts_data/catalog={source_catalog}/{source_catalog}__{source_schema}.yaml")
yaml_file_path = dbutils.widgets.get("yaml_file_path")
print(f"yaml_file_path: {yaml_file_path}")


# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook
source_tables, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Read in the Data Contract Yaml
# MAGIC
# MAGIC This step defines reads in the existing data contract yaml as a dictionary.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read in the Data Contact Yaml
with open(yaml_file_path, 'r') as f:
    data_contract_odcs_yaml = yaml.safe_load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Read Target Catalog and Target Schema From Data Contract
# MAGIC
# MAGIC This step reads in the target catalog and schema from the ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Read Target Catalog and Target Schema From Data Contract
# Get the data contract catalog and schema
target_catalog = data_contract_odcs_yaml["servers"][0]["catalog"] # This represents target catalog
target_schema = data_contract_odcs_yaml["servers"][0]["schema"] # This represents target schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Loop through Source Tables and Insert New Records into Target
# MAGIC
# MAGIC The cell below uses a Secure Hash Algorithm (SHA) across all common columns between the source and target tables to define a join key and only inserts new records into the target table.

# COMMAND ----------

# DBTITLE 1,Write Data to Catalog.Target_Schema.Tables
for table in source_tables:

    if table == "temp_view" or source_schema == target_schema:
        print(f"⚠️ Skipping table '{table}': unable to process 'temp_view' or source_schema '{source_schema}' and target_schema '{target_schema}' are the same")
        continue
    
    source_table = f"{source_catalog}.{source_schema}.{table}"
    target_table = f"{target_catalog}.{target_schema}.{table}"
    
    try:
        spark.sql(f"DESCRIBE TABLE {target_table}")
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        print(f"⚠️ Skipping table '{table}': target table {target_table} does not exist\n")
        continue
    print(f"🔹 Reading source table: {source_table}")
    
    source_df = spark.read.table(source_table).limit(100)
    source_df_schema = source_df.schema
    print(f"🔹 Reading target table: {target_table}")
    
    target_df = spark.read.table(target_table)
    target_df_schema = target_df.schema
    if source_df.count() == 0:
        print(f"⚠️ No data in source table: {source_table}")
        continue
    
    # Get target schema as a dictionary {column_name: column_type}
    target_schema_dict = {field.name: field.dataType for field in target_df_schema.fields}

    # Ensure both tables have the same columns
    common_columns = [c for c in source_df.columns if c in target_schema_dict]
    if not common_columns:
        print(f"⚠️ No common columns between source and target for table: {table}")
        continue

    # Get the orderable columns for sorting both source and target dataframes the same
    orderable_columns = [c for c in common_columns if not isinstance(target_schema_dict[c], (VariantType, ArrayType, MapType, StructType))]

    # Add row level sha hashes to source Spark dataframe
    source_df = source_df.select([F.col(c) for c in common_columns]).orderBy(*orderable_columns)
    row_hash_source = F.sha2(F.to_json(F.struct(*[F.col(c) for c in common_columns])), 256)
    source_df = source_df.withColumn("row_hash", row_hash_source)
    
    # Add row level sha hashes to target Spark dataframe
    target_df = target_df.select([F.col(c) for c in common_columns]).orderBy(*orderable_columns)
    row_hash_target = F.sha2(F.to_json(F.struct(*[F.col(c) for c in common_columns])), 256)
    target_df = target_df.withColumn("row_hash", row_hash_target)


    # Perform the left outer join to detect new records
    source_df = source_df.alias("src")
    target_df = target_df.alias("tgt")
    
    # Null-safe join condition using <=> (eqNullSafe)
    join_condition = [F.col(f"src.row_hash").eqNullSafe(F.col(f"tgt.row_hash"))]
    print(f"🔁 Join condition: {join_condition}")
    
    # Perform left outer join to find new records
    joined_df = source_df.join(target_df, on=join_condition, how="left_outer")
    
    # Filter for new records not in the target
    new_records_df = joined_df.filter(F.col(f"tgt.row_hash").isNull())
    if new_records_df.count() > 0:
        print(f"✅ Found {new_records_df.count()} new records to insert into: {target_table}")
        
        # Select only source columns to insert
        new_data = new_records_df.select([F.col(f"src.{c}") for c in common_columns if "row_hash" not in c])
        
        # Create temp view and insert into target
        new_data.createOrReplaceTempView("temp_view")
        spark.sql(f"INSERT INTO {target_table} SELECT * FROM temp_view")
        
        spark.sql("DROP VIEW IF EXISTS temp_view")
        print(f"✅ Successfully inserted new records into {target_table}\n")
        
        # Clean up to avoid memory issues
        del source_df
        del target_df
    else:
        print(f"ℹ️ No new records to insert into {target_table}\n")

