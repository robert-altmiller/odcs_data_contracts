# Databricks notebook source
# MAGIC %md
# MAGIC # Create Tables Notebook
# MAGIC
# MAGIC This notebook is used to generate tables in a target catalog and schema for a given data contract.
# MAGIC
# MAGIC Given catalog, schema, and the file path to the data contract folder, this notebook will perform the following steps:
# MAGIC 1. Derive the full path to the relevant data contract.
# MAGIC 2. Read in the .yaml file.
# MAGIC 3. Generate CREATE TABLE IF NOT EXISTS DDL statements for each object in the contract.
# MAGIC 4. Execute each DDL statement to create the tables in the UC catalog and schema defined within the contract server metadata.
# MAGIC
# MAGIC Note: 
# MAGIC This notebook assumes that the data contract conforms to a standard naming convention based on the catalog and schema: [catalog]__[schema].yaml

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
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="source_catalog", value=source_catalog)
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="source_schema", value=source_schema)
print(f"source_schema: {source_schema}")


dbutils.widgets.text("yaml_folder_path", f"{author_folder_path.split('/input_data')[0]}/data_contracts_data") # should be a volume
yaml_folder_path = dbutils.widgets.get("yaml_folder_path")
print(f"yaml_folder_path: {yaml_folder_path}")


# This variable below is used in the workflow named 'data_contract_deploy' and task 's5_data_contract_dq_checks' (IMPORTANT)
# yaml_file_path syntax --> "{source_catalog}__{source_schema}.yaml"
yaml_file_path = f"{yaml_folder_path}/catalog={source_catalog}/{source_catalog}__{source_schema}.yaml"
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="yaml_file_path", value=yaml_file_path)
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Initialize the Data Contract Object
# MAGIC
# MAGIC This step initializes the Data Contract CLI object.

# COMMAND ----------

# DBTITLE 1,Initialize the Data Contract Object
data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Get Data Contract Custom DDL
# MAGIC
# MAGIC This step exports SQL DDL from the ODCS data contract and splits them into individual statements.

# COMMAND ----------

# DBTITLE 1,Get Data Contract Custom DDL
queries_ddl_list = data_contract.export("sql")[:-1].split(";")
print(queries_ddl_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Execute Data Contract DDL
# MAGIC
# MAGIC Cleaning Steps:
# MAGIC - CREATE OR REPLACE TABLE is changed to CREATE TABLE IF NOT EXISTS
# MAGIC - Decimal data types are changed to double to avoid precision errors

# COMMAND ----------

# DBTITLE 1,Execute Data Contract DDL
for query in queries_ddl_list:
    query_clean = query.replace("CREATE OR REPLACE TABLE", "CREATE TABLE IF NOT EXISTS") # We do not want to overwrite an existing source table with a generated DL
    query_clean = query_clean.replace("decimal", "double") # We use 'double' due to floating point precision errors with decimal (e.g. decimal(10,2) --> decimal(10,0))
    try:
        x = spark.sql(query_clean)
    except Exception as e:
        print(f"{e}\n")
        if "[SCHEMA_NOT_FOUND]" in str(e):
            match = query.split("REPLACE TABLE")[1].split("(")[0].strip()
            catalog = match.split(".")[0] if match else None
            schema = match.split(".")[1] if match else None
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            print(f"created schema {catalog}.{schema} successfully\n")
            spark.sql(query_clean)
            continue
    print(f"COMPLETED RUNNING DDL QUERY:\n{query_clean}\n")
