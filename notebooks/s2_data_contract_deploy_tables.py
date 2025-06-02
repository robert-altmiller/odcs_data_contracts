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

# MAGIC %md
# MAGIC ## Step: Workflow Widget Parameters
# MAGIC
# MAGIC This step defines widgets and initializes variables.

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
current_directory = os.getcwd()

dbutils.widgets.text("data_contract_path", f"{current_directory}data_contracts_data/")  
data_contract_path = dbutils.widgets.get("data_contract_path")
print(f"data_contract_path: {data_contract_path}")

dbutils.widgets.text("server_name", "dev")  
server_name = dbutils.widgets.get("server_name")
print(f"server_name: {server_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Initialize the Data Contract Object
# MAGIC
# MAGIC This step initializes the Data Contract CLI object.

# COMMAND ----------

# DBTITLE 1,Initialize the Data Contract Object
data_contracts = get_list_of_contract_objects(data_contract_path)
data_contracts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Get Data Contract Custom DDL
# MAGIC
# MAGIC This step exports SQL DDL from the ODCS data contract and splits them into individual statements.

# COMMAND ----------

# DBTITLE 1,Get Data Contract Custom DDL
queries_ddl_list = []
for data_contract in data_contracts:
    try:
        queries_ddl_list.extend(data_contract.export("sql", server=server_name)[:-1].split(";"))
    except KeyError as e:
        raise KeyError(f"Server Name provided not found in data contract: {server_name}. Valid servers: {data_contract.get_data_contract_specification().servers}")
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
