# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[databricks]'

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers"

# COMMAND ----------

# DBTITLE 1,Python Imports
import ast, re, time
from datacontract.data_contract import DataContract

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Folder and File Path Widget Parameters
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


dbutils.widgets.text("data_contract_folder_path", "./data_contracts_data") # should be a volume
data_contract_folder_path = dbutils.widgets.get("data_contract_folder_path")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="data_contract_folder_path", value=data_contract_folder_path) 
print(f"data_contract_folder_path: {data_contract_folder_path}")


# This variable below is used in the workflow named 'data_contract_deploy' task which 
# runs 's4_data_contract_deploy_data' and 's5_data_contract_dq_checks' (IMPORTANT)
yaml_file_path = f"{data_contract_folder_path}/{source_catalog}__{source_schema}.yaml"
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="yaml_file_path", value=yaml_file_path)
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# DBTITLE 1,Read in the Data Contract Yaml File
data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)

# COMMAND ----------

# DBTITLE 1,Verify and Test ODCS Contract
test_result = data_contract.test()

# Show test results
print(f"'{yaml_file_path}' ODCS test validation: {test_result.result}")

# COMMAND ----------

# DBTITLE 1,Get Data Contract Custom DDL
queries_ddl_list = data_contract.export("sql")[:-1].split(";")
print(queries_ddl_list)

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
