# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[databricks,avro,csv,parquet,sql]'

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

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
dbutils.widgets.text("data_contract_filename_catalog", "hive_metastore")
data_contract_filename_catalog = dbutils.widgets.get("data_contract_filename_catalog")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="data_contract_filename_catalog", value=data_contract_filename_catalog) 

dbutils.widgets.text("data_contract_filename_schema", "default")
data_contract_filename_schema = dbutils.widgets.get("data_contract_filename_schema")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="data_contract_filename_schema", value=data_contract_filename_schema) 

# Tables: Using a JSON widget to store multiple key-value pairs
dbutils.widgets.text(
    "tables", "{'customer': 'customer table description', \
    'customer1': 'customer1 table description', \
    'customer2': 'customer2 table description', \
    'my_managed_table': 'my_managed_table description', \
    'my_managed_table1': 'my_managed_table1 description', \
    'my_managed_table2': 'my_managed_table2 description'}"
)
tables = ast.literal_eval(dbutils.widgets.get("tables"))
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="tables", value=tables) 

dbutils.widgets.text("data_contract_folder_path", "./data_contracts_data") # should be a volume
data_contract_folder_path = dbutils.widgets.get("data_contract_folder_path")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="data_contract_folder_path", value=data_contract_folder_path) 


# This variable below is used in the workflow named 'data_contract_deploy' task which runs 's4_data_contract_dq_checks' (IMPORTANT)
yaml_file_path = f"{data_contract_folder_path}/{data_contract_filename_catalog}__{data_contract_filename_schema}.yaml"
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="yaml_file_path", value=yaml_file_path)
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# DBTITLE 1,Verify ODCS Contract
data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)
test_result = data_contract.test()

# Show test results
print(f"'{yaml_file_path}' ODCS test validation: {test_result.result}")

# COMMAND ----------

# DBTITLE 1,Get Data Contract Custom DDL
queries_ddl_list = data_contract.export("sql")[:-1].split(";")

# COMMAND ----------

# DBTITLE 1,Execute Data Contract DDL
for query in queries_ddl_list:
    query = f'''{query}'''
    try:
        spark.sql(query)
    except Exception as e:
        print(f"{e}\n")
        if "[SCHEMA_NOT_FOUND]" in str(e):
            match = query.split("REPLACE TABLE")[1].split("(")[0].strip()
            catalog = match.split(".")[0] if match else None
            schema = match.split(".")[1] if match else None
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            spark.sql(query)
            continue
    print(f"COMPLETED RUNNING DDL QUERY:\n{query}\n")
