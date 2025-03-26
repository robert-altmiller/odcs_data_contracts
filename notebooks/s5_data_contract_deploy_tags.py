# Databricks notebook source
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



# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Folder and File Path Widget Parameters
dbutils.widgets.text("data_contract_filename_catalog", "hive_metastore")
data_contract_filename_catalog = dbutils.widgets.get("data_contract_filename_catalog")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
dbutils.jobs.taskValues.set(key="data_contract_filename_catalog", value=data_contract_filename_catalog) 




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
