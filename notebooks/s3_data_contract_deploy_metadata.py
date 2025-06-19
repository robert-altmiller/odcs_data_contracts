# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Metadata Notebook
# MAGIC
# MAGIC This notebook is used to deploy tags and descriptions defined in the contract.
# MAGIC
# MAGIC Given the path to a data contract folder, this notebook performs the following actions:
# MAGIC 1. Reads in the contracts.
# MAGIC 2. Retrieves schema, table, and column level tags and descriptions.
# MAGIC 3. Formats the tags according to unity catalog requirments.
# MAGIC 4. Deploys the tags and descriptions to the relevant object in UC.

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
# MAGIC ## Step: Read in the Data Contracts
# MAGIC
# MAGIC This step defines reads in the existing data contract yamls a list of DataContract objects
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read in the Data Contract Yaml
data_contracts = get_list_of_contract_objects(data_contract_path)
data_contracts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Iterates through Contracts and Deploys Metadata
# MAGIC
# MAGIC This step leverages three helper functions from the general_helpers notebook to deploy the metadata: format_tags, apply_uc_metadata, and deploy_contract_metadata.

# COMMAND ----------

for contract in data_contracts:
    deploy_contract_metadata(contract)
