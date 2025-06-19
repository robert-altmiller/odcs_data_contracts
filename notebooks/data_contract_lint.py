# Databricks notebook source
# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
current_directory = os.getcwd()

dbutils.widgets.text("data_contract_path", f"{current_directory}data_contracts_data/")  
data_contract_path = dbutils.widgets.get("data_contract_path")
print(f"data_contract_path: {data_contract_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Lint ODCS Data Contracts
# MAGIC
# MAGIC The Data Contract CLI has a linting built-in method for checking the syntax of a created ODCS data contract.  It performs a static check of the ODCS data contract YAML file to ensure correct syntax, required field presence, and overall spec compliance.  This is an important step to pass prior to creating a new contract or updating an existing contact to a new version.
# MAGIC
# MAGIC This section below uses the Python helper function 'lint_data_contract()' in the helpers folder --> contract_helpers.py to run a lint syntax check on a saved ODCS data contract YAML file to validate its structure, completeness, and rule compliance.

# COMMAND ----------

errors = []
for contract in get_list_of_contract_files(data_contract_path):
    yaml_dict = yaml.safe_load(open(f"{data_contract_path}/{contract}"))
    print(f"Validating {contract}")
    try:
        validate_data_contract(yaml_dict, spark)
    except Exception as e:
        errors.append(f"Error validating {contract}: {e}")
if errors:
    raise Exception("Errors encountered during validation: {}".format(errors))
