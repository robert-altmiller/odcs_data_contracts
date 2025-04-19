# Databricks notebook source
# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/extract_helpers"

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Folder Path Parameters
# MAGIC
# MAGIC  The 'yaml_folder_path' specifies where the generated ODCS data contract is stored
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Source Catalog and Schema
# MAGIC
# MAGIC The specified 'source_catalog' and 'source_schema' specify the schema and tables that the ODCS data contract should be built against.

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
dbutils.widgets.text("yaml_folder_path", "./data_contracts_data")  # should be a UC volume
yaml_folder_path = dbutils.widgets.get("yaml_folder_path")
print(f"yaml_folder_path: {yaml_folder_path}")


# Source catalog parameter
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


# Source schema parameter
dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Check For and Read Existing Data Contract (If Exists)
# MAGIC Check if a data contract already exists for the user specified 'source catalog' and 'source schema'.  If it does this implies that the product domain subject matter expert (SME) is making updates and/or deletes to an existing contract.  So, read the contents of the current version data contract, and copy all the contract details into the authoring user input JSON files.  Then the SME can update or delete from the JSON user input files and create a new versioned contract by running the 's1_data_contract_create' workflow.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check For and Read Existing Data Contract
def check_file_exists(filepath):
    if os.path.exists(filepath):
        return True
    else: return False

existing_yaml_file_path = yaml_folder_path + f"/{source_catalog}__{source_schema}.yaml"
print(f"existing_yaml_file_path: {existing_yaml_file_path}")
contract_exists = check_file_exists(existing_yaml_file_path)

if contract_exists: # then the data contract exists

    with open(existing_yaml_file_path, 'r') as f:
        data_contract_odcs_yaml = yaml.safe_load(f)

    # Get contract metadata
    contract_metadata = extract_data_contract_metadata(data_contract_odcs_yaml)
    write_json_file(contract_metadata, f"input_data2/contract_metadata_input/contract_metadata.json")
    
    # Get data quality rules metadata
    data_quality_rules = extract_quality_rules_metadata(data_contract_odcs_yaml)
    write_json_file(data_quality_rules, f"input_data2/data_quality_rules_input/data_quality_rules.json")  
    
    # Get teams metadata
    team_metadata = data_contract_odcs_yaml["team"]
    write_json_file(team_metadata, f"input_data2/team_metadata_input/team_metadata.json") 
    
    # Get roles metadata
    roles_metadata = data_contract_odcs_yaml["roles"]
    write_json_file(roles_metadata, f"input_data2/roles_metadata_input/roles_metadata.json")
    
    # Get pricing metadata
    pricing_metadata = data_contract_odcs_yaml["price"]
    write_json_file(pricing_metadata, f"input_data2/pricing_metadata_input/pricing_metadata.json")
    
    # Get server metadata
    server_metadata = data_contract_odcs_yaml["servers"]
    write_json_file(server_metadata, f"input_data2/server_metadata_input/server_metadata.json")
    
    # Get sla metadata
    sla_metadata = extract_sla_metadata(data_contract_odcs_yaml)
    write_json_file(sla_metadata, f"input_data2/sla_metadata_input/sla_metadata.json")
    
    # Extract support channels metadata
    support_channels_metadata = data_contract_odcs_yaml["support"]
    write_json_file(support_channels_metadata, f"input_data2/support_channel_metadata_input/support_channel_metadata.json")
    
    # Extract schema metadata and remove dq rules
    schema_metadata = data_contract_odcs_yaml["schema"]
    for table in schema_metadata: table.pop("quality", None)
    write_json_file({"schema": schema_metadata}, f"input_data2/schema_metadata_input/schema_metadata.json")
