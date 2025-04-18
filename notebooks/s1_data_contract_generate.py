# Databricks notebook source
# DBTITLE 1,Import Notebooks
# MAGIC %run "./s0_data_contract_generate"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Metadata (Custom)
# MAGIC
# MAGIC The high-level ODCS contract metadata specifies the title, version, domain, status, dataproduct, tenant, description, and tags.  These fields should be used and defined in a way that a data consumer knows exactly what data product they are looking at from a specific product domain.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_contract_metadata()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'contract_metadata_input' folder --> contract_metadata.json and append the contract metadata to the ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Metadata (Custom)
if contract_build_method != "contract_first": # then data first approach
    
    # Apply metadata updates to the ODCS YAML contract
    data_contract_odcs_yaml = update_odcs_contract_metadata(data_contract_odcs_yaml, contract_metadata_input, source_catalog, source_schema)

else: print(f"---> contract_build_method is '{contract_build_method}' so skip this step <---")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Server Configuration (Custom)
# MAGIC
# MAGIC The ODCS server configuration specifies the server, type, host, catalog, and schema.  These fields are used by the Data Contract CLI to know how and where to deploy the tables and columns defined in the ODCS data contract.  The Data Contract CLI is able to export the data contract 'schema' section as table SQL definitions (e.g. DDLs) and the server metadata schema and catalog are used within those table SQL definitions.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_server_config()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'server_metadata_input' folder --> server_metadata.json and append the contract server metadata to the ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Server Configuration (Custom)
# Update the server configuration in the ODCS data contract
data_contract_odcs_yaml = update_odcs_server_config(data_contract_odcs_yaml, server_metadata_input, source_catalog, source_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Add SQL Data Quality Rules to ODCS Contract
# MAGIC
# MAGIC Data quality rules represent a reusable component in ODCS data contracts since they can be scheduled to run in batch or real-time depending on workload requirements.  The Data Contract CLI reads the DQ rules from the ODCS data contract and executes the data quality rules against the tables defined in the contract using the command 'data_contract.test()'.  The results from 'running data_contract.test()' produce results that list all the out of the box, generic, and custom data quality and the results of each of those dq rules as 'pass or 'fail'.  Out of the box (OOB) data quality rules are automatically run against tables using the Data Contract CLI and check that columns exist, and they are the correct datatype.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_data_quality_rules()' in the helpers folder --> contract_helpers.py to read user specified JSON data quality rules in the 'input_data' folder --> 'data_quality_rules_input' folder --> data_quality_rules.json and appends those 'custom' data quality rules to the appropriate table under the ODCS data contract 'schema' section.  We also append 'general' data quality into the ODCS data contract.  These rules get appended to every table section under the ODCS data contract schema section and the SQL is generated dynamically.
# MAGIC
# MAGIC - The 'update_data_quality_rules()' Python function uses the following functions: 
# MAGIC   - 'get_general_data_quality_rules()'
# MAGIC   - 'get_custom_data_quality_rules()'
# MAGIC
# MAGIC - There is no limit on the number of custom data quality rules in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Add SQL Data Quality Rules to ODCS Contract
# Apply data quality rules to the ODCS YAML contract
data_contract_odcs_yaml = update_data_quality_rules(data_contract_odcs_yaml, source_catalog, source_schema, custom_dq_rules_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Support Channel (Custom)
# MAGIC
# MAGIC The ODCS support channel configuration specifies channel, scope, url, scope, and description.  These fields are additional metadata for the data consumer to understand how to get help with a data product, find product domain and data product announcements and updates, or know how to engage in an interactive chat with a product domain team or specialist.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_support_channel()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'support_channel_metadata_input' folder --> support_channel_metadata.json and append the support channel metadata to the ODCS data contract.  There is no limit on the number of support channel requirements in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Support Channel (Custom)
# Add support channels to the ODCS data contract
data_contract_odcs_yaml = update_odcs_support_channel(data_contract_odcs_yaml, support_channel_metadata_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS SLA (Custom)
# MAGIC
# MAGIC The ODCS service level agreement (SLA) configuration specifies property, value, valueext, unit, and element.  These fields are additional metadata for the data consumer to understand what SLAs apply to the data product they want to use such as data freshness, data retention, data frequency, and data time of availability.  This is important for a data consumer because these upstream data product SLAs are inherited by all other downstream data products that the data consumer builds.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_sla_metadata()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'sla_metadata_input' folder --> sla_metadata.json and append the SLA metadata to the ODCS data contract.  There is no limit on the number of SLA requirements in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS SLA (Custom)
# Add server level agreements (SLAs) to the ODCS data contract
data_contract_odcs_yaml = update_odcs_sla_metadata(data_contract_odcs_yaml, sla_metadata_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Team (Custom)
# MAGIC
# MAGIC The ODCS team configuration specifies username, role, datein, dateout, replacebyusernam, comment, and name.  These fields are additional metadata for the data consumer to understand who supports a specifc product domain.  This is important for a data consumer because these product domain subject matter experts (SMEs) can be reached if there are issues, questions or enhancements needed for a data product.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_team_metadata()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'team_metadata_input' folder --> teams_metadata.json and append the Team metadata to the ODCS data contract.  There is no limit on the number of Team requirements in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Team (Custom)
# Add teams to the ODCS data contract
data_contract_odcs_yaml = update_odcs_team_metadata(data_contract_odcs_yaml, team_metadata_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Roles (Custom)
# MAGIC
# MAGIC The ODCS roles configuration specifies role, access, firstlevelapprovers, and secondlevelapprovers.  These fields are additional metadata that define what roles data consumers need to be able to access data products.  These roles can also be mapped directly to user groups.  These roles are important for data consumers to have the right level of access to a data product.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_roles_metadata()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'roles_metadata_input' folder --> roles_metadata.json and append the Roles metadata to the ODCS data contract.  There is no limit on the number of Role requirements in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Roles (Custom)
data_contract_odcs_yaml = update_odcs_roles_metadata(data_contract_odcs_yaml, roles_metadata_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Update ODCS Pricing (Custom)
# MAGIC
# MAGIC The ODCS pricing configuration specifies priceamount, priceunit, anbd pricecurrency.  These fields are additional metadata that define how much .  These roles can also be mapped directly to user groups.  These roles are important for data consumers to have the right level of access to a data product.
# MAGIC
# MAGIC This section below uses the Python helper function 'update_odcs_pricing_metadata()' in the helpers folder --> contract_helpers.py to read user specified JSON high-level contract metadata in the 'input_data' folder --> 'pricing_metadata_input' folder --> pricing_metadata.json and append the Pricing metadata to the ODCS data contract.  There is no limit on the number of Pricing requirements in an ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Update ODCS Pricing (Custom)
data_contract_odcs_yaml = update_odcs_pricing_metadata(data_contract_odcs_yaml, pricing_metadata_input)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Save ODCS Data Contract Locally
# MAGIC
# MAGIC After all the required sections are appended to the ODCS data contract object it's time to save the contract locally, in the Databricks file system, or in a Unity Catalog (UC) volume.  The __RECOMMENDATION__ is to use a custom UC volumes location that is backed by a storage account S3 bucket or ADLSGEN2 container. Since the contract data-first approach creates an ODCS data contract at the Catalog.Schema level the __NAMING CONVENTION__ for the contract should reflect the following pattern: '{catalog_name}__{schema_name}.yaml'.  It is also important to ensure you have a robust and scalable organizational folder structure or Spark dataframe partitioning strategy for storing data contracts.  This folder structure or Spark dataframe partitioning strategy should enable storing multiple versions of contracts easily when existing contracts are updated or deleted from over time.  In consideration of the spark dataframe method in 'Example 3' below the contract 'version_number' would have to be stored in each row of the dataframe.  A single row in the Spark dataframe would represent all the data associated with one Catalog.Schema data product ODCS data contract.
# MAGIC
# MAGIC - Example 1 organizational folder structure:
# MAGIC   - Level 1: 'workspace_name / url' folder
# MAGIC   - Level 2: 'data_contracts' folder
# MAGIC   - Level 3: 'catalog_name' folder
# MAGIC   - Level 4a: 'schema_name' folder
# MAGIC     - Level 4b: 'odcs_data_contract.yaml' generic filename
# MAGIC     - level 4c: 'versions' folder
# MAGIC       - level 4c_a:  'odcs_data_contract __ {version_number} __ {timestamp}.yaml' versioned filename
# MAGIC
# MAGIC
# MAGIC - Example 2 organizational folder structure:
# MAGIC   - Level 1: 'workspace_name / url' folder
# MAGIC   - Level 2: 'data_contracts' folder
# MAGIC   - level 3a: 'catalog_name' folder
# MAGIC     - Level 3b: '{catalog_name} __ {schema_name}.yaml' filename
# MAGIC     - Level 3c: 'versions' folder
# MAGIC       - level 3c_a: '{catalog_name} __ {schema_name} __ {version_number} __ {timestamp}.yaml' versioned filename
# MAGIC
# MAGIC - Example 3 partitioned folder structure from a Spark dataframe with contract metadata.  This partitioned structure enables querying the latest version via Spark using ORDER BY 'version_number' DESC or similar logic.
# MAGIC   - Level 1: 'workspace_name / url' folder
# MAGIC   - Level 2: 'data_contracts' folder
# MAGIC   - Level 3: 'catalog=catalog_name' partitioned df folder
# MAGIC   - level 4: 'schema=schema_name' partitioned df folder
# MAGIC   - level 5: version={version_number} partitioned df folder
# MAGIC   - level 6: filename={custom_contract_file_name}.yaml filename
# MAGIC
# MAGIC Among the 3 examples above, example 3 is recommended for product domain teams that want to store contract metadata using Spark dataframes.  This method is flexible and scalable, however, when using the Spark dataframe to serve metadata into the Data Contract CLI the Spark dataframe row(s) would have to be converted to an ODCS compliant yaml format for executing CLI API commands.
# MAGIC
# MAGIC This section below uses the Python helper function 'save_odcs_data_contract_local()' in the helpers folder --> contract_helpers.py to save the ODCS data contract data stored in the 'data_contract_odcs_yaml' variable to the UC volumes path defined in the 'yaml_folder_path' variable.

# COMMAND ----------

# DBTITLE 1,Save ODCS Data Contract Locally
# Save the ODCS data contract locally
yaml_file_path = save_odcs_data_contract_local(data_contract_odcs_yaml, source_catalog, source_schema, yaml_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Lint ODCS Data Contracts
# MAGIC
# MAGIC The Data Contract CLI has a linting built-in method for checking the syntax of a created ODCS data contract.  It performs a static check of the ODCS data contract YAML file to ensure correct syntax, required field presence, and overall spec compliance.  This is an important step to pass prior to creating a new contract or updating an existing contact to a new version.
# MAGIC
# MAGIC This section below uses the Python helper function 'lint_data_contract()' in the helpers folder --> contract_helpers.py to run a lint syntax check on a saved ODCS data contract YAML file to validate its structure, completeness, and rule compliance.

# COMMAND ----------

# DBTITLE 1,Lint ODCS Data Contracts
# Lint and ODCS data contract
lint_result = lint_data_contract(yaml_file_path, spark)
