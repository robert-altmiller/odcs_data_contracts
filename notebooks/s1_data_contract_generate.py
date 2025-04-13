# Databricks notebook source
# MAGIC %md
# MAGIC ### Create ODCS Data Contract Against and Existing Databricks Schema and Tables (Contract Data First Approach)
# MAGIC
# MAGIC ODCS (Operational Data Contract Specification) data contracts are structured agreements between data producers and consumers that define the expectations, structure, and quality of shared data. These contracts are written in YAML and follow a standardized specification to ensure consistency across teams and platforms. They include schema definitions, metadata, and semantic tags, and they serve as the source of truth for how data should look and behave—enabling validation, documentation, and automation across the data lifecycle. Within ODCS, data contracts help enforce schema governance, improve data quality, and streamline collaboration between data producers and consumers in large-scale data platforms and data meshes.
# MAGIC
# MAGIC The Open Data Contract Specification has the following features:
# MAGIC - YAML-based schema contracts
# MAGIC - Defines tables, columns, types
# MAGIC - Supports nested + complex types
# MAGIC - Tagging at table/column level
# MAGIC - Owner + consumer metadata
# MAGIC - CLI validation and linting
# MAGIC - Tracks spec version
# MAGIC - Detects breaking changes
# MAGIC - Example exports: AVRO, CSV, JSON, SQL 

# COMMAND ----------

# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[databricks,avro,csv,parquet,sql]' fastavro

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import fastavro

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Initialize Data Contract Object
# Data contract object initialization
data_contract_obj = DataContract(spark=spark)

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Workflow Widget Parameters
# MAGIC
# MAGIC This section below sets up dynamic configuration for use within Databricks workflows and notebooks. It retrieves authentication credentials and initializes folder paths and catalog parameters via widgets, enabling seamless, parameterized execution across multiple workflow tasks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Authentication
# MAGIC
# MAGIC The Databricks workspace URL and a user-scoped personal access token (PAT) are generated dynamically. These credentials are used to interact with the Databricks REST API during workflow or notebook execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Folder Path Parameters (Avro, CSV, Parquet, SQL)
# MAGIC
# MAGIC The different folder path parameters below specify the different ways you can store source data to create an ODCS data contract.  CSV, Avro, Parquet and SQL files are created from the specified source schema and tables and used with 'data_contract.import_from_source()' Data Contract CLI function to generate a base contract template using the Data Contract data-first approach.  The 'yaml_folder_path' specifies where to store the generated ODCS data contract.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Source Catalog and Schema
# MAGIC
# MAGIC The specified 'source_catalog' and 'source_schema' specify the schema and tables that the ODCS data contract should be built against.

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Get Databricks instance and personal access token dynamically
databricks_instance = spark.conf.get("spark.databricks.workspaceUrl")
databricks_pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


# Folder path parameters
dbutils.widgets.text("avro_folder_path", "./avro_data")  # should be a volume
avro_folder_path = dbutils.widgets.get("avro_folder_path")
print(f"avro_folder_path: {avro_folder_path}")


dbutils.widgets.text("csv_folder_path", "./csv_data")  # should be a volume
csv_folder_path = dbutils.widgets.get("csv_folder_path")
print(f"csv_folder_path: {csv_folder_path}")


dbutils.widgets.text("parquet_folder_path", "./parquet_data")  # should be a volume
parquet_folder_path = dbutils.widgets.get("parquet_folder_path")
print(f"parquet_folder_path: {parquet_folder_path}")


dbutils.widgets.text("sql_folder_path", "./sql_data")  # should be a volume
sql_folder_path = dbutils.widgets.get("sql_folder_path")
print(f"sql_folder_path: {sql_folder_path}")


dbutils.widgets.text("yaml_folder_path", "./data_contracts_data")  # should be a volume
yaml_folder_path = dbutils.widgets.get("yaml_folder_path")
# BELOW IS IMPORTANT TO PASS PARAMETER BETWEEN WORKFLOW STEPS
#dbutils.jobs.taskValues.set(key="yaml_folder_path", value=yaml_folder_path) 
print(f"yaml_folder_path: {yaml_folder_path}")


# Source catalog and source schema parameters
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ## Step: Get a List of Tables and Table Comments in a Catalog.Schema
# MAGIC
# MAGIC This section below uses the helper Python function 'list_tables_in_schema()' in the helpers folder --> general_helpers.py to generate a list of table_names for a user specified Databricks catalog and schema.

# COMMAND ----------

# DBTITLE 1,Get a List of Tables and Table Comments in a Catalog.Schema
# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook
tables_list, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

# remove data quality tables created by the data contract framework
# tables_list = [t for t in tables_list if "data_quality" not in t]
# tables_with_desc_dict = {k: v for k, v in tables_with_desc_dict.items() if "data_quality" not in k}

print(f"tables_list: {tables_list}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Create Folder Path Dictionary
# MAGIC
# MAGIC This section below creates a 'folder_path_dict' Python dictionary to enable seamless lookup of the Avro, CSV, Parquet, and SQL folder paths specified in the notebook widgets above.

# COMMAND ----------

# DBTITLE 1,Create Folder Path Dictionary
folder_path_dict = {
    "avro": avro_folder_path,
    "csv": csv_folder_path,
    "parquet": parquet_folder_path,
    "sql": sql_folder_path
}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Read the Tables and Save as Avro, CSV, Parquet, or SQL
# MAGIC
# MAGIC This section below uses the Python helper function 'create_local_data()' in the helpers folder --> contract_helpers.py to read all the tables in the 'source_catalog' and 'source_schema' and generate a set of Avro, CSV, and Parquet files with a single row of data from the source table.  Every column in the generated files will contain a value so the Data Contract CLI knows what datatype to assign in the ODCS Data Contract.  If you prefer SQL instead to ensure datatypes are 'not inferred' a SQL table definition (e.g. DDL) with datatypes will be generated for each schema source table, and these SQL files can be used with the Data Contract CLI to generate the 'schema' within the ODCS data contract definition.
# MAGIC
# MAGIC - The 'create_local_data()' Python function uses the following functions:
# MAGIC   - 'infer_avro_schema()'
# MAGIC   - 'convert_complex_type_cols_to_str()'
# MAGIC   - 'serialize_complex()'
# MAGIC   - 'is_scalar()'
# MAGIC   - 'get_uc_table_ddl()'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read the Tables and Save as Avro, CSV, Parquet, or SQL
methods = ["sql"] # OR ["avro", "parquet", "csv", "sql"]
for method in methods:
    create_local_data(source_catalog, source_schema, tables_list, folder_path_dict[method], method = method)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Create Data Contracts For Each Table and Combine
# MAGIC
# MAGIC This section below uses the Python helper function 'combine_data_contract_models()' in the helpers folder --> contract_helpers.py to read the generated files (e.g. SQL files) created in the previous step and generate a data contract specification (dcs) data contract for each one.  We then convert each of these dcs data contracts into an ODCS compliant data contract using the Data Contract CLI.  If there are any table columns with complex data types (e.g. variants) we capture those datatypes from the 'dcs data contract' for those columns and store them in a Python dictionary.  Next the complex column datatypes (e.g. nested StructType()) are appended to the generated ODCS data contract.  The very last step is to join all the individual table level ODCS data contracts into a single combined ODCS data contract.  Combined implies that all the schema tables are in a single contract 'schema' block.
# MAGIC
# MAGIC - The 'data_contracts_combined' can be printed with the following command for testing: __print(json.dumps(data_contracts_combined))__
# MAGIC - The 'data_contracts_dict' can print each individual table level ODCS contract with the following command for testing: __print(json.dumps(data_contracts_dict["YOUR_TABLE_NAME"]))__
# MAGIC
# MAGIC - The 'combine_data_contract_models()' Python function uses the following functions:
# MAGIC   - 'generate_odcs_base_contract()'
# MAGIC   - 'dcs_extract_variant_columns_and_physicaltypes()'
# MAGIC   - 'odcs_import_variant_columns_and_physicaltypes()'
# MAGIC   - 'get_column_comments()'
# MAGIC   - 'get_data_contract_tags()'
# MAGIC   - 'get_data_contract_column_tags()'
# MAGIC   - 'tag_dict_to_list()'
# MAGIC   - 'replace_none_with_empty_string_in_json()'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Data Contracts For Each Table and Combine
# you can print an individual contract using print(json.dumps(data_contracts_dict["table_name"])) for testing.  Table name comes from the name of the parquet file.
method = "sql" # or csv or parquet or sql
data_contracts_combined, data_contracts_dict = combine_data_contract_models(source_catalog, source_schema, tables_with_desc_dict, folder_path_dict[method], method = method)

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
data_contract_odcs_yaml = update_data_quality_rules(data_contracts_combined, source_catalog, source_schema, custom_dq_rules_input)

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
# Apply metadata updates to the ODCS YAML contract
data_contract_odcs_yaml = update_odcs_contract_metadata(data_contract_odcs_yaml, contract_metadata_input, source_catalog, source_schema)

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
