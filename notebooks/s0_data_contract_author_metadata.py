# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[avro,csv,parquet,sql]' fastavro

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Library Imports
import fastavro

# COMMAND ----------

# DBTITLE 1,Initialize Data Contract Object
# Initialize Data Contract object
data_contract_obj = DataContract(spark=spark)

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Workflow Widget Parameters
# MAGIC
# MAGIC This section below sets up dynamic configuration for use within Databricks workflows and notebooks.  We initializes folder paths and catalog parameters via widgets, enabling seamless, parameterized execution across multiple workflow tasks.
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
# Widget Parameters
dbutils.widgets.text("user_email", dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())
user_email = dbutils.widgets.get("user_email")
print(f"user_email: {user_email}")


dbutils.widgets.text("author_folder_path", f"/Workspace/Users/{user_email}/odcs_data_contracts/notebooks/input_data")  # should be a Workspace Users folder
author_folder_path = dbutils.widgets.get("author_folder_path")
print(f"author_folder_path: {author_folder_path}")


dbutils.widgets.text("json_folder_path", f"{author_folder_path.split('/input_data')[0]}/json_data")  # should be a UC volume
json_folder_path = dbutils.widgets.get("json_folder_path")
print(f"avro_folder_path: {json_folder_path}")


# yaml folder path for where to store data contract
dbutils.widgets.text("yaml_folder_path", f"{author_folder_path.split('/input_data')[0]}/data_contracts_data")  # should be a UC volume
yaml_folder_path = dbutils.widgets.get("yaml_folder_path")
print(f"yaml_folder_path: {yaml_folder_path}")


# Git repository url
dbutils.widgets.text("git_repo_url", "https://github.com/robert-altmiller/odcs_data_contracts.git")
git_repo_url = dbutils.widgets.get("git_repo_url")
print(f"git_repo_url: {git_repo_url}")


# Source catalog parameter
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


# Source schema parameter
dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


# Running in workflow widget
dbutils.widgets.text("running_in_workflow", "")
# Retrieve widget values safely
job_context = {
    "running_in_workflow": dbutils.widgets.get("running_in_workflow"),
}
# Unit test
print(f"is_running_in_databricks_workflow: {is_running_in_databricks_workflow(job_context)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Copy Authoring Input Files From Github to Databricks
# MAGIC
# MAGIC This section below copies the input_data folder from Github into Databricks if it does not exist.  If it already exists in Databricks we do not overwrite the files.  The authoring files for building the ODCS data contract are store in the following location: /Workspace/Users/[YOUR_EMAIL_ADDRESS]/odcs_data_contracts/notebooks/input_data

# COMMAND ----------

# DBTITLE 1,Copy Authoring Input Files From Github to Databricks
# Copy input data authoring files from Github to Workspace/Users if running in workflow
gh_clone_base_path = "/tmp/odcs_data_contracts"
gh_input_data_path = f"{gh_clone_base_path}/notebooks/input_data"
subprocess.run(["rm", "-rf", gh_clone_base_path], check=True)
subprocess.run(["git", "clone", git_repo_url, gh_clone_base_path], check=True)
dbutils.fs.cp(
    f"file:{gh_input_data_path}",
    f"file:{author_folder_path}",
    recurse=True
)
subprocess.run(["rm", "-rf", gh_clone_base_path], check=True)

for root, dirs, files in os.walk(author_folder_path):
    for file in files:
        if file.endswith(".crc"):
            file_path = os.path.join(root, file)
            os.remove(file_path)

print("finished copying the input_data folder from Github into Databricks")
print(f"input_data folder path: {author_folder_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Check For and Read Existing Data Contract (If Exists)
# MAGIC Check if a data contract already exists for the user specified 'source catalog' and 'source schema'.  If it does this implies that the product domain subject matter expert (SME) is making updates and/or deletes to an existing contract.  So, read the contents of the current version data contract, and copy all the contract details into the authoring user input JSON files.  Then the SME can update or delete from the JSON user input files and create a new versioned contract by running the 's1_data_contract_create' workflow.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check For and Read Existing Data Contract
# Check if data contract already exists
yaml_file_path = f"{yaml_folder_path}/catalog={source_catalog}/{source_catalog}__{source_schema}.yaml"
contract_exists = check_file_exists(yaml_file_path)
print(f"{yaml_file_path} exists: {contract_exists}")


if contract_exists: # then the data contract exists

    with open(yaml_file_path, 'r') as f:
        data_contract_odcs_yaml = yaml.safe_load(f)

    # Get contract metadata
    contract_metadata = extract_data_contract_metadata(data_contract_odcs_yaml)
    write_json_file(contract_metadata, f"{author_folder_path}/contract_metadata_input/contract_metadata.json")
    
    # Get data quality rules metadata
    data_quality_rules = extract_quality_rules_metadata(data_contract_odcs_yaml)
    write_json_file(data_quality_rules, f"{author_folder_path}/data_quality_rules_input/data_quality_rules.json")  
    
    # Get teams metadata
    team_metadata = data_contract_odcs_yaml.get("team", [])
    write_json_file(team_metadata, f"{author_folder_path}/team_metadata_input/team_metadata.json") 
    
    # Get roles metadata
    roles_metadata = data_contract_odcs_yaml.get("roles", [])
    write_json_file(roles_metadata, f"{author_folder_path}/roles_metadata_input/roles_metadata.json")
    
    # Get pricing metadata
    pricing_metadata = data_contract_odcs_yaml.get("price", {})
    write_json_file(pricing_metadata, f"{author_folder_path}/pricing_metadata_input/pricing_metadata.json")
    
    # Get server metadata
    server_metadata = data_contract_odcs_yaml.get("servers", [])
    write_json_file(server_metadata, f"{author_folder_path}/server_metadata_input/server_metadata.json")
    
    # Get sla metadata
    sla_metadata = extract_sla_metadata(data_contract_odcs_yaml)
    write_json_file(sla_metadata, f"{author_folder_path}/sla_metadata_input/sla_metadata.json")

    # Extract support channels metadata
    support_channels_metadata = data_contract_odcs_yaml.get("support", [])
    write_json_file(support_channels_metadata, f"{author_folder_path}/support_channel_metadata_input/support_channel_metadata.json")
    
    # Extract schema metadata and remove dq rules
    schema_metadata = data_contract_odcs_yaml.get("schema", {})
    for table in schema_metadata: table.pop("quality", None)
    write_json_file({"schema": schema_metadata}, f"{author_folder_path}/schema_metadata_input/schema_metadata.json")
else: print("there is no existing data contract to create authoring files for")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Update Schema Metadata Authoring JSON File
# MAGIC
# MAGIC If a data contract does not exist and the user specified 'source catalog' and 'source schema' does exist then we will overwrite the input_data --> schema_metadata_input --> schema_metadata.json with the schema, schema/table/column level tags, table and columns descriptions/comments for the existing 'source_catalog' and 'source_schema'.  After the schema_metadata.json is overwritten it will be used to build the 'schema' section in the ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Get a List of Tables in the User Specified Catalog.Schema
# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook

# This section below uses the helper Python function 'list_tables_in_schema()' in the helpers folder --> general_helpers.py to generate a list of table_names for a user specified Databricks catalog and schema.
# remove data quality tables created by the data contract framework if they exist

tables_list, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

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

# Update schema_metadata.json with existing catalog.schema metadata
if not contract_exists and catalog_exists(source_catalog) and schema_exists(source_catalog, source_schema) and len(tables_list) > 0:
    
    create_local_data(source_catalog, source_schema, tables_list, json_folder_path)

else:
    print("--> INFO: can only update the schema_metadata authoring file if data contract does NOT EXIST and source_schema tables EXIST <--")
    print(f"contract_exists: {contract_exists}")
    print(f"source_catalog exists: {catalog_exists(source_catalog)}")
    print(f"source_schema exists: {catalog_exists(source_catalog)}")
    print(f"source_schema tables: {tables_list}")

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

# Update schema_metadata.json with existing catalog.schema metadata
if not contract_exists and catalog_exists(source_catalog) and schema_exists(source_catalog, source_schema) and len(tables_list) > 0:

    # Create Data Contracts For Each Table and Combine
    # you can print an individual contract using print(json.dumps(data_contracts_dict["table_name"])) for testing.  Table name comes from the name of the avro, csv, parquet, or sql files 
    data_contracts_combined, data_contracts_dict = combine_data_contract_models(source_catalog, source_schema, tables_with_desc_dict, json_folder_path)
    data_contract_odcs_yaml = data_contracts_combined

    # Extract schema metadata and remove dq rules
    schema_metadata = {}
    schema_metadata['schema'] = data_contract_odcs_yaml.get("schema")
    schema_metadata['tags'] = data_contract_odcs_yaml.get('tags', [])
    for table in schema_metadata['schema']: table.pop("quality", None)
    write_json_file(
        schema_metadata, f"{author_folder_path}/schema_metadata_input/schema_metadata.json")
else:
    print("--> INFO: can only update the schema_metadata authoring file if data contract does NOT EXIST and source_schema tables EXIST <--")
    print(f"contract_exists: {contract_exists}")
    print(f"source_catalog exists: {catalog_exists(source_catalog)}")
    print(f"source_schema exists: {catalog_exists(source_catalog)}")
    print(f"source_schema tables: {tables_list}")
