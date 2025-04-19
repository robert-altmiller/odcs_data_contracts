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
# MAGIC %pip install 'datacontract-cli[avro,csv,parquet,sql]' fastavro

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Library Imports
import fastavro

# COMMAND ----------

# DBTITLE 1,Initialize Data Contract Object
# Data contract object initialization
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


# Contract or data first approach widget
dbutils.widgets.text("contract_build_method", "contract_first")  # value: "contract_first" or "data_first"
contract_build_method = dbutils.widgets.get("contract_build_method")
print(f"contract_build_method: {contract_build_method}")


# Widget Parameters
dbutils.widgets.text("avro_folder_path", "./avro_data")  # should be a UC volume
avro_folder_path = dbutils.widgets.get("avro_folder_path")
print(f"avro_folder_path: {avro_folder_path}")


dbutils.widgets.text("csv_folder_path", "./csv_data")  # should be a UC volume
csv_folder_path = dbutils.widgets.get("csv_folder_path")
print(f"csv_folder_path: {csv_folder_path}")


dbutils.widgets.text("parquet_folder_path", "./parquet_data")  # should be a UC volume
parquet_folder_path = dbutils.widgets.get("parquet_folder_path")
print(f"parquet_folder_path: {parquet_folder_path}")


dbutils.widgets.text("sql_folder_path", "./sql_data")  # should be a UC volume
sql_folder_path = dbutils.widgets.get("sql_folder_path")
print(f"sql_folder_path: {sql_folder_path}")


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

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ## Step: Get a List of Tables and Table Comments in a Catalog.Schema
# MAGIC
# MAGIC This section below uses the helper Python function 'list_tables_in_schema()' in the helpers folder --> general_helpers.py to generate a list of table_names for a user specified Databricks catalog and schema.

# COMMAND ----------

# DBTITLE 1,Get a List of Tables and Table Comments in a Catalog.Schema
if contract_build_method != "contract_first": # then data first

    # Get a list of the tables in a Catalog.Schema
    # list_tables_in_schema() Python function is in the helpers notebook
    tables_list, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)

    # remove data quality tables created by the data contract framework if they exist
    #tables_list = [t for t in tables_list if "data_quality" not in t]
    #tables_with_desc_dict = {k: v for k, v in tables_with_desc_dict.items() if "data_quality" not in k}

    print(f"tables_list: {tables_list}")

else: print(f"---> contract_build_method is '{contract_build_method}' so skip this step <---")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step: Create Folder Path Dictionary
# MAGIC
# MAGIC This section below creates a 'folder_path_dict' Python dictionary to enable seamless lookup of the Avro, CSV, Parquet, and SQL folder paths specified in the notebook widgets above.

# COMMAND ----------

# DBTITLE 1,Create Folder Path Dictionary
if contract_build_method != "contract_first": # then data first

    folder_path_dict = {
        "avro": avro_folder_path,
        "csv": csv_folder_path,
        "parquet": parquet_folder_path,
        "sql": sql_folder_path
    }

else: print(f"---> contract_build_method is '{contract_build_method}' so skip this step <---")

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
if contract_build_method != "contract_first": # then data first
    
    methods = ["sql"] # OR ["avro", "parquet", "csv", "sql"]
    for method in methods:
        create_local_data(source_catalog, source_schema, tables_list, folder_path_dict[method], method = method)

else: print(f"---> contract_build_method is '{contract_build_method}' so skip this step <---")

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
if contract_build_method != "contract_first": # then data first approach

    # you can print an individual contract using print(json.dumps(data_contracts_dict["table_name"])) for testing.  Table name comes from the name of the parquet file.
    method = "sql" # or csv or parquet or sql
    data_contracts_combined, data_contracts_dict = combine_data_contract_models(source_catalog, source_schema, tables_with_desc_dict, folder_path_dict[method], method = method)
    data_contract_odcs_yaml = data_contracts_combined

else: # then contract first approach

    # Create initial ODCS yaml file
    data_contract_odcs_yaml = {}
    # Apply metadata updates to the ODCS YAML contract
    data_contract_odcs_yaml = update_odcs_contract_metadata(data_contract_odcs_yaml, contract_metadata_input, source_catalog, source_schema)
    
    # Get the contract first approach schema block
    schema_block = schema_metadata_input.get("schema", schema_metadata_input)
    data_contract_odcs_yaml["schema"] = schema_block
