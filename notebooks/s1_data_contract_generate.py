# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[databricks,avro,csv,parquet,sql]' fastavro

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers"

# COMMAND ----------

# DBTITLE 1,Python Imports
import ast, json, fastavro, os, shutil, time, yaml
from datetime import datetime, date
from datacontract.data_contract import DataContract
from pyspark.sql.functions import *

# Data contract object initialization
data_contract_obj = DataContract(spark=spark)

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

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

# DBTITLE 1,Get a List of Tables and Table Comments in a Catalog.Schema
# Get a list of the tables in a Catalog.Schema
# list_tables_in_schema() Python function is in the helpers notebook
tables_list, tables_with_desc_dict = list_tables_in_schema(source_catalog, source_schema)
print(f"tables_list: {tables_list}")

# COMMAND ----------

# DBTITLE 1,Read the Tables and Save as CSV File
def get_uc_table_ddl(catalog, schema, table):
    """
    Retrieves the DDL SQL statement to create a specified table from a Spark SQL catalog.
    Args:
        catalog (str): The catalog in which the table is located.
        schema (str): The schema in which the table is located.
        table (str): The name of the table for which to get the DDL.
    Returns:
        str: The DDL SQL statement for the specified table.
    """
    return spark.sql(f"""SHOW CREATE TABLE {catalog}.{schema}.{table};""").first()[0]


def create_local_data(catalog, schema, uc_tables_list, folder_path, method="csv"):
    """
    Creates local data files for given tables in specified formats (AVRO, CSV, PARQUET, SQL) and saves them to a designated folder path.
    Args:
        catalog (str): The catalog in Spark SQL from which to read tables.
        schema (str): The schema in Spark SQL from which to read tables.
        uc_tables_dict (dict): A dictionary with table names as keys and descriptions as values.
        folder_path (str): The path to the folder where data files will be saved.
        method (str, optional): The format of the data files (default is "csv").
    Raises:
        Exception: If there is an error fetching custom data quality rules.
    Prints:
        Messages indicating whether data files have been saved or if an error occurred.
    """
    if os.path.exists(folder_path): shutil.rmtree(folder_path)
    for table in uc_tables_list:
        file_name = f"{table}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = f"{folder_path}/{file_name}.{method}"
        df_initial = spark.read.table(f"{catalog}.{schema}.{table}").limit(5000)
        
        # Use agg() with first() to get the first non-null value for each column
        df = df_initial.select([first(col(c), ignorenulls=True).alias(c) for c in df_initial.columns]).dropna(how="all")
        
        if df.count() > 0:
            if method == "avro":
                df_avro = df.toPandas()
                for col_avro in df_avro.select_dtypes(include=["datetime64", "datetime", "timedelta", "object"]).columns:
                    df_avro[col_avro] = df_avro[col_avro].astype(str)
                avro_records = df_avro.to_dict(orient="records")
                # infer_avro_schema() Python function is in the helpers notebook
                avro_schema = infer_avro_schema(df_avro)
                with open(file_path, "wb") as out:
                    fastavro.writer(out, avro_schema, avro_records)
                print(f"✅ AVRO file saved at: {file_path}")
            elif method == "csv":
                df.toPandas().to_csv(file_path, index=False)
                print(f"✅ CSV file saved at: {file_path}")
            elif method == "parquet":
                df_pandas = df.toPandas()
                df_pandas.attrs.clear() # Clears non-serializable metadata (IMPORTANT)
                df_pandas.to_parquet(file_path)
                print(f"✅ PARQUET file saved at: {file_path}")
            elif method == "sql":
                sql_ddl = get_uc_table_ddl(catalog, schema, table)
                with open(file_path, "w+") as out:
                    out.write(sql_ddl)
                print(f"✅ SQL file saved at: {file_path}")
            else:
                print(f"method ({method}) not recognized")
folder_path_dict = {
    "avro": avro_folder_path,
    "csv": csv_folder_path,
    "parquet": parquet_folder_path,
    "sql": sql_folder_path
}
methods = ["parquet", "sql"] # OR ["avro", "parquet", "csv", "sql"]
for method in methods:
    create_local_data(source_catalog, source_schema, tables_list, folder_path_dict[method], method = method)

# COMMAND ----------

# DBTITLE 1,Generate ODCS Base Contract
def generate_odcs_base_contract(data_contract):
    """
    Converts a unified DataContract object into an ODCS-compatible YAML dictionary.
    This function takes a DataContract object, serializes it to YAML, reinitializes it, 
    and then exports it in the "odcs" format. The final result is parsed into a Python 
    dictionary for further use (e.g., YAML file creation or API submission).
    Args:
        data_contract (DataContract): The unified DataContract object containing multiple models.
    Returns:
        dict: A dictionary representing the ODCS-compatible data contract in YAML format.
    """
    # Serialize the DataContract object to a YAML string
    data_contract_yaml = data_contract.to_yaml()
    # Reinitialize a DataContract object using the YAML string and export to "odcs" format
    data_contract_odcs = DataContract(data_contract_str=data_contract_yaml, spark=spark).export("odcs")
    # Convert the ODCS YAML string to a Python dictionary
    data_contract_odcs_yaml = yaml.safe_load(data_contract_odcs)
    return data_contract_odcs_yaml

# COMMAND ----------

# DBTITLE 1,Create Data Contracts For Each Table and Combine
def combine_data_contract_models(catalog, schema, uc_tables_dict, folder_path, method="csv"):
    """
    Combines multiple data contract models into a single data contract object.
    This function iterates through a dictionary of table names and descriptions, importing
    data contract models from files (e.g., CSV, Parquet, SQL, or Avro) located at the 
    specified folder path. Each model's table and column level descriptions are updated based 
    on the input dictionary.
    Args:
        catalog (str): Catalog in which the tables are located.
        schema (str): Schema in which the tables are located.
        uc_tables_dict (dict): Dictionary where keys are table names and values are their descriptions.
        folder_path (str): Path to the folder containing the serialized data contract files.
        method (str, optional): The file format to use for importing data contract models.
                                Supported values are "csv", "parquet", "avro", and "sql".
                                Defaults to "csv".
    Returns:
        tuple:
            - data_contracts_table_first (DataContract): A single DataContract object combining all imported models.
            - data_contracts_dict (dict): Dictionary of individual table-level DataContract objects.
    """
    data_contracts_dict = {}
    counter = 0

    for table, table_desc in uc_tables_dict.items():
        try:
            # Try to import data contract model from file; skip if the file doesn't exist or is empty (e.g. empty table)
            source = f"{folder_path}/{table}.{method}"
            print(f"\n✅START✅ --> reading local table: {source} <-- ✅START✅")
            data_contracts_table = generate_odcs_base_contract(data_contract_obj.import_from_source(format=method, source=f"{source}"))
        except Exception as e:
            print(f"No rows of data exists: {e}")
            continue

        # Get table column level comments
        # column_comments() Python function is in the helpers notebook
        column_comments = get_column_comments(catalog, schema, table)

        # Get table and column level tags
        # get_data_contract_table_tags() and get_data_contract_column_tags Python functiona are in the helpers notebook
        try:
            tbl_tags = tag_dict_to_list(get_data_contract_tags(catalog, schema, table))
            col_tags = tag_dict_to_list(get_data_contract_column_tags(catalog, schema, table))
        except Exception as e: 
            tbl_tags = col_tags = None
            print("unable to get table and column level tags")
            print(e)

        # Update schema properties
        schema_obj = data_contracts_table["schema"][0] # Hold constant per table
        schema_obj["description"] = table_desc # Table level description
        
        if tbl_tags != None: schema_obj["tags"] = tbl_tags["tags"][table]
        else: schema_obj["tags"] = []
        
        for col in schema_obj["properties"]:
            col["description"] = column_comments[f"{catalog}.{schema}.{table}"][col["name"]] # Column level descriptions
            
            if col_tags != None: col["tags"] = col_tags["tags"][col["name"]]
            else: col["tags"] = []
        
        data_contracts_table = replace_none_with_empty_string_in_json(data_contracts_table)
        data_contracts_dict[table] = data_contracts_table

        if counter == 0:
            data_contracts_table_first = data_contracts_table
        else:
            # Merge models into the first data contract object
            data_contracts_table_first["schema"].extend(data_contracts_table["schema"])

        counter += 1

    return data_contracts_table_first, data_contracts_dict


method = "parquet" # or csv or parquet or sql
data_contracts_combined, data_contracts_dict = combine_data_contract_models(source_catalog, source_schema, tables_with_desc_dict, folder_path_dict[method], method = method)

# COMMAND ----------

# DBTITLE 1,Define Generic Data Quality Rules For All Tables (Custom)
def get_general_data_quality_rules(table, columns=None):
    """
    Generates a generic set of data quality SQL rules for a given data contract.

    These rules include:
    1. A row count check to ensure the table contains data.
    2. A uniqueness check across specified columns to ensure no duplicate rows.
    Args:
        table (str): The name of the table for which to create rules.
        columns (list, optional): List of column names to use for the duplicate check.
                                  If None or empty, the duplicate rule will be skipped or invalid.
    Returns:
        list: A list of data quality rule dictionaries formatted for use in a data contract.
    """
    partition_by_clause = ", ".join(columns) if columns else ""
    
    general_data_quality_rules = {
        f"{table}": {
            "quality": [
                {
                    "type": "sql",
                    "description": f"Ensures '{table}' table has data",
                    "query": f"SELECT COUNT(*) FROM {table}",
                    "mustBeGreaterThanOrEqualTo": 0
                }
            ]
        }
    }

    # Add duplicate check only if valid columns are provided
    if partition_by_clause:
        general_data_quality_rules[table]["quality"].append(
            {
                "type": "sql",
                "description": f"Ensure '{table}' table has no duplicate rows across all columns",
                "query": f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT *, COUNT(*) OVER (PARTITION BY {partition_by_clause}) AS row_count
                        FROM {table}
                    ) AS subquery
                    WHERE row_count > 1
                """,
                "mustBe": 0
            }
        )

    # Clean up SQL formatting (flatten multi-line SQL to single-line strings)
    for dq_rule in general_data_quality_rules[table]["quality"]:
        dq_rule["query"] = ' '.join(dq_rule["query"].split())
    return general_data_quality_rules[table]["quality"]

# COMMAND ----------

# DBTITLE 1,Define Custom Data Quality Rules (Custom)
def get_custom_data_quality_rules(table_name, custom_dq_rules_input):
    """
    Generates a custom set of data quality SQL rules for a given data contract table.
    These rules are specific to the `customer` table and include:
    1. A row count check to ensure the table does not exceed 100 records.
    2. A null check to ensure all customers have an email.
    3. A null check to ensure all customers have both first and last names.
    Args:
        table_name (str): The name of the table for which to generate custom rules.
    Returns:
        list: A list of data quality rule dictionaries formatted for use in a data contract.
    Raises:
        KeyError: If no custom rules are defined for the specified table.
    """

    if table_name not in str(custom_dq_rules_input):
        raise KeyError(f"No custom data quality rules defined for table: {table_name}")

    custom_data_quality_rules = {}
    for quality in custom_dq_rules_input:
        for rule_table, rules in quality.items():
            if rule_table == table_name: # Then updates rules
                return rules["quality"]

# COMMAND ----------

# DBTITLE 1,Add Data Quality Rules to ODCS Contract
def update_data_quality_rules(data_contract, catalog, schema, custom_dq_rules_input):
    """
    Appends general and custom data quality SQL rules to each table in a data contract schema.
    For each table in the input data contract:
    1. Retrieves the table's columns from Unity Catalog.
    2. Generates general data quality rules based on those columns.
    3. Attempts to retrieve any custom data quality rules (e.g., for specific business checks).
    4. Ensures no duplicate rules are added if they already exist in the contract.
    5. Appends the rules to the table's `quality` section in the data contract.
    Args:
        data_contract (dict): The data contract dictionary in ODCS YAML format.
        catalog (str): The Unity Catalog catalog name where the tables are located.
        schema (str): The Unity Catalog schema name where the tables are located.
    Returns:
        dict: The updated data contract with data quality rules applied to each table.
    """
    for table in data_contract["schema"]:
        if "quality" not in table:
            table["quality"] = []

        # Get column names for the table to generate general DQ rules
        cols = spark.read.table(f"{catalog}.{schema}.{table['name']}").columns
        general_dq_sql_rules = get_general_data_quality_rules(table["name"], cols)

        # Attempt to get custom rules; fall back to general only if not found
        try:
            custom_dq_sql_rules = get_custom_data_quality_rules(table["name"], custom_dq_rules_input)
            dq_rule_type = "generic and custom"
        except Exception as e:
            custom_dq_sql_rules = []
            dq_rule_type = "generic"

        # Prevent adding duplicate rules by comparing JSON string representations
        existing_rules = table["quality"]
        set_existing_rules = set(json.dumps(d, sort_keys=True) for d in existing_rules)
        set_general_dq_sql_rules = set(json.dumps(d, sort_keys=True) for d in general_dq_sql_rules)
        set_custom_dq_sql_rules = set(json.dumps(d, sort_keys=True) for d in custom_dq_sql_rules)

        # Check if rules are already applied
        if set_general_dq_sql_rules.issubset(set_existing_rules) and set_custom_dq_sql_rules.issubset(set_existing_rules):
            print(f"already appended '{dq_rule_type}' dq rules to table {table['name']}")
        else:
            table["quality"].extend(general_dq_sql_rules)
            table["quality"].extend(custom_dq_sql_rules)
            print(f"appended '{dq_rule_type}' dq rules to table {table['name']}")

    return data_contract


# Apply data quality rules to the ODCS YAML contract
data_contract_odcs_yaml = update_data_quality_rules(data_contracts_combined, source_catalog, source_schema, custom_dq_rules_input)

# COMMAND ----------

# DBTITLE 1,Update ODCS Metadata (Custom)
def update_odcs_contract_metadata(data_contract, contract_metadata_input, catalog, schema):
    """
    Updates the top-level metadata fields of an ODCS data contract.
    This function sets the contract’s title, version, status, domain, and additional metadata 
    like tags, tenant, and data product name. It also includes a default description structure 
    for purpose, limitations, and usage.
    Args:
        The contract_metadata_input list contains the following:
        - data_contract (dict): The ODCS data contract dictionary to update.
        - contract_version (str): Version string for the contract (e.g., "1.0.0").
        - contract_status (str): Status of the contract (e.g., "active", "inactive").
        - contract_title (str): Title of the data contract.
        - contract_domain (str): The business domain the data contract belongs to.
        - contract_tenant (str): The assicated airline company.
        - contract_description (str): The high level description for the data contract.
        - contract_tags (list): A list of string tags associated with the contract.
    Returns:
        dict: The updated data contract dictionary with domain and metadata fields populated.
    """
    for metadata in contract_metadata_input:
        data_contract["name"] = metadata["name"]
        data_contract["version"] = metadata["version"]
        data_contract["domain"] = metadata["domain"]
        data_contract["status"] = metadata["status"]
        data_contract["dataProduct"] = metadata["dataproduct"]
        data_contract["tenant"] = metadata["tenant"]
        data_contract["description"] = metadata["description"]
        
        try:
            data_contract["tags"] = tag_dict_to_list(get_data_contract_tags(catalog, schema))["tags"][schema]
        except Exception as e: 
            print("unable to get schema level tags so use manual set value in input_data -->  contract_metadata_input --> contract_metadata.json")
            data_contract["tags"] = metadata["contract_tags"] # set manually by user
            print(e)
            continue
            
    return data_contract


# Apply metadata updates to the ODCS YAML contract
data_contract_odcs_yaml = update_odcs_contract_metadata(data_contract_odcs_yaml, contract_metadata_input, source_catalog, source_schema)

# COMMAND ----------

# DBTITLE 1,Update ODCS Server Configuration (Custom)
def update_odcs_server_config(data_contract, server_metadata_input, catalog = None, schema = None):
    """
    Updates the server configuration block in an ODCS data contract.
    This sets the Unity Catalog environment details such as server type, host URL, 
    catalog, and schema used for the data contract.
    Args:
        The server_metadata_input list contains the following:
        data_contract (dict): The ODCS data contract dictionary to update.
        environment (str): The target environment name (e.g., "development", "production").
        dbricks_instance (str): The Databricks workspace URL.
        catalog (str): Unity Catalog catalog name.
        schema (str): Unity Catalog schema name.
    Returns:
        dict: The updated data contract dictionary with server configuration populated.
    """
    # if catalog and schema parameters are None
    if catalog == None: catalog = metadata["catalog"]
    if schema == None: schema = metadata["schema"]
    
    for metadata in server_metadata_input:
        updated_server_config = {
            "server": metadata["server"],
            "type": metadata["type"],
            "host": metadata["host"],
            "catalog": catalog,
            "schema": schema
        }
    data_contract["servers"] = [updated_server_config]
    return data_contract


# Update the server configuration in the ODCS data contract
data_contract_odcs_yaml = update_odcs_server_config(data_contract_odcs_yaml, server_metadata_input, source_catalog, source_schema)

# COMMAND ----------

# DBTITLE 1,Update ODCS Support Channel (Custom)
def update_odcs_support_channel(data_contract, support_channel_metadata_input):
    """
    Appends a support channel configuration to the ODCS data contract.
    This allows specifying support or communication channels (e.g., Teams, Email) 
    that users of the data product can use for help, announcements, or collaboration.
    Args:
        The support_channel_metadata_input list contains the following:
        data_contract (dict): The ODCS data contract dictionary to update.
        channel (str): The name or label of the support channel (e.g., "DAS Teams Channel").
        tool (str): The communication tool used (e.g., "teams", "email").
        scope (str): The type of support channel (e.g., "interactive", "announcements").
        url (str): The URL or address of the support channel.
        description (str, optional): Additional description of the support channel.
        invitation_url (str, optional): Optional invite URL to join the channel.
    Returns:
        dict: The updated data contract dictionary with a new support channel entry.
    """
    existing_channels = data_contract.setdefault("support", [])
    for metadata in support_channel_metadata_input:
        updated_support_channel_config = {
            "channel": metadata["channel"],
            "tool": metadata["tool"],
            "scope": metadata["scope"],
            "url": metadata["url"]
        }
        if "description" in metadata: updated_support_channel_config["description"] = metadata["description"]
        if "invitation_url" in metadata: updated_support_channel_config["invitationUrl"] = metadata["invitation_url"]
        
        # Append to the list of support channels (initialize if not present)
        if json.dumps(updated_support_channel_config) not in json.dumps(existing_channels):
            data_contract.setdefault("support", []).append(updated_support_channel_config)
            print(f"appended support channel: '{updated_support_channel_config}' to data contract")
        else: print(f"already appended support channel: '{updated_support_channel_config}' to data contract")
    return data_contract
  

# Add support channels to the ODCS data contract
data_contract_odcs_yaml = update_odcs_support_channel(data_contract_odcs_yaml, support_channel_metadata_input)

# COMMAND ----------

# DBTITLE 1,Update ODCS SLA (Custom)
def update_odcs_sla_metadata(data_contract, sla_metadata_input):
    """
    Appends SLA metadata to the ODCS data contract.
    This updates SLA properties (e.g., thresholds, time limits) associated 
    with the data product.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        sla_metadata_input (list of dict): A list containing SLA metadata entries.
            Each entry can contain the following keys:
            - property (str): Name of the SLA property.
            - value (str): The value of the SLA property.
            - valueext (str, optional): Extended value information.
            - unit (str, optional): Unit of measurement.
            - element (str, optional): Associated element.
            - driver (str, optional): The driver for the SLA.
    Returns:
        dict: The updated data contract dictionary with new SLA metadata.
    """
    # Default value for slaDefaultElement
    data_contract.setdefault("slaDefaultElement", "partitionColumn")
    
    # Ensure slaProperties is a list
    existing_sla = data_contract.setdefault("slaProperties", [])

    for metadata in sla_metadata_input:
        updated_sla_metadata = {
            "property": metadata["property"],
            "value": metadata["value"]
        }
        if "valueext" in metadata: updated_sla_metadata["valueExt"] = metadata["valueext"]
        if "unit" in metadata: updated_sla_metadata["unity"] = metadata["unit"]
        if "element" in metadata: updated_sla_metadata["element"] = metadata["element"]
        if "driver" in metadata: updated_sla_metadata["driver"] = metadata["driver"]

        # Check if SLA metadata already exists
        if json.dumps(updated_sla_metadata, sort_keys=True) not in [json.dumps(s, sort_keys=True) for s in existing_sla]:
            existing_sla.append(updated_sla_metadata)
            print(f"appended sla: '{updated_sla_metadata}' to data contract")
        else:
            print(f"already appended sla: '{updated_sla_metadata}' to data contract")
    return data_contract


# Add server level agreements (SLAs) to the ODCS data contract
data_contract_odcs_yaml = update_odcs_sla_metadata(data_contract_odcs_yaml, sla_metadata_input)

# COMMAND ----------

# DBTITLE 1,Save ODCS Data Contract Locally
def save_odcs_data_contract_local(data_contract, catalog, schema, yaml_folder_path):
    """
    Saves the ODCS data contract to a local YAML file.
    This function serializes the provided data contract dictionary to a YAML-formatted string
    and writes it to a file using the pattern `<catalog>__<schema>.yaml`. If a file with the
    same name already exists, it is overwritten.
    Args:
        data_contract (dict): The ODCS data contract dictionary to save.
        catalog (str): The Unity Catalog catalog name (used in the filename).
        schema (str): The Unity Catalog schema name (used in the filename).
        yaml_folder_path (str): The path to the folder where the YAML file will be saved.
    Returns:
        str: The full file path of the saved YAML contract.
    """
    # Ensure the folder path exists
    os.makedirs(yaml_folder_path, exist_ok=True)
    # Serialize the data contract to a YAML string
    yaml_content = yaml.dump(data_contract, default_flow_style=False, sort_keys=False)
    # Define the output YAML file path
    yaml_file_path = f"{yaml_folder_path}/{catalog}__{schema}.yaml"
    # Remove existing file to ensure clean overwrite
    if os.path.exists(yaml_file_path):
        print(f"removing {yaml_file_path}")
        os.remove(yaml_file_path)
    # Write YAML content to the file
    with open(yaml_file_path, "w+") as yaml_file:
        yaml_file.write(yaml_content)
    print(f"✅ ODCS Data Contract YAML saved at: {yaml_file_path}")
    return yaml_file_path


# Save the ODCS data contract locally
yaml_file_path = save_odcs_data_contract_local(data_contract_odcs_yaml, source_catalog, source_schema, yaml_folder_path)

# COMMAND ----------

# DBTITLE 1,Lint ODCS Data Contracts
def lint_data_contract(yaml_file_path, spark):
    """
    Runs a lint syntax check on a saved ODCS data contract YAML file.
    This function loads the data contract from the specified YAML file path and 
    performs a linting process to validate its structure, completeness, and rule compliance.
    Args:
        yaml_file_path (str): Path to the saved data contract YAML file.
        spark (SparkSession): The active Spark session used by the DataContract class.

    Returns:
        dict: The result of the linting process, typically including warnings or validation messages.
    """
    # Load the contract from YAML file
    data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)
    # Run linting to validate the contract structure and rules
    test_results = data_contract.lint()
    # Print lint results for visibility
    print(f"Linting (e.g. syntax) test result: {test_results.result}")
    return test_results.result


# Lint and ODCS data contract
lint_result = lint_data_contract(yaml_file_path, spark)
