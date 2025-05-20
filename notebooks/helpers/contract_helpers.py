# Databricks notebook source
# DBTITLE 1,Import Python Helpers
# MAGIC %run "./general_helpers"

# COMMAND ----------

# MAGIC %run "./extract_helpers"

# COMMAND ----------

# DBTITLE 1,Get UC Table Query Definition (DDL)
def get_uc_table_ddl(catalog, schema, table):
    """
    Retrieves the DDL SQL statement to create a specified table or view from a Spark SQL catalog.
    Args:
        catalog (str): The catalog in which the object is located.
        schema (str): The schema in which the object is located.
        table (str): The name of the table or view.
    Returns:
        str: The DDL SQL statement that can be used to recreate the specified object.
    """
    # If the object is a view, create a temporary table from the view's schema
    try:
        if check_if_object_is_view(source_catalog, source_schema, table) == True:
            # Create an empty table from the view's schema (structure only)
            view_tbl_name = create_view_as_table(source_catalog, source_schema, table)
            # Retrieve the DDL for the newly created table
            table_ddl = spark.sql(
                f"""SHOW CREATE TABLE {catalog}.{schema}.{view_tbl_name};"""
            ).first()[0]
            table_ddl = table_ddl.replace(view_tbl_name, table)
            # Drop the temporary table (commented out)
            spark.sql(f"DROP TABLE {catalog}.{schema}.{view_tbl_name}")
        else:
            # If it's already a table, get the DDL directly
            table_ddl = spark.sql(
                f"""SHOW CREATE TABLE {catalog}.{schema}.{table};"""
            ).first()[0]
        # Return the generated DDL
        return table_ddl
    except Exception as e:
        print(f"error: {e}")
        return None


# COMMAND ----------


# DBTITLE 1,Read the Tables and Save as CSV File
def is_scalar(val):
    """
    Determines if a value is a scalar type (e.g., str, int, float, bool, None, or numpy scalar).
    Args:
        val (Any): The value to check.
    Returns:
        bool: True if the value is a scalar, False otherwise.
    """
    return isinstance(val, (str, int, float, bool, type(None), np.generic))


def serialize_complex(val):
    """
    Serializes a complex object (e.g., list, dict) into a JSON-formatted string.
    Falls back to string conversion if JSON serialization fails.
    Args:
        val (Any): The value to serialize.
    Returns:
        str: The serialized string representation of the value.
    """
    if is_scalar(val):
        return val
    try:
        return json.dumps(val)
    except Exception as e:
        print(f"Warning: Failed to serialize value of type {type(val)} - {e}")
        return str(val)


def convert_complex_type_cols_to_str(df_pandas: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all object-type columns with complex or mixed types in a DataFrame to string representations.
    Complex types are detected based on unique types present in each column.
    Args:
        df_pandas (pd.DataFrame): The input pandas DataFrame to process.

    Returns:
        pd.DataFrame: A new DataFrame with complex object columns serialized to strings.
    """
    for col in df_pandas.select_dtypes(include="object").columns:
        try:
            unique_types = df_pandas[col].dropna().map(type).unique()
            if not all(
                issubclass(t, (str, int, float, bool, type(None))) for t in unique_types
            ):
                print(
                    f"Serializing column '{col}' due to mixed or complex types: {unique_types}"
                )
                df_pandas[col] = df_pandas[col].apply(serialize_complex)
        except Exception as e:
            print(f"Error while processing column '{col}': {e}")
            df_pandas[col] = df_pandas[col].apply(
                lambda x: str(x)
            )  # fallback serialization
    return df_pandas


def create_local_data(catalog, schema, uc_tables_list, folder_path):
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
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    for table in uc_tables_list:
        file_name = f"{table}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = f"{folder_path}/{file_name}.json"

        json_schema = w.tables.get(f"{catalog}.{schema}.{table}").as_dict()
        with open(file_path, "w") as out:
            json.dump(json_schema, out, indent=4)


# COMMAND ----------


# DBTITLE 1,Extract Complex Variant Types From Data Contract Specification
def dcs_extract_variant_columns_and_physicaltypes(
    data_contract_specification: dict,
) -> dict:
    """
    Extracts all fields of type 'variant' from a Data Contract YAML structure for
    the Data Contract Specification format.  DCS = Data Contract Specification.
    Args:
        data_contract_specification_yaml (dict): The loaded YAML as a Python dictionary,
            expected to conform to the Data Contract Specification v1.1.0 format
            with a 'models' key.
    Returns:
        dict: A nested dictionary structured by table name. For each table that contains
              at least one variant column, the result includes:
              - 'table_physicaltype': The physical type of the table (e.g., 'table', 'view')
              - 'columns': A nested dictionary of column names where each entry contains:
                  - 'col_physicaltype': The physical type of the column (should be 'variant')
                  - 'col_datatype': The associated Databricks data type (from config)
    """
    result = {}
    models = yaml.safe_load(data_contract_specification).get("models", {})

    for model_name, model_def in models.items():
        table_physical_type = model_def.get("type")  # e.g., 'table'
        fields = model_def.get("fields", {})
        for field_name, field_def in fields.items():
            if field_def.get("type") == "variant":
                config = field_def.get("config", {})
                if model_name not in result:
                    result[model_name] = {
                        "table_physicaltype": table_physical_type,
                        "columns": {},
                    }
                result[model_name]["columns"][field_name] = {
                    "col_physicaltype": field_def.get("type"),
                    "col_datatype": config.get("databricksType", None),
                }
    return result


# COMMAND ----------


# DBTITLE 1,Import Complex Variant Types into ODCS Contract
def odcs_import_variant_columns_and_physicaltypes(
    data_contract_odcs: dict, variants_data: dict
) -> dict:
    """
    Replaces the physicalType of 'variant' columns in an ODCS-format Data Contract
    using the corrected variant type information.
    Args:
        data_contract_odcs (dict): The original ODCS-format Data Contract YAML
            as a Python dictionary (containing 'schema' and 'properties').
        variants_data (dict): A dictionary structured by table name with the following format:
            {
                "table_name": {
                    "table_physicaltype": str,
                    "columns": {
                        "column_name": {
                            "col_physicaltype": str,
                            "col_datatype": str
                        },
                        ...
                    }
                }
            }
    Returns:
        dict: The updated ODCS-format Data Contract with corrected physicalType
              values applied at the table and column level based on the variant mapping.
    """
    data_contract_odcs_yaml = yaml.safe_load(data_contract_odcs)
    tables = data_contract_odcs_yaml.get("schema", [])

    for table in tables:
        tbl_name = table.get("name")
        if tbl_name in variants_data:
            # Update table-level physicalType
            table["physicalType"] = variants_data[tbl_name].get(
                "table_physicaltype", table.get("physicalType")
            )

            # Update variant columns
            for column in table.get("properties", []):
                col_name = column.get("name")
                if col_name in variants_data[tbl_name].get("columns", {}):
                    col_properties = variants_data[tbl_name]["columns"][col_name]
                    column["physicalType"] = col_properties.get(
                        "col_datatype", column.get("physicalType")
                    )
    return data_contract_odcs_yaml


# COMMAND ----------


# DBTITLE 1,Generate ODCS Base Contract
def generate_odcs_base_contract(data_contract):
    """
    Converts a unified DataContract object into an ODCS-compatible YAML dictionary.
    This function takes a DataContract object, serializes it to YAML, reinitializes it,
    and then exports it in the "odcs" format. The final result is parsed into a Python
    dictionary for further use (e.g., YAML file creation or API submission).
    DCS = Data Contract Specification
    Args:
        data_contract (DataContract): The unified DataContract object containing multiple models.
    Returns:
        dict: A dictionary representing the ODCS-compatible data contract in YAML format.
    """
    # Serialize the DataContract object to a YAML string
    data_contract_yaml = data_contract.to_yaml()

    # Get Data Contract Specification column complex variant types (e.g. nested json)
    data_contract_yaml_variants_dict = dcs_extract_variant_columns_and_physicaltypes(
        data_contract_yaml
    )
    data_contract_yaml = data_contract_yaml.replace(
        "type: variant", "type: struct"
    )  # variant is not support in the Data Contract CLI

    # Reinitialize a DataContract object using the YAML string and export to "odcs" format
    data_contract_odcs = DataContract(
        data_contract_str=data_contract_yaml, spark=spark
    ).export("odcs")

    # Timestamps get store as 'timestamp_ntz' by the Data Contract CLI in the ODCS data contract
    # data_contract_odcs_yaml = data_contract_odcs.replace(
    #     "physicalType: timestamp_ntz", "physicalType: timestamp"
    # )

    # Deploy column complex variant types (e.g. nested json)
    data_contract_odcs_yaml = odcs_import_variant_columns_and_physicaltypes(
        data_contract_odcs, data_contract_yaml_variants_dict
    )

    return data_contract_odcs_yaml


# COMMAND ----------


# DBTITLE 1,Create Data Contracts For Each Table and Combine
def combine_data_contract_models(catalog, schema, uc_tables_dict, folder_path):
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

    # Get table and column level tags
    # get_data_contract_table_tags() and get_data_contract_column_tags Python functiona are in the helpers notebook
    try:
        tbl_tags = tag_dict_to_list(
            get_existing_uc_tags(
                input_catalog=catalog, input_schema=schema, tag_level="table"
            )
        )
        col_tags = tag_dict_to_list(
            get_existing_uc_tags(
                input_catalog=catalog,
                input_schema=schema,
                tag_level="column",
            )
        )
    except Exception as e:
        tbl_tags = col_tags = None
        print("unable to get table and column level tags")
        print(e)

    for table, table_desc in uc_tables_dict.items():
        try:
            # Try to import data contract model from file; skip if the file doesn't exist or is empty (e.g. empty table)
            source = f"{folder_path}/{table}.json"
            print(f"\n✅START✅ --> reading local table: {source} <-- ✅START✅")
            data_contracts_table = yaml.safe_load(
                DataContract(
                    data_contract=data_contract_obj.import_from_source(
                        "unity", f"{source}"
                    )
                ).export("odcs")
            )
        except Exception as e:
            print(str(e))
            continue

        schema_obj = data_contracts_table["schema"][0]  # Hold constant per table

        if tbl_tags != None:
            schema_obj["tags"] = tbl_tags.get(f"{catalog}.{schema}.{table}")
        else:
            schema_obj["tags"] = []

        for col in schema_obj["properties"]:
            if col_tags != None:
                col["tags"] = col_tags.get(f"{catalog}.{schema}.{table}.{col['name']}")
            else:
                col["tags"] = []

        # data_contracts_table = replace_none_with_empty_string_in_json(
        #     data_contracts_table
        # )
        data_contracts_dict[table] = data_contracts_table

        if counter == 0:
            data_contracts_table_first = data_contracts_table
        else:
            # Merge models into the first data contract object
            data_contracts_table_first["schema"].extend(data_contracts_table["schema"])

        counter += 1

    return data_contracts_table_first, data_contracts_dict


# COMMAND ----------


# DBTITLE 1,Define Generic Data Quality Rules For All Tables (Custom)
def get_general_data_quality_rules(table, columns=None):
    """
    Generates a generic set of data quality SQL rules for a given data contract.

    These rules include:
    1. A row count check to ensure the table contains data.
    2. A uniqueness check across specified columns to ensure no duplicate rows (commented out).
    Args:
        table (str): The name of the table for which to create rules.
        columns (list, optional): List of column names to use for the duplicate check.
                                  If None or empty, the duplicate rule will be skipped or invalid.
    Returns:
        list: A list of data quality rule dictionaries formatted for use in a data contract.
    """
    # partition_by_clause = ", ".join(columns) if columns else ""

    general_data_quality_rules = {
        f"{table}": {
            "quality": [
                {
                    "type": "sql",
                    "description": f"Ensures '{table}' table has data",
                    "query": f"SELECT COUNT(*) FROM {table}",
                    "dimension": "completeness",
                    "mustBeGreaterThan": 0,
                }
            ]
        }
    }

    # Add duplicate check only if valid columns are provided
    # if partition_by_clause:
    #     general_data_quality_rules[table]["quality"].append(
    #         {
    #             "type": "sql",
    #             "description": f"Ensure '{table}' table has no duplicate rows across all columns",
    #             "query": f"""
    #                 SELECT COUNT(*)
    #                 FROM (
    #                     SELECT *, COUNT(*) OVER (PARTITION BY {partition_by_clause}) AS row_count
    #                     FROM {table}
    #                 ) AS subquery
    #                 WHERE row_count > 1
    #             """,
    #             "mustBe": 0
    #         }
    #     )

    # Clean up SQL formatting (flatten multi-line SQL to single-line strings)
    for dq_rule in general_data_quality_rules[table]["quality"]:
        dq_rule["query"] = " ".join(dq_rule["query"].split())
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
            if rule_table == table_name:  # Then updates rules
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
    if custom_dq_rules_input != None:
        for table in data_contract["schema"]:
            if "quality" not in table:
                table["quality"] = []

            # Get column names for the table to generate general DQ rules
            # try:
            #     cols = spark.read.table(f"{catalog}.{schema}.{table['name']}").columns
            # except Exception as e:
            #     print(f"Exception: {e}")
            #     continue
            cols = None
            general_dq_sql_rules = get_general_data_quality_rules(table["name"], cols)

            # Attempt to get custom rules; fall back to general only if not found
            try:
                custom_dq_sql_rules = get_custom_data_quality_rules(
                    table["name"], custom_dq_rules_input
                )
                dq_rule_type = "generic and custom"
            except Exception as e:
                custom_dq_sql_rules = []
                dq_rule_type = "generic"

            # Prevent adding duplicate rules by comparing JSON string representations
            existing_rules = table["quality"]
            set_existing_rules = set(
                json.dumps(d, sort_keys=True) for d in existing_rules
            )
            set_general_dq_sql_rules = set(
                json.dumps(d, sort_keys=True) for d in general_dq_sql_rules
            )
            set_custom_dq_sql_rules = set(
                json.dumps(d, sort_keys=True) for d in custom_dq_sql_rules
            )

            # Check if rules are already applied
            if set_general_dq_sql_rules.issubset(
                set_existing_rules
            ) and set_custom_dq_sql_rules.issubset(set_existing_rules):
                print(
                    f"already appended '{dq_rule_type}' dq rules to table {table['name']} in ODCS data contract"
                )
            else:
                table["quality"].extend(general_dq_sql_rules)
                table["quality"].extend(custom_dq_sql_rules)
                print(
                    f"appended '{dq_rule_type}' dq rules to table {table['name']} in ODCS data contract"
                )
    return data_contract


# COMMAND ----------


# DBTITLE 1,Update ODCS Metadata (Custom)
def update_odcs_contract_metadata(
    data_contract, contract_metadata_input, catalog, schema
):
    """
    Updates the top-level metadata fields of an ODCS data contract.
    This function sets core metadata for the contract such as name, version, domain, status,
    data product, tenant, and description. It also attempts to auto-populate tags from Unity Catalog;
    if that fails, it falls back to user-provided tags.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        contract_metadata_input (list): A list containing metadata dictionaries, each with the following keys:
            - name (str): Name of the contract.
            - version (str): Version string for the contract (e.g., "1.0.0").
            - status (str): Status of the contract (e.g., "active", "inactive").
            - domain (str): The business domain the data contract belongs to.
            - dataproduct (str): The data product this contract describes.
            - tenant (str): The associated airline or business tenant.
            - description (str): High-level description including purpose, limitations, and usage.
            - tags (list): List of tags (as strings) related to the contract.
        catalog (str): Unity Catalog catalog name used for fetching tags.
        schema (str): Unity Catalog schema name used for fetching tags.
    Returns:
        dict: The updated data contract dictionary with metadata fields populated.
    """
    if contract_metadata_input != None:
        for metadata in contract_metadata_input:
            data_contract["name"] = metadata.get("name")
            data_contract.setdefault("apiVersion", metadata.get("apiversion"))
            data_contract.setdefault("kind", metadata.get("kind"))
            data_contract.setdefault("id", metadata.get("id"))
            data_contract["version"] = metadata.get("version")
            data_contract["domain"] = metadata.get("domain")
            data_contract["status"] = metadata.get("status")
            data_contract["dataProduct"] = metadata.get("dataproduct")
            data_contract["tenant"] = metadata.get("tenant")
            data_contract["description"] = metadata.get("description")
            data_contract["authored_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            print(f"appended general metadata to ODCS data contract: {metadata}")
    return data_contract


# COMMAND ----------


# DBTITLE 1,Update ODCS Server Configuration (Custom)
def update_odcs_server_config(
    data_contract, server_metadata_input, catalog=None, schema=None
):
    """
    Updates the server configuration block in an ODCS data contract.
    This function populates the "servers" section of the contract with details such as server name,
    connection type, host URL, Unity Catalog catalog, and schema. If `catalog` or `schema` are not
    provided as arguments, they are taken from the server metadata input.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        server_metadata_input (list): A list containing a single dictionary with the following keys:
            - server (str): The name of the target environment (e.g., "development", "production").
            - type (str): Type of connection/server (e.g., "databricks").
            - host (str): The Databricks workspace URL.
            - catalog (str): Unity Catalog catalog name.
            - schema (str): Unity Catalog schema name.
        catalog (str, optional): Optional override for Unity Catalog catalog.
        schema (str, optional): Optional override for Unity Catalog schema.
    Returns:
        dict: The updated data contract dictionary with the server configuration populated.
    """
    if server_metadata_input != None:
        for metadata in server_metadata_input:
            # Use catalog/schema from metadata if not provided as parameters
            resolved_catalog = catalog if catalog is not None else metadata["catalog"]
            resolved_schema = schema if schema is not None else metadata["schema"]

            updated_server_config = {
                "server": metadata["server"],
                "type": metadata["type"],
                "host": metadata["host"],
                "catalog": resolved_catalog,
                "schema": resolved_schema,
            }

            # Only one server config is expected; override any existing entry
            data_contract["servers"] = [updated_server_config]
            print(
                f"appended servers section to ODCS data contract: {data_contract['servers']}"
            )
    return data_contract


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
        channel (str): The name or label of the support channel (e.g., "Test Teams Channel").
        tool (str): The communication tool used (e.g., "teams", "email").
        scope (str): The type of support channel (e.g., "interactive", "announcements").
        url (str): The URL or address of the support channel.
        description (str, optional): Additional description of the support channel.
        invitation_url (str, optional): Optional invite URL to join the channel.
    Returns:
        dict: The updated data contract dictionary with a new support channel entry.
    """
    if support_channel_metadata_input != None:
        existing_channels = data_contract.setdefault("support", [])
        for metadata in support_channel_metadata_input:
            updated_support_channel_config = {
                "channel": metadata["channel"],
                "tool": metadata["tool"],
                "scope": metadata["scope"],
                "url": metadata["url"],
            }
            if "description" in metadata:
                updated_support_channel_config["description"] = metadata["description"]
            if "invitation_url" in metadata:
                updated_support_channel_config["invitationUrl"] = metadata[
                    "invitation_url"
                ]

            # Append to the list of support channels (initialize if not present)
            if json.dumps(updated_support_channel_config) not in json.dumps(
                existing_channels
            ):
                data_contract.setdefault("support", []).append(
                    updated_support_channel_config
                )
                print(
                    f"appended support channel to ODCS data contract: '{updated_support_channel_config}' to data contract"
                )
            else:
                print(
                    f"already appended support channel to ODCS data contract: '{updated_support_channel_config}' to data contract"
                )
    return data_contract


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
    if sla_metadata_input != None:
        # Default value for slaDefaultElement
        data_contract.setdefault("slaDefaultElement", "partitionColumn")

        # Ensure slaProperties is a list
        existing_sla = data_contract.setdefault("slaProperties", [])

        for metadata in sla_metadata_input:
            updated_sla_metadata = {
                "property": metadata["property"],
                "value": metadata["value"],
            }
            if "valueext" in metadata:
                updated_sla_metadata["valueExt"] = metadata["valueext"]
            if "unit" in metadata:
                updated_sla_metadata["unity"] = metadata["unit"]
            if "element" in metadata:
                updated_sla_metadata["element"] = metadata["element"]
            if "driver" in metadata:
                updated_sla_metadata["driver"] = metadata["driver"]

            # Check if SLA metadata already exists
            if json.dumps(updated_sla_metadata, sort_keys=True) not in [
                json.dumps(s, sort_keys=True) for s in existing_sla
            ]:
                existing_sla.append(updated_sla_metadata)
                print(
                    f"appended sla to ODCS data contract: '{updated_sla_metadata}' to data contract"
                )
            else:
                print(
                    f"already appended sla to ODCS data contract: '{updated_sla_metadata}' to data contract"
                )
    return data_contract


# COMMAND ----------


# DBTITLE 1,Update ODCS Team (Custom)
def update_odcs_team_metadata(data_contract, team_metadata_input):
    """
    Appends Team metadata to the ODCS data contract.
    This updates Team properties (e.g., usernames, roles) associated
    with the data product.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        team_metadata_input (list of dict): A list containing Team metadata entries.
            Each entry can contain the following keys:
            - name (str): user full name.
            - username (str): user name.
            - role (str): user role.
            - datein (str): access granted date.
            - dateout (str): revoked access date.
            - replacebyusername (srt): replace by username.
            - comment (str): comments.
    Returns:
        dict: The updated data contract dictionary with new Team metadata.
    """
    if team_metadata_input != None:
        # Ensure Team is a list
        existing_team = data_contract.setdefault("team", [])

        for metadata in team_metadata_input:
            updated_team_metadata = {
                "username": metadata["username"],
                "role": metadata["role"],
                "dateIn": metadata["dateIn"],
            }
            if "dateOut" in metadata:
                updated_team_metadata["dateOut"] = metadata["dateOut"]
            if "replacedByUsername" in metadata:
                updated_team_metadata["replacedByUsername"] = metadata[
                    "replacedByUsername"
                ]
            if "comment" in metadata:
                updated_team_metadata["comment"] = metadata["comment"]
            if "name" in metadata:
                updated_team_metadata["name"] = metadata["name"]

            # Check if Team metadata already exists
            if json.dumps(updated_team_metadata, sort_keys=True) not in [
                json.dumps(s, sort_keys=True) for s in existing_team
            ]:
                existing_team.append(updated_team_metadata)
                print(
                    f"appended team to ODCS data contract: '{updated_team_metadata}' to data contract"
                )
            else:
                print(
                    f"already appended team to ODCS data contract: '{updated_team_metadata}' to data contract"
                )
    return data_contract


# COMMAND ----------


# DBTITLE 1,Update ODCS Roles (Custom)
def update_odcs_roles_metadata(data_contract, roles_metadata_input):
    """
    Appends Roles metadata to the ODCS data contract.
    This updates Roles properties (e.g. role, access level) associated
    with the data product.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        roles_metadata_input (list of dict): A list containing Roles metadata entries.
            Each entry can contain the following keys:
            - role (str): user name.
            - access (str): role access.
            - firstlevelapprovers (str): first level approver.
            - secondlevelapprovers (str): second level approver.
    Returns:
        dict: The updated data contract dictionary with new Roles metadata.
    """
    if roles_metadata_input != None:
        # Ensure Roles is a list
        existing_roles = data_contract.setdefault("roles", [])

        for metadata in roles_metadata_input:
            updated_roles_metadata = {
                "role": metadata["role"],
                "access": metadata["access"],
                "firstlevelApprovers": metadata["firstlevelApprovers"],
                "secondlevelApprovers": metadata["firstlevelApprovers"],
            }

            # Check if Roles metadata already exists
            if json.dumps(updated_roles_metadata, sort_keys=True) not in [
                json.dumps(s, sort_keys=True) for s in existing_roles
            ]:
                existing_roles.append(updated_roles_metadata)
                print(
                    f"appended roles to ODCS data contract: '{updated_roles_metadata}' to data contract"
                )
            else:
                print(
                    f"already appended roles to ODCS data contract: '{updated_roles_metadata}' to data contract"
                )
    return data_contract


# COMMAND ----------


# DBTITLE 1,Update ODCS Pricing (Custom)
def update_odcs_pricing_metadata(data_contract, pricing_metadata_input):
    """
    Appends Pricing metadata to the ODCS data contract.
    This updates Pricing properties (e.g. priceamount, pricecurrency) associated
    with the data product.
    Args:
        data_contract (dict): The ODCS data contract dictionary to update.
        pricing_metadata_input (dict): A dict containing the Pricing metadata entry.
            The entry can contain the following keys:
            - priceamount (float): price amount.
            - pricecurrencyt (str): price unit (e.g. megabytes).
            - priceunit (str): pricing unit.
    Returns:
        dict: The updated data contract dictionary with new Pricing metadata.
    """
    if pricing_metadata_input != None:
        # Ensure Pricing is a list
        existing_pricing = data_contract.setdefault("price", {})

        updated_pricing_metadata = {
            "priceAmount": pricing_metadata_input.get("priceAmount"),
            "priceCurrency": pricing_metadata_input.get("priceCurrency"),
            "priceUnit": pricing_metadata_input.get("priceUnit"),
        }

        # Check if Pricing metadata already exists
        if json.dumps(updated_pricing_metadata, sort_keys=True) != json.dumps(
            existing_pricing, sort_keys=True
        ):
            data_contract["price"] = updated_pricing_metadata
            print(
                f"updated pricing to ODCS data contract: '{updated_pricing_metadata}'"
            )
        else:
            print(
                f"already updated pricing to ODCS data contract: '{updated_pricing_metadata}' to data contract"
            )
    return data_contract


# COMMAND ----------


# DBTITLE 1,Save ODCS Data Contract Locally
def save_odcs_data_contract_local(
    data_contract, catalog, schema, base_yaml_folder_path
):
    """
    Saves the ODCS data contract locally in both current_versions and historical_versions folders.

    Args:
        data_contract (dict): The ODCS data contract dictionary to save.
        catalog (str): The Unity Catalog catalog name (used in the filename).
        schema (str): The Unity Catalog schema name (used in the filename).
        base_yaml_folder_path (str): Base path where the folders will be created.

    Returns:
        dict: A dictionary with keys 'current_version_path' and 'historical_version_path'.
    """
    # Get a timestamp for historical contract version
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Define paths
    current_version_path = os.path.join(base_yaml_folder_path, f"catalog={catalog}")
    historical_versions_path = os.path.join(
        base_yaml_folder_path, f"catalog={catalog}", "versions"
    )

    os.makedirs(current_version_path, exist_ok=True)
    os.makedirs(historical_versions_path, exist_ok=True)

    # File paths
    current_version_file = os.path.join(
        current_version_path, f"{catalog}__{schema}.yaml"
    )
    historical_version_file = os.path.join(
        historical_versions_path, f"{catalog}__{schema}__{timestamp}.yaml"
    )

    # Serialize data contract
    yaml_content = yaml.dump(data_contract, default_flow_style=False, sort_keys=False)

    # Copy existing contract current version into the historical_versions folder if it exists
    # Then remove the existing contract current version
    if os.path.exists(current_version_file):
        print(
            f"moving '{current_version_file}' to versions folder named as '{historical_version_file}'"
        )
        shutil.copy2(current_version_file, historical_version_file)
        os.remove(current_version_file)

    # Save current version (overwrite clean)
    with open(current_version_file, "w+") as f:
        f.write(yaml_content)
    print(f"✅ Current version saved at: {current_version_file}")

    # Return the data contract current_version data file path
    return current_version_file

def remove_databricks_config(obj):
  """Removes the config attribute from each field in the data contract model specification so that the cli
  type converter will function properly"""
  if isinstance(obj, dict):
    return {k: remove_databricks_config(v) if k != 'config' else None for k, v in obj.items()}
  elif isinstance(obj, list):
    return [remove_databricks_config(item) for item in obj]
  elif hasattr(obj, '__dict__'):
    obj.__dict__ = {k: remove_databricks_config(v) if k != 'config' else None for k, v in obj.__dict__.items()}
    return obj
  else:
    return obj

def validate_data_contract(data_contract_dict: dict, spark: SparkSession):
    # Load the contract from YAML file
    data_contract = DataContract(
        data_contract_str=json.dumps(data_contract_dict), spark=spark
    )
    # Run linting to validate the contract structure and rules
    test_results = data_contract.lint()
    print(f"Linting (e.g. syntax) test result: {test_results.result}")

    results = []
    for check in test_results.checks:
        if str(check.result) != "ResultEnum.passed" and check.name not in [
            "Linter 'Objects have descriptions'",
            "Linter 'Fields use valid constraints'",
        ]:
            results.append(
                {
                    "reason": check.reason,
                    "result": str(check.result),
                    "type": check.type,
                    "name": check.name,
                }
            )

    if results:
        spark.createDataFrame(results).display()

    error_types = set()
    error_details = []
    warning_count = 0
    error_count = 0

    # Validate the sql queries are valid using sqlglot
    data_contract_spec = data_contract.get_data_contract_specification()
    data_contract_spec = remove_databricks_config(data_contract_spec)

    data_contract = DataContract(data_contract=data_contract_spec)
    queries_ddl_list = data_contract.export("sql", dialect="databricks")[:-1].split(";")
    for query in queries_ddl_list:
        try:
            spark.sql(f"EXPLAIN {query}")
        except Exception as e:
            error_type = "invalid_sql"
            error_types.add(error_type)
            error_count += 1
            error_details.append(
                {
                    "error_location": "sql",
                    "error_severity": "error",
                    "error_type": error_type,
                    "error_message": str(e),
                    "error_input": query,
                    "table_name": None,
                    "column_name": None,
                }
            )

    warning_error_types = ["too_short", "string_too_short"]

    try:
        DataContractMetadata(**data_contract_dict)
    except ValidationError as e:
        for error in e.errors():
            error_types.add(error.get("type"))
            location = error.get("loc", [])
            table_name = None
            column_name = None
            warning_count += 1 if error.get("type") in warning_error_types else 0
            error_count += 1 if error.get("type") not in warning_error_types else 0
            if len(location) > 1 and location[0] == "schema":
                table_name = data_contract_dict.get("schema", [])[location[1]].get(
                    "name", ""
                )
                column_name = (
                    data_contract_dict.get("schema", [])[location[1]]
                    .get("properties", {})[location[3]]
                    .get("name", "")
                    if location[2] == "properties"
                    else None
                )
            error_details.append(
                {
                    "error_location": ".".join(map(str, error.get("loc", []))),
                    "error_severity": "warning"
                    if error.get("type") in warning_error_types
                    else "error",
                    "error_type": error.get("type", ""),
                    "error_message": error.get("msg", ""),
                    "error_input": error.get("input", ""),
                    "table_name": table_name,
                    "column_name": column_name,
                }
            )
        schema = StructType(
            [
                StructField("error_location", StringType(), True),
                StructField("error_severity", StringType(), True),
                StructField("error_type", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("error_input", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("column_name", StringType(), True),
            ]
        )
        print("Error details:")
        spark.createDataFrame(error_details, schema=schema).display()
        print(f"Error types: {error_types}")

    if not error_types.issubset(warning_error_types):
        raise PydanticCustomError(
            "validation_error",
            "Data contract validation failed with {error_count} errors and {warning_count} warnings: {error_types}",
            {
                "error_count": error_count,
                "warning_count": warning_count,
                "error_types": list(error_types),
            },
        )
