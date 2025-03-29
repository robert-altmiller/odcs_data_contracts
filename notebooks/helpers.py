# Databricks notebook source
# DBTITLE 1,Library Imports
import json

# COMMAND ----------

# DBTITLE 1,Check if Running in a Databricks Job
def get_job_context():
    """Retrieve job-related context from the current Databricks notebook."""
    # Retrieve the notebook context
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    # Convert the context to a JSON string
    ctx_json = ctx.toJson()
    # Parse the JSON string into a Python dictionary
    ctx_dict = json.loads(ctx_json)
    # Access the 'tags' dictionary
    tags_dict = ctx_dict.get('tags', {})
    # Filter for keys containing 'job' or 'run'
    job_context = {k: v for k, v in tags_dict.items() if 'job' in k.lower() or 'run' in k.lower()}
    return ctx_json


def is_running_in_databricks_workflow():
    """Detect if running inside a Databricks Workflow job."""
    job_context = get_job_context()
    return 'jobName' in job_context

# Unit test
# print(f"is_running_in_databricks_workflow: {is_running_in_databricks_workflow()}")

# COMMAND ----------

# DBTITLE 1,List All the Tables in a Catalog.Schema (Works with Single Node and Shared Clusters)
def list_tables_in_schema(catalog: str, schema: str) -> tuple[list, dict]:
    """
    Returns a list of all table names in the specified catalog and schema.
    Parameters:
    - catalog (str): The catalog name (e.g., 'hive_metastore' or 'your_catalog_name').
    - schema (str): The schema/database name within the catalog.
    Returns:
    - list: A list of table names as strings or a dictonary of table names and table descriptions.
    """
    tables_list = []
    tables_dict = {}

    try:
        full_schema = f"{catalog}.{schema}"
        tables_df = spark.sql(f"SHOW TABLES IN {full_schema}")
        table_names = [row.tableName for row in tables_df.collect()]

        for table_name in table_names:
            tables_list.append(table_name)

            # Get description for each table
            desc_df = spark.sql(f"DESCRIBE TABLE EXTENDED {full_schema}.{table_name}")
            desc_dict = {row.col_name.strip(): row.data_type for row in desc_df.collect() if row.col_name and row.col_name.strip()}

            # Extract comment from extended description
            comment = desc_dict.get("Comment", "")
            tables_dict[table_name] = comment

        return tables_list, tables_dict

    except Exception as e:
        print(f"Error listing tables in {catalog}.{schema}: {e}")
        return None, None

# Unit Test
# tables_list, tables_with_desc_dict = list_tables_in_schema("hive_metastore", "default")

# COMMAND ----------

# DBTITLE 1,List All the Tables in a Catalog.Schema (Old - Does Not Work With Shared Cluster)
# def list_tables_in_schema(catalog: str, schema: str) -> list:
#     """
#     Returns a list of all table names in the specified catalog and schema.
#     Parameters:
#     - catalog (str): The catalog name (e.g., 'hive_metastore' or 'your_catalog_name').
#     - schema (str): The schema/database name within the catalog.
#     Returns:
#     - list: A list of table names as strings.
#     """
#     tables_list = []
#     tables_dict = {}
#     try:
#         full_schema = f"{catalog}.{schema}"
#         tables = spark.catalog.listTables(full_schema)
#         for table in tables:
#             tables_list.append(table.name)
#             tables_dict[table.name] = table.description
#         return tables_list, tables_dict
#     except Exception as e:
#         print(f"Error listing tables in {catalog}.{schema}: {e}")
#         return None, None


# # Unit test
# tables_list, tables_with_desc_dict = list_tables_in_schema("hive_metastore", "default")

# COMMAND ----------

# DBTITLE 1,Get Column Comments
def get_column_comments(catalog: str, schema: str, table: str) -> dict:
    """
    Returns a dictionary mapping column names to comments for a given table.
    Parameters:
    - catalog (str): Catalog name
    - schema (str): Schema name
    - table (str): Table name
    Returns:
    - dict: { "<catalog>.<schema>.<table>": { column_name: comment, ... } }
    """
    try:
        full_table = f"{catalog}.{schema}.{table}"
        column_metadata_dict = {}
        columns_list = spark.sql(f"DESCRIBE TABLE {full_table}").collect()
        
        for row in columns_list:
            # Only include actual columns (skip metadata rows)
            if row.col_name and not row.col_name.startswith("#"):
                column_metadata_dict[row.col_name] = row.comment or ""
        
        return {full_table: column_metadata_dict}
    
    except Exception as e:
        print(f"Error getting column comments for {catalog}.{schema}.{table}: {e}")
        return {}


# Unit test
# result = get_column_comments("hive_metastore", "default", "customer")
# print(result)

# COMMAND ----------

# DBTITLE 1,Infer Avro Schema
def infer_avro_schema(df):
    """
    Infers an Avro schema from a given pandas DataFrame based on data types of the DataFrame's columns.
    Args:
        df (pandas.DataFrame): The DataFrame for which to infer the schema.
    Returns:
        dict: A dictionary representing the Avro schema with types mapped from DataFrame dtypes.
    """
    avro_types = {
        "int64": "long",
        "int32": "int",
        "float64": "double",
        "float32": "float",
        "object": "string",
        "bool": "boolean",
        "date": "string"
    }
    fields = [{"name": col, "type": avro_types.get(str(df[col].dtype), "string")} for col in df.columns]
    schema = {
        "type": "record",
        "name": "AutoGeneratedSchema",
        "fields": fields
    }
    return schema

# COMMAND ----------

# DBTITLE 1,Read Input JSON File
def read_json_file(file_path: str = None, json_str: str = None):
    """
    Reads a JSON file and returns the data as a Python object.
    Parameters:
        file_path (str): Path to the JSON file.
    Returns:
        dict or list: Parsed JSON data.
    """
    try:
        if file_path != None:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        elif json_str != None:
                data = json.loads(json_str)
        return data
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON - {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

# Unit test
# a = read_json_file(file_path = "A")
# b = read_json_file(json_str = "B")
# c = read_json_file(json_str = json.dumps([{'a': 'b'}]))
# print(c)

# COMMAND ----------

# DBTITLE 1,Replace None With Empty Strings in JSON
def replace_none_with_empty_string_in_json(json_obj):
    """
    Recursively traverses a nested dictionary or list and replaces all None values
    with empty strings ("").
    Args:
        json_obj (Any): The input object, typically a dictionary or list that may contain None values.
    Returns:
        Any: The same structure with all None values replaced by empty strings.
             Lists and dictionaries are preserved recursively.
    """
    if isinstance(json_obj, dict):
        return {k: replace_none_with_empty_string_in_json(v) for k, v in json_obj.items()}
    elif isinstance(json_obj, list):
        return [replace_none_with_empty_string_in_json(i) for i in json_obj]
    elif json_obj is None:
        return ""
    else:
        return json_obj

# COMMAND ----------

# DBTITLE 1,Get Table Columns
def get_columns_in_table(catalog: str, schema: str, table: str) -> list:
    """
    Returns the list of column names in a specified Unity Catalog table.
    Args:
        catalog (str): The catalog name.
        schema (str): The schema (database) name.
        table (str): The table name.
    Returns:
        list: A list of column names as strings.
    """
    return spark.read.table(f"{catalog}.{schema}.{table}").columns


# COMMAND ----------

# DBTITLE 1,Get UC Tags From Information_Schema
def get_existing_uc_tags(input_catalog: str = None, input_schema: str = None, input_table: str = None, input_column: str = None,
                      catalog: str = "system", schema: str = "information_schema", tag_level: str = "catalog") -> dict:
    """
    Retrieve existing Unity Catalog tags for a given catalog, schema, table, or column level.
    Args:
        input_catalog (str): Catalog name (required for all levels).
        input_schema (str): Schema name (required for schema, table, column levels).
        input_table (str): Table name (required for table, column levels).
        input_column (str): Column name (required for column level).
        catalog (str): Catalog where the *_tags metadata tables are stored.
        schema (str): Schema where the *_tags metadata tables are stored.
        tag_level (str): One of ['catalog', 'schema', 'table', 'column'].
    Returns:
        dict: A dictionary of {tag_name: tag_value} for the specified object.
    Raises:
        ValueError: If an invalid tag_level is provided.
    """

    if tag_level not in ["catalog", "schema", "table", "column"]:
        raise ValueError(f"Invalid tag_level: {tag_level}. Must be one of ['catalog', 'schema', 'table', 'column'].")

    table_name = f"{catalog}.{schema}.{tag_level}_tags"

    where_clauses = []
    if input_catalog is not None:
        where_clauses.append(f"catalog_name = '{input_catalog}'")
    if tag_level in ["schema", "table", "column"] and input_schema is not None:
        where_clauses.append(f"schema_name = '{input_schema}'")
    if tag_level in ["table", "column"] and input_table is not None:
        where_clauses.append(f"table_name = '{input_table}'")
    if tag_level == "column" and input_column is not None:
        where_clauses.append(f"column_name = '{input_column}'")

    where_clause = " AND ".join(where_clauses)
    sql_query = f"SELECT * FROM {table_name} WHERE {where_clause}"

    print(f"Running SQL: {sql_query}\n")
    records = spark.sql(sql_query).collect()

    # Build the identifier key (like catalog.schema.table.column)
    identifier_parts = [input_catalog]
    if input_schema: identifier_parts.append(input_schema)
    if input_table: identifier_parts.append(input_table)
    if input_column: identifier_parts.append(input_column)
    key = ".".join(identifier_parts)

    existing_tags = {key: {row.tag_name: row.tag_value for row in records}}
    return existing_tags


# Unit test
# catalog_tags = get_existing_tags(input_catalog = "sales_db", tag_level = "catalog")
# print(f"catalog_tags: {catalog_tags}\n")
# schema_tags = get_existing_tags(input_catalog = "sales_db", input_schema = "public", tag_level = "schema")
# print(f"schema_tags: {schema_tags}\n")
# table_tags = get_existing_tags(input_catalog = "sales_db", input_schema = "public", input_table = "customers", tag_level = "table")
# print(f"table_tags: {table_tags}\n")
# column_tags = get_existing_tags(input_catalog = "sales_db", input_schema = "public", input_table = "customers", tag_level = "column", input_column = "email")
# print(f"column_tags: {column_tags}")

# COMMAND ----------

# DBTITLE 1,Format UC Tags For Data Contract
def tag_dict_to_list(input_list: list) -> dict:
    """
    Converts a list of dicts in the format:
    [{'catalog.schema.table.column': {'tag1': 'value1'}}]
    Into:
    {'tags': {'column_name': ['tag1:value1', ...]}}
    """
    tags_by_column = {}

    for tag_entry in input_list:
        for fq_name, tag_dict in tag_entry.items():
            # Extract the column name from the full FQN
            column_name = fq_name.split(".")[-1]
            tags = [f"{k}:{v}" for k, v in tag_dict.items()]
            tags_by_column[column_name] = tags
    return {"tags": tags_by_column}


def get_data_contract_tags(catalog: str, schema: str = None, table: str = None) -> list:
    """
    Retrieves tags from Unity Catalog for a given catalog, schema, or table level,
    based on which arguments are provided. Returns tags in a format suitable for
    data contract generation.

    Args:
        catalog (str): The catalog name (required).
        schema (str, optional): The schema name (optional, required for table-level tags).
        table (str, optional): The table name (optional, only used for table-level tags).
    Returns:
        list: A single-item list containing a dictionary of tags.
              Example: [{'tag_name': 'tag_value', ...}]
    Raises:
        ValueError: If invalid parameter combinations are passed.
    """
    if catalog and not schema and not table:
        # Catalog-level
        tags = get_existing_uc_tags(input_catalog=catalog, tag_level="catalog")
    elif catalog and schema and not table:
        # Schema-level
        tags = get_existing_uc_tags(input_catalog=catalog, input_schema=schema, tag_level="schema")
    elif catalog and schema and table:
        # Table-level
        tags = get_existing_uc_tags(input_catalog=catalog, input_schema=schema, input_table=table, tag_level="table")
    else:
        raise ValueError("Invalid arguments: must provide either (catalog), (catalog, schema), or (catalog, schema, table).")

    return [tags]



def get_data_contract_column_tags(catalog: str, schema: str, table: str) -> list:
    """
    Retrieves column-level tags from Unity Catalog for all columns in a given table,
    and returns them as a list of tag dictionaries keyed by fully-qualified column name.
    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.
        table (str): The table name.
    Returns:
        list: A list of dictionaries, each containing column-level tags for a column.
              Example: [{'catalog.schema.table.column': {'tag_name': 'tag_value'}}]
    """
    col_tags_list = []
    table_cols = get_columns_in_table(catalog, schema, table)
    for col in table_cols:
        column_tags = get_existing_uc_tags(input_catalog=catalog, input_schema=schema, input_table=table, input_column=col, tag_level="column")
        col_tags_list.append(column_tags)
    return col_tags_list


# Unit test
# catalog_tags = tag_dict_to_list(get_data_contract_tags("uc_demos_charles_chen"))
# print(f"catalog_tags: {catalog_tags}")
# schema_tags = tag_dict_to_list(get_data_contract_tags("uc_demos_charles_chen", "system"))
# print(f"schema_tags: {schema_tags}")
# table_tags = tag_dict_to_list(get_data_contract_tags("uc_demos_charles_chen", "system", "customers"))
# print(f"table_tags: {table_tags}")
# col_tags = tag_dict_to_list(get_data_contract_column_tags("uc_demos_charles_chen", "system", "customers"))
# print(f"col_tags: {col_tags}")

# COMMAND ----------

# DBTITLE 1,Deploy Tags to UC Tables
def apply_uc_tags(level: str, fully_qualified_name: str, tags: dict):
    """
    Apply Unity Catalog tags to a catalog, schema, table, or column.
    Automatically creates tags if they do not exist.
    Args:
        level (str): One of 'catalog', 'schema', 'table', or 'column'.
        fully_qualified_name (str): Dot-delimited name depending on tag level:
            - catalog
            - catalog.schema
            - catalog.schema.table
            - catalog.schema.table.column
        tags (dict): Dictionary of tags to apply, e.g. { "sensitivity": "high" }
    """
    if level not in ["catalog", "schema", "table", "column"]:
        raise ValueError("Level must be one of: 'catalog', 'schema', 'table', or 'column'.")

    if not tags or not isinstance(tags, dict):
        raise ValueError("Tags must be a dictionary of {tag_name: tag_value}.")

    parts = fully_qualified_name.split(".")

    if (level == "catalog" and len(parts) != 1) or \
       (level == "schema" and len(parts) != 2) or \
       (level == "table" and len(parts) != 3) or \
       (level == "column" and len(parts) != 4):
        raise ValueError(f"Invalid object name format for level '{level}': {fully_qualified_name}")

    # Build tag assignment clause
    tag_fragments = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])

    # Build SQL command
    if level == "catalog":
        catalog = parts[0]
        sql = f"ALTER CATALOG {catalog} SET TAGS ({tag_fragments})"

    elif level == "schema":
        catalog, schema = parts
        sql = f"ALTER SCHEMA {catalog}.{schema} SET TAGS ({tag_fragments})"

    elif level == "table":
        catalog, schema, table = parts
        sql = f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tag_fragments})"

    elif level == "column":
        catalog, schema, table, column = parts
        sql = f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} SET TAGS ({tag_fragments})"

    print(f"Running SQL:\n{sql}\n")
    spark.sql(sql)


# Unit test
# mydict = {'hive_metastore.default': {'retention_policy': '3_years', 'domain': 'customer_data'}}
# for key, val in mydict.items():
#     apply_uc_tags("schema", key, val)

# COMMAND ----------

# DBTITLE 1,Ingest Input JSON Data for Data Contract
input_base_path = "./input_data"

# Data quality rules input
custom_dq_rules_input_path = f"{input_base_path}/data_quality_rules_input/data_quality_rules.json"
with open(custom_dq_rules_input_path, 'r') as file:
    custom_dq_rules_input = file.read()
# Replace any incorrect control characters or format issues
custom_dq_rules_input = ' '.join(custom_dq_rules_input.split())
custom_dq_rules_input = read_json_file(json_str = custom_dq_rules_input)
# print(f"custom_dq_rules_input:\n{custom_dq_rules_input}\n")

# Contract metadata input
contract_metadata_input_path = f"{input_base_path}/contract_metadata_input/contract_metadata.json"
contract_metadata_input = read_json_file(file_path = contract_metadata_input_path)
# print(f"contract_metadata_input:\n{contract_metadata_input}\n")

# Server metadata input
server_metadata_input_path = f"{input_base_path}/server_metadata_input/server_metadata.json"
server_metadata_input = read_json_file(file_path = server_metadata_input_path)
# print(f"server_metadata_input:\n{server_metadata_input}\n")

# Support channel metadata input
support_channel_metadata_input_path = f"{input_base_path}/support_channel_metadata_input/support_channel_metadata.json"
support_channel_metadata_input = read_json_file(file_path = support_channel_metadata_input_path)
# print(f"support_channel_metadata:\n{support_channel_metadata}\n")

# Tags metadata input
tags_metadata_input_path = f"{input_base_path}/tags_metadata_input/tags_metadata.json"
tags_metadata_input = read_json_file(file_path = tags_metadata_input_path)
# print(f"tags_metadata_input:\n{tags_metadata_input}\n")

# Service level agreement (SLA) metadata input
sla_metadata_input_path = f"{input_base_path}/sla_metadata_input/sla_metadata.json"
sla_metadata_input = read_json_file(file_path = sla_metadata_input_path)
# print(f"sla_metadata_input:\n{sla_metadata_input}\n")

# Service level agreement (SLA) metadata input
sla_metadata_input_path = f"{input_base_path}/sla_metadata_input/sla_metadata.json"
sla_metadata_input = read_json_file(file_path = sla_metadata_input_path)
# print(f"sla_metadata_input:\n{sla_metadata_input}\n")