# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
# Standard library imports
import ast, json, os, re, random, shutil, string, subprocess, time, sys
from datetime import datetime, date

# Concurrent futures multi-threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third-party libraries
import numpy as np
import pandas as pd
import yaml

# PySpark imports
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient


# ODCS data contract SDK
from datacontract.data_contract import DataContract
from pydantic import ValidationError
from pydantic_core import PydanticCustomError

sys.path.append(os.path.abspath("../src/"))
print(sys.path)
from contract_validation.contract_models import DataContractMetadata

# COMMAND ----------
w = WorkspaceClient()


# DBTITLE 1,Check if Running in a Workflow
def is_running_in_databricks_workflow(job_context):
    """Detect if running inside a Databricks Workflow job."""
    return bool(job_context.get("running_in_workflow"))


# COMMAND ----------


# DBTITLE 1,Check if a File Exists
def check_file_exists(filepath):
    """Return whether a file or directory exists at *filepath*.
    Args:
        filepath (str | os.PathLike): Absolute or relative path to the
            target file or directory.
    Returns:
        bool: ``True`` if the path exists, otherwise ``False``.
    """
    return os.path.exists(filepath)


# COMMAND ----------


# DBTITLE 1,Check if Databricks Catalog and Schema Existswhat
def catalog_exists(catalog_name: str) -> bool:
    """
    Checks if a specified Databricks catalog exists.
    Args:
        catalog_name (str): The name of the catalog to check.
    Returns:
        bool: True if the catalog exists, False otherwise.
    """
    try:
        w.catalogs.get(catalog_name)
        return True
    except AnalysisException:
        return False


def schema_exists(catalog_name: str, schema_name: str) -> bool:
    """
    Checks if a specified schema exists within a given Databricks catalog.
    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema to check.
    Returns:
        bool: True if the schema exists, False otherwise.
    """
    try:
        w.schemas.get(f"{catalog_name}.{schema_name}")
        return True
    except AnalysisException:
        return False


# COMMAND ----------


# DBTITLE 1,Make a Random String
def make_random():
    """
    Factory function to create a random string generator.
    This function returns another function (`inner`) that, when called, generates a random
    string of specified length. The characters used are uppercase letters, lowercase letters,
    and digits.
    Example usage:
        random_string = make_random()()       # Generates a 16-character string
        random_string = make_random()(k=8)    # Generates an 8-character string
    """

    def inner(k=16) -> str:
        """
        Generates a random alphanumeric string.
        Parameters:
        -----------
        k : int, optional
            The desired length of the random string. Default is 16.
        Returns:
        --------
        str:
            A string of randomly selected characters (A-Z, a-z, 0-9).
        """
        # Define the character set: uppercase, lowercase, and digits
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        # Randomly select `k` characters from the character set and return as a string
        return "".join(random.choices(charset, k=int(k)))

    # Return the inner function to be called later
    return inner


# COMMAND ----------


# DBTITLE 1,Check If an SQL Object is a View and Create View  as a Table
def check_if_object_is_view(catalog, schema, obj_name):
    """
    Checks if the given object in the specified catalog and schema is a VIEW.
    Args:
        catalog (str): The catalog name in Unity Catalog.
        schema (str): The schema/database name.
        obj_name (str): The name of the object (table or view) to check.
    Returns:
        bool: True if the object is a view, False otherwise.
    """
    # Set the current catalog and schema context
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
    # Run SHOW TABLE EXTENDED to retrieve metadata about the object
    SQL = f"SHOW TABLE EXTENDED IN {catalog}.{schema} LIKE '{obj_name}'"
    object_props = spark.sql(SQL).select("information")
    # Check if the metadata includes "Type: VIEW"
    if object_props.first()["information"].find("Type: VIEW") != -1:
        return True
    else:
        return False


def create_view_as_table(catalog, schema, view_name):
    """
    Creates an empty table (structure only) from an existing view’s schema.
    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.
        view_name (str): The name of the view to convert.
    Returns:
        str: The name of the newly created table.
    """
    # Define the new table name by appending "_tbl" to the view name

    view_tbl_name = f"{view_name}_{make_random()(k=8)}"
    # Construct and run the SQL to create the new table structure only
    SQL = f"CREATE OR REPLACE TABLE {view_tbl_name} AS SELECT * FROM {catalog}.{schema}.{view_name} WHERE FALSE"
    spark.sql(SQL)
    # Output status message
    print(
        f"Created table named '{view_tbl_name}' from view named '{view_name}' in catalog.schema '{catalog}.{schema}'"
    )
    # Return the name of the created table
    return view_tbl_name


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
        for table in w.tables.list(catalog_name=catalog, schema_name=schema):
            tables_list.append(table.name)
            tables_dict[table.name] = table.comment

        tables_list = [t for t in tables_list if "data_quality" not in t]
        tables_dict = {k: v for k, v in tables_dict.items() if "data_quality" not in k}
        print(f"tables_list: {tables_list}")

        return tables_list, tables_dict

    except Exception as e:
        print(f"Error listing tables in {catalog}.{schema}: {e}")
        return None, None


# Unit Test
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
        "date": "string",
    }
    fields = [
        {"name": col, "type": avro_types.get(str(df[col].dtype), "string")}
        for col in df.columns
    ]
    schema = {"type": "record", "name": "AutoGeneratedSchema", "fields": fields}
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
            with open(file_path, "r", encoding="utf-8") as f:
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


def write_json_file(data, file_path: str, indent: int = 2):
    """
    Writes a Python object to a JSON file with optional indentation.
    Parameters:
        data (dict or list): The data to write to the file.
        file_path (str): Path to the output JSON file.
        indent (int): Number of spaces to use for indentation. Default is 2.
    """
    try:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(file_path, "w+", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        print(f"JSON written to {file_path}")
    except Exception as e:
        print(f"Error writing JSON to file: {e}")


# Unit Test
# sample_data = {"key": "value", "list": [1, 2, 3]}
# write_json_file(sample_data, "test.json")

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
        return {
            k: replace_none_with_empty_string_in_json(v) for k, v in json_obj.items()
        }
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
    try:
        return spark.read.table(f"{catalog}.{schema}.{table}").columns
    except Exception as e:
        print(f"Error getting table columns for {catalog}.{schema}.{table}: {e}")
        return []


# COMMAND ----------


# DBTITLE 1,Get UC Tags From Information_Schema
def get_existing_uc_tags(
    input_catalog: str = None,
    input_schema: str = None,
    input_table: str = None,
    input_column: str = None,
    catalog: str = "system",
    schema: str = "information_schema",
    tag_level: str = "catalog",
) -> dict:
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
        raise ValueError(
            f"Invalid tag_level: {tag_level}. Must be one of ['catalog', 'schema', 'table', 'column']."
        )

    table_name = f"{catalog}.{schema}.{tag_level}_tags"

    where_clauses = []
    if input_catalog is not None:
        where_clauses.append(f"catalog_name = '{input_catalog}'")
    if tag_level in ["table", "column"] and input_schema is not None:
        where_clauses.append(f"schema_name = '{input_schema}'")
    if tag_level in ["column"] and input_table is not None:
        where_clauses.append(f"table_name = '{input_table}'")

    where_clause = " AND ".join(where_clauses)
    sql_query = f"SELECT * FROM {table_name} WHERE {where_clause}"

    print(f"Running SQL: {sql_query}\n")
    records = spark.sql(sql_query).collect()

    existing_tags = {}
    for row in records:
        key = ".".join(
            [
                x
                for x in [
                    getattr(row, 'catalog_name', None),
                    getattr(row, 'schema_name', None), 
                    getattr(row, 'table_name', None),
                    getattr(row, 'column_name', None)
                ]
                if x is not None
            ]
        )
        existing_tags[key] = {row.tag_name: row.tag_value}
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
def tag_dict_to_list(input_dict: dict) -> dict:
    """
    Converts a list of dicts in the format:
    [{'catalog.schema.table.column': {'tag1': 'value1'}}]
    Into:
    {'tags': {'column_name': ['tag1:value1', ...]}}
    """
    result_dict = {}

    for fq_name, tag_dict in input_dict.items():
        # Extract the column name from the full FQN
        tags = [f"{k}:{v}" for k, v in tag_dict.items()]
        result_dict[fq_name] = tags
    return result_dict


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
        tags = get_existing_uc_tags(
            input_catalog=catalog, input_schema=schema, tag_level="schema"
        )
    elif catalog and schema and table:
        # Table-level
        tags = get_existing_uc_tags(
            input_catalog=catalog,
            input_schema=schema,
            input_table=table,
            tag_level="table",
        )
    else:
        raise ValueError(
            "Invalid arguments: must provide either (catalog), (catalog, schema), or (catalog, schema, table)."
        )

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
        column_tags = get_existing_uc_tags(
            input_catalog=catalog,
            input_schema=schema,
            input_table=table,
            input_column=col,
            tag_level="column",
        )
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
        raise ValueError(
            "Level must be one of: 'catalog', 'schema', 'table', or 'column'."
        )

    if not tags or not isinstance(tags, dict):
        raise ValueError("Tags must be a dictionary of {tag_name: tag_value}.")

    parts = fully_qualified_name.split(".")

    if (
        (level == "catalog" and len(parts) != 1)
        or (level == "schema" and len(parts) != 2)
        or (level == "table" and len(parts) != 3)
        or (level == "column" and len(parts) != 4)
    ):
        raise ValueError(
            f"Invalid object name format for level '{level}': {fully_qualified_name}"
        )

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
def get_authoring_data(base_dir="./input_data", contract_exists=False):
    """
    Initializes and loads input metadata files for authoring a data contract.
    Parameters:
        base_dir (str): The base input directory path.
        contract_exists (bool): Determines if a data contract already exists.
    Returns:
        dict: Dictionary with loaded JSON payloads keyed by metadata type.
    """
    input_paths = {
        "custom_dq_rules_input": f"data_quality_rules_input/data_quality_rules.json",
        "contract_metadata_input": f"contract_metadata_input/contract_metadata.json",
        "server_metadata_input": f"server_metadata_input/server_metadata.json",
        "support_channel_metadata_input": f"support_channel_metadata_input/support_channel_metadata.json",
        "sla_metadata_input": f"sla_metadata_input/sla_metadata.json",
        "team_metadata_input": f"team_metadata_input/team_metadata.json",
        "roles_metadata_input": f"roles_metadata_input/roles_metadata.json",
        "pricing_metadata_input": f"pricing_metadata_input/pricing_metadata.json",
        "schema_metadata_input": f"schema_metadata_input/schema_metadata.json",
    }

    inputs = {}

    for folder_name, file_path in input_paths.items():
        author_path = f"{base_dir}/{file_path}"
        if not contract_exists:  # then data contract does not exist
            if check_file_exists(author_path):
                print(f"{author_path}")
                if folder_name == "custom_dq_rules_input":
                    with open(author_path, "r") as file:
                        raw_data = file.read()
                    raw_data = " ".join(raw_data.split())
                    inputs[folder_name] = read_json_file(json_str=raw_data)
                else:
                    inputs[folder_name] = read_json_file(author_path)
            else:  # data contract and author_path do not exist
                inputs[folder_name] = None
        elif check_file_exists(author_path):  # and the contract also exists
            print(f"{author_path}")
            inputs[folder_name] = read_json_file(author_path)
        else:
            inputs[folder_name] = None
    return inputs
