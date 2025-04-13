# Databricks notebook source
# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Folder and File Path Widget Parameters
dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


dbutils.widgets.text("yaml_file_path", f"./data_contracts_data/{source_catalog}__{source_schema}.yaml")
yaml_file_path = dbutils.widgets.get("yaml_file_path")
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# DBTITLE 1,Read in the Data Contract Yaml
with open(yaml_file_path, 'r') as f:
    data_contract_odcs_yaml = yaml.safe_load(f)

# COMMAND ----------

# DBTITLE 1,Format Tags for Databricks SQL
def format_tags(tags_list):
    """
    Converts a list of tag strings in the format "key:value" into a dictionary.
    Args:
        tags_list (list): A list of strings representing tags, where each string is in "key:value" format.
    Returns:
        dict: A dictionary where keys are tag names and values are tag values.
              Example: ["pii:true", "classification:high"] → {"pii": "true", "classification": "high"}
    """
    try:
        tags_formatted = {}
        for tag in tags_list:
            tag_name = tag.split(":")[0]
            tag_value = tag.split(":")[1]
            tags_formatted[tag_name] = tag_value
        return tags_formatted
    except: return {} # Unable to format tags

# COMMAND ----------

# DBTITLE 1,Deploy Tags Using Databricks SQL

def deploy_tags(level, deploy_tags_list):
    """
    Deploys formatted tags to Unity Catalog objects using the apply_uc_tags function.
    Args:
        level (str): The Unity Catalog level to apply tags to — one of ['catalog', 'schema', 'table', 'column'].
        deploy_tags_list (dict): A dictionary where each key is a fully qualified object name (e.g., "catalog.schema.table.column")
                                 and each value is a dictionary of tags to apply.
    Returns:
        str: A success message or an error message if deployment fails.
    """
    try:
        for key, val in deploy_tags_list.items():
            apply_uc_tags(level, key, val)
        return "successfully deployed tags"
    except Exception as e:
        return f"Unable to deploy tags: ({e})"

# COMMAND ----------

# DBTITLE 1,Get Tags From Data Contract
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get the data contract catalog and schema
contract_source_catalog = data_contract_odcs_yaml["servers"][0]["catalog"]
contract_source_schema = data_contract_odcs_yaml["servers"][0]["schema"]

# Deploy schema-level tags
schema_tags_formatted = format_tags(data_contract_odcs_yaml["tags"])
schema_tags_deploy = {
    f"{contract_source_catalog}.{contract_source_schema}": schema_tags_formatted
}
if schema_tags_formatted:
    results = deploy_tags("schema", schema_tags_deploy)
    print(f"{results} for {contract_source_catalog}.{contract_source_schema}\n")


# Define threaded column deployment
def deploy_column_tag(catalog: str, schema: str, table: str, col_props: dict):
    source_col = col_props["name"]
    col_tags_formatted = format_tags(col_props.get("tags", {}))
    if col_tags_formatted:
        fq_col = f"{catalog}.{schema}.{table}.{source_col}"
        col_tags_deploy = {fq_col: col_tags_formatted}
        results = deploy_tags("column", col_tags_deploy)
        return f"{results} for {fq_col}"
    return None


# Deploy table-level and column-level tags
schema_obj = data_contract_odcs_yaml["schema"]
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []

    for table_properties in schema_obj:
        source_table = table_properties["name"]

        # Table-level tags
        table_tags_formatted = format_tags(table_properties.get("tags", {}))
        if table_tags_formatted:
            table_tags_deploy = {
                f"{contract_source_catalog}.{contract_source_schema}.{source_table}": table_tags_formatted
            }
            results = deploy_tags("table", table_tags_deploy)
            print(f"{results} for {contract_source_catalog}.{contract_source_schema}.{source_table}\n")

        # Column-level tags (submitted to thread pool)
        for col_properties in table_properties.get("properties", []):
            futures.append(
                executor.submit(
                    deploy_column_tag,
                    contract_source_catalog,
                    contract_source_schema,
                    source_table,
                    col_properties
                )
            )

    # Wait for all futures to complete
    for future in as_completed(futures):
        result = future.result()
        if result:
            print(result)


# COMMAND ----------

# DBTITLE 1,Get Tags From Data Contract (Old)
# # Get the data contract catalog and schema
# contract_source_catalog = data_contract_odcs_yaml["servers"][0]["catalog"]
# contract_source_schema = data_contract_odcs_yaml["servers"][0]["schema"]

    
# # Deploy schema level tags
# schema_tags_formatted = format_tags(data_contract_odcs_yaml["tags"])
# schema_tags_deploy = {f"{contract_source_catalog}.{contract_source_schema}": schema_tags_formatted}
# if schema_tags_deploy: # Created SQL alter tags stmt
#     results = deploy_tags("schema", schema_tags_deploy)
#     print(f"{results} for {contract_source_catalog}.{contract_source_schema}\n")


# # Deploy table level tags
# schema_obj = data_contract_odcs_yaml["schema"]
# for table_properties in schema_obj:
#     source_table = table_properties["name"]
#     table_tags_formatted = format_tags(table_properties["tags"])
#     if table_tags_formatted: # Created SQL alter tags stmt
#         table_tags_deploy = {f"{contract_source_catalog}.{contract_source_schema}.{source_table}": table_tags_formatted}
#         results = deploy_tags("table", table_tags_deploy)
#         print(f"{results} for {contract_source_catalog}.{contract_source_schema}.{source_table}\n")


#     # Deploy column level tags
#     for col_properties in table_properties["properties"]:
#         source_col = col_properties["name"]
#         col_tags_formatted = format_tags(col_properties["tags"])
#         if col_tags_formatted: # Created SQL alter tags stmt
#             col_tags_deploy = {f"{contract_source_catalog}.{contract_source_schema}.{source_table}.{source_col}": col_tags_formatted}
#             results = deploy_tags("column", col_tags_deploy)
#             print(f"{results} for {contract_source_catalog}.{contract_source_schema}.{source_table}.{source_col}\n")
