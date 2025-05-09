# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Tags Notebook
# MAGIC
# MAGIC This notebook is used to deploy tags defined in the contract.
# MAGIC
# MAGIC Given the path to a data contract .yaml, this notebook performs the following actions:
# MAGIC 1. Reads in the contract.
# MAGIC 2. Retrieves schema, table, and column level tags.
# MAGIC 3. Formats the tags according to unity catalog requirments.
# MAGIC 4. Deploys the tags to the relevant object in UC in a multi-threaded fashion to support high throughput.

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Workflow Widget Parameters
# MAGIC
# MAGIC This step defines widgets and initializes variables.

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Widget Parameters
dbutils.widgets.text("user_email", dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())
user_email = dbutils.widgets.get("user_email")
print(f"user_email: {user_email}")


dbutils.widgets.text("author_folder_path", f"/Workspace/Users/{user_email}/odcs_data_contracts/notebooks/input_data")  # should be a Workspace Users folder
author_folder_path = dbutils.widgets.get("author_folder_path")
print(f"author_folder_path: {author_folder_path}")


dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


# yaml_file_path syntax --> "{source_catalog}__{source_schema}.yaml"
dbutils.widgets.text("yaml_file_path", f"{author_folder_path.split('/input_data')[0]}/data_contracts_data/catalog={source_catalog}/{source_catalog}__{source_schema}.yaml")
yaml_file_path = dbutils.widgets.get("yaml_file_path")
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Read in the Data Contract Yaml
# MAGIC
# MAGIC This step defines reads in the existing data contract yaml as a dictionary.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read in the Data Contract Yaml
with open(yaml_file_path, 'r') as f:
    data_contract_odcs_yaml = yaml.safe_load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step: Parse Catalog and Schema from Contract and Define Functions
# MAGIC
# MAGIC Tags are stored in the contract as key:value pairs encoded as string. The string has to be parsed into a dictionary before passing to UC.
# MAGIC
# MAGIC The ```deploy_tags()``` function relies on an 'apply_uc_tags()' function defined in the contract_helpers notebook. The ```apply_uc_tags()``` function generates the necessary SQL DDL command based on the level of the tag.

# COMMAND ----------

# DBTITLE 1,Read the Target Catalog and Target Schema From Data Contract
# Get the data contract catalog and schema
target_catalog = data_contract_odcs_yaml["servers"][0]["catalog"] # This represents target catalog
target_schema = data_contract_odcs_yaml["servers"][0]["schema"] # This represents target schema

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

# MAGIC %md
# MAGIC ## Step: Parse Tags from Contract and Execute DDLs to Update Tags
# MAGIC
# MAGIC This step leverages ```ThreadPoolExecutor()``` to parallelize the execution of the DDLs across multiple tables and columns.

# COMMAND ----------

# DBTITLE 1,Get Tags From Data Contract
# Deploy schema-level tags
schema_tags_formatted = format_tags(data_contract_odcs_yaml["tags"])
schema_tags_deploy = {
    f"{target_schema}.{target_schema}": schema_tags_formatted
}
if schema_tags_formatted:
    results = deploy_tags("schema", schema_tags_deploy)
    print(f"{results} for {target_schema}.{target_schema}\n")


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
                f"{target_catalog}.{target_schema}.{source_table}": table_tags_formatted
            }
            results = deploy_tags("table", table_tags_deploy)
            print(f"{results} for {target_catalog}.{target_schema}.{source_table}\n")

        # Column-level tags (submitted to thread pool)
        for col_properties in table_properties.get("properties", []):
            futures.append(
                executor.submit(
                    deploy_column_tag,
                    target_catalog,
                    target_schema,
                    source_table,
                    col_properties
                )
            )

    # Wait for all futures to complete
    for future in as_completed(futures):
        result = future.result()
        if result:
            print(result)

