# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Notebook
# MAGIC
# MAGIC This notebook is used to execute data quality checks against rules defined in the contract and record the results in a ```odcs_data_quality``` Delta table within the same schema defined by the data contract.

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Collect Parameter to Indicate Whether the Notebook was Triggered from a Databricks Workflow
# MAGIC
# MAGIC This information is used to update the path appropriately to make sure the notebook works when run both manually and from a workflow.

# COMMAND ----------

# DBTITLE 1,Check if Running in Databricks Job
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


dbutils.widgets.text("dq_folder_path", f"{author_folder_path.split('/input_data')[0]}/data_quality")
dq_folder_path = dbutils.widgets.get("dq_folder_path")
print(f"dq_folder_path: {dq_folder_path}")


dbutils.widgets.text("source_catalog", "hive_metastore")
source_catalog = dbutils.widgets.get("source_catalog")
print(f"source_catalog: {source_catalog}")


dbutils.widgets.text("source_schema", "default")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


# yaml_file_path = sourcecatalog__sourceschema.yaml
dbutils.widgets.text("yaml_file_path", f"{author_folder_path.split('/input_data')[0]}/data_contracts_data/catalog={source_catalog}/{source_catalog}__{source_schema}.yaml")
yaml_file_path = dbutils.widgets.get("yaml_file_path")
print(f"yaml_file_path: {yaml_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Initialize the Data Contract Object
# MAGIC
# MAGIC This step initializes the Data Contract CLI object.

# COMMAND ----------

# DBTITLE 1,Initialize the Data Contract Object
data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Read in the Data Contract Yaml
# MAGIC
# MAGIC This step defines reads in the existing data contract yaml as a dictionary.

# COMMAND ----------

# DBTITLE 1,Read in the Data Contract Yaml
with open(yaml_file_path, 'r') as f:
    data_contract_odcs_yaml = yaml.safe_load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Read Target Catalog and Target Schema From Data Contract
# MAGIC
# MAGIC This step reads in the target catalog and schema from the ODCS data contract.

# COMMAND ----------

# DBTITLE 1,Read Target Catalog and Target Schema From Data Contract
# Get the data contract catalog and schema
target_catalog = data_contract_odcs_yaml["servers"][0]["catalog"] # This represents target catalog
target_schema = data_contract_odcs_yaml["servers"][0]["schema"] # This represents target schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Define Function for Running Data Quality Tests
# MAGIC
# MAGIC This function uses the Data Contract CLI's 'test()' method to perform the built-in as well as contract-defined quality checks. The results are written to a json file before being read back into a dataframe and returned.

# COMMAND ----------

# DBTITLE 1,Run Data Quality Tests Function
def run_data_quality_tests(data_contract, yaml_file_path, dq_path="./data_quality", dq_file="data_quality.json"):
    """
    Executes data quality tests defined in an ODCS data contract and writes the results to a JSON file.

    This function:
    1. Loads a data contract from the provided YAML path.
    2. Runs the `.test()` method to evaluate all defined data quality checks.
    3. Extracts and structures the results into a list of dictionaries.
    4. Saves the results as a JSON file at the specified output path.
    5. Loads the results into a Spark DataFrame and displays rows where the tested field is null.

    Args:
        yaml_file_path (str): Path to the ODCS data contract YAML file.
        spark (SparkSession): Active Spark session to run the test and load results.
        dq_path (str, optional): Directory where the JSON results file will be saved. Default is "./data_quality".
        dq_file (str, optional): Filename for the JSON results. Default is "data_quality.json".

    Returns:
        DataFrame: A Spark DataFrame containing the test results.
    """
    # Run data contract data quality tests
    test_results = data_contract.test()

    # Extract test results into list of dictionaries
    list_results = []
    for result in test_results.checks:
        json_results = {item[0]: item[1] for item in result}
        list_results.append(json_results)

    # Save results to a local JSON file
    os.makedirs(dq_path, exist_ok=True)
    dq_file_path = f"{dq_path}/{dq_file}"

    # Remove local dq results file if exists
    if os.path.exists(dq_file_path):
        os.remove(dq_file_path)

    with open(dq_file_path, "w+") as json_file:
        json_file.write(json.dumps(list_results))

    # Load results into Spark DataFrame
    if is_running_in_databricks_workflow(job_context) == True:
        data_path = f"{dq_folder_path.replace('/dbfs', '')}/{dq_file}"
    else: 
        data_path = f"file:{dq_folder_path}/{dq_file}"
    df = spark.read.json(data_path)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    # Print overall test result summary
    print(f"'{yaml_file_path}' ODCS data quality result: {test_results.result}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Run Data Quality Tests and Save as Table
# MAGIC
# MAGIC The ```run_data_quality_tests()``` function is called in this step and the returned quality results are appended to the ```odcs_data_quality``` Delta table stored in the data contract's schema.

# COMMAND ----------

# DBTITLE 1,Run ODCS Contract Data Quality Tests and Store in DF
# Example usage
dq_results_df = run_data_quality_tests(data_contract, yaml_file_path, dq_path = dq_folder_path, dq_file = f"{target_catalog}__{target_schema}_odcs_data_quality.json")

# Write to a managed Delta table (overwrite mode)
dq_results_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f"{target_catalog}.{target_schema}.odcs_data_quality")

# Display the results
display(dq_results_df)
