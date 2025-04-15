# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install 'datacontract-cli[databricks,avro,csv,parquet,sql]'

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Python Imports
import json, os, time
import pyspark.sql.functions as F
from datacontract.data_contract import DataContract

# COMMAND ----------

# DBTITLE 1,Remove DB Widgets
dbutils.widgets.removeAll()
time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers"

# COMMAND ----------

# DBTITLE 1,Check if Running in Databricks Job
# Create widgets to capture metadata
dbutils.widgets.text("running_in_workflow", "")

# Retrieve widget values safely
job_context = {
    "running_in_workflow": dbutils.widgets.get("running_in_workflow"),
}

def is_running_in_databricks_workflow():
    """Detect if running inside a Databricks Workflow job."""
    return bool(job_context.get("running_in_workflow"))

# Unit test
print(f"is_running_in_databricks_workflow: {is_running_in_databricks_workflow()}")

# COMMAND ----------

# DBTITLE 1,Workflow Widget Parameters
# Data Contract Parameters
# Create text widgets with default values
dbutils.widgets.text("dq_catalog", "flightstats_historical_dev_azr_westus")
dq_catalog = dbutils.widgets.get("dq_catalog")
print(f"dq_catalog: {dq_catalog}")


dbutils.widgets.text("dq_schema", "data_contract_prod")
dq_schema = dbutils.widgets.get("dq_schema")
print(f"dq_schema: {dq_schema}")


dbutils.widgets.text("source_schema", "data_contract_dev")
source_schema = dbutils.widgets.get("source_schema")
print(f"source_schema: {source_schema}")


# Create yaml_file_path dynamically using f-string
yaml_file_path_default = f"./data_contracts_data/{dq_catalog}__{source_schema}.yaml"
dbutils.widgets.text("yaml_file_path", yaml_file_path_default)
yaml_file_path = dbutils.widgets.get("yaml_file_path")
print(f"yaml_file_path: {yaml_file_path}")


dbutils.widgets.text("dq_folder_path", "./data_quality")
dq_folder_path = dbutils.widgets.get("dq_folder_path")
print(f"dq_folder_path: {dq_folder_path}")

# COMMAND ----------

# DBTITLE 1,Run Data Quality Tests Function
def run_data_quality_tests(yaml_file_path, dq_path="./data_quality", dq_file="data_quality.json"):
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
    # Load the contract and run tests
    data_contract = DataContract(data_contract_file=yaml_file_path, spark=spark)
    test_results = data_contract.test()

    # Extract test results into list of dictionaries
    list_results = []
    for result in test_results.checks:
        json_results = {item[0]: item[1] for item in result}
        list_results.append(json_results)

    # Save results to a local JSON file
    os.makedirs(dq_path, exist_ok=True)
    dq_file_path = f"{dq_path}/{dq_file}"

    if os.path.exists(dq_file_path):
        os.remove(dq_file_path)

    with open(dq_file_path, "w+") as json_file:
        json_file.write(json.dumps(list_results))

    # Load results into Spark DataFrame
    if is_running_in_databricks_workflow() == True:
        data_path = f"{dq_folder_path.replace('/dbfs', '')}/{dq_file}"
    else: 
        data_path = f"file:{os.getcwd()}/{dq_file_path.split('./')[1]}"
    df = spark.read.json(data_path)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    # Print overall test result summary
    print(f"'{yaml_file_path}' ODCS syntax validation: {test_results.result}")

    return df

# COMMAND ----------

# DBTITLE 1,Run ODCS Contract Data Quality Tests and Store in DF
# Example usage
dq_results_df = run_data_quality_tests(yaml_file_path, dq_path = dq_folder_path)

# Write to a managed Delta table (overwrite mode)
dq_results_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f"{dq_catalog}.{dq_schema}.odcs_data_quality")

# Display the results
display(dq_results_df)
