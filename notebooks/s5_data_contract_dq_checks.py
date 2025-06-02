# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Notebook
# MAGIC
# MAGIC This notebook is used to execute data quality checks against rules defined in the contract and record the results in a ```odcs_data_quality``` Delta table within the same schema defined by the data contract.

# COMMAND ----------

# DBTITLE 1,Import Python Helpers
# MAGIC %run "./helpers/contract_helpers"

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
current_directory = os.getcwd()

dbutils.widgets.text("data_contract_path", f"{current_directory}data_contracts_data/")  
data_contract_path = dbutils.widgets.get("data_contract_path")
print(f"data_contract_path: {data_contract_path}")

dbutils.widgets.text("server_name", "dev")  
server_name = dbutils.widgets.get("server_name")
print(f"server_name: {server_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Initialize the Data Contract Object
# MAGIC
# MAGIC This step initializes the Data Contract CLI object.

# COMMAND ----------

# DBTITLE 1,Initialize the Data Contract Object
data_contracts = get_list_of_contract_objects(data_contract_path)
data_contracts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Define Function for Running Data Quality Tests
# MAGIC
# MAGIC This function uses the Data Contract CLI's 'test()' method to perform the built-in as well as contract-defined quality checks. The results are written to a json file before being read back into a dataframe and returned.

# COMMAND ----------

# DBTITLE 1,Run Data Quality Tests Function
def run_data_quality_tests(data_contract):
    """
    Executes data quality tests defined in an ODCS data contract and writes the results to a JSON file.

    This function:
    1. Loads a data contract from the provided YAML path.
    2. Runs the `.test()` method to evaluate all defined data quality checks.
    3. Extracts and structures the results into a list of dictionaries.
    4. Loads the results into a Spark DataFrame and displays rows where the tested field is null.

    Args:
        spark (SparkSession): Active Spark session to run the test and load results.
        data_contract(DataContract): DataContract object

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

    json_data = json.dumps(list_results)

    df_schema = ArrayType(
        StructType(
            [
                StructField("category", StringType(), True),
                StructField("details", StringType(), True),
                StructField("diagnostics", VariantType(), True),
                StructField("engine", StringType(), True),
                StructField("field", StringType(), True),
                StructField("id", StringType(), True),
                StructField("implementation", StringType(), True),
                StructField("key", StringType(), True),
                StructField("language", StringType(), True),
                StructField("model", StringType(), True),
                StructField("name", StringType(), True),
                StructField("reason", StringType(), True),
                StructField("result", StringType(), True),
                StructField("type", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True),
            ]
        )
    )

    df = spark.createDataFrame([json_data], "string")
    df = df.withColumn("value", F.explode(F.from_json(F.col("value"), df_schema))).select(
        "value.*"
    )
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    # Print overall test result summary
    print(
        f"'{data_contract.get_data_contract_specification().info.title}' ODCS data quality result: {test_results.result}"
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step: Run Data Quality Tests and Save as Table
# MAGIC
# MAGIC The ```run_data_quality_tests()``` function is called in this step and the returned quality results are appended to the ```odcs_data_quality``` Delta table stored in the data contract's schema.

# COMMAND ----------

# DBTITLE 1,Run ODCS Contract Data Quality Tests and Store in DF
for contract in data_contracts:
    # Extract contract specification details
    contract_details = contract.get_data_contract_specification()

    # Get target catalog and schema from server configuration
    target_catalog = contract_details.servers.get(server_name).catalog
    target_schema = contract_details.servers.get(server_name).schema_

    # Example usage
    dq_results_df = run_data_quality_tests(contract)

    # Write to a managed Delta table (overwrite mode)
    dq_results_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f"{target_catalog}.{target_schema}.odcs_data_quality")

    # Display the results
    display(dq_results_df)
