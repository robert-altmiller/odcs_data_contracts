# Databricks notebook source
pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


# COMMAND ----------

input_df = spark.read.table("flightstats_historical_dev_azr_westus.data_contract_dev.flightstats")

# profile input data
ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"

dq_engine = DQEngine(ws)

# save checks in arbitrary workspace location
dq_engine.save_checks_in_workspace_file(checks, workspace_path="checks.yml")

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# checks = DQEngine.load_checks_from_workspace_file(workspace_path="./dqx/checks.yml")
dq_engine = DQEngine(WorkspaceClient())
dq_engine.load_checks_from_workspace_file(workspace_path="/Volumes/flightstats_historical_dev_azr_westus/data_contract_test/contracts/checks.yml")

input_df = spark.read.table("flightstats_historical_dev_azr_westus.data_contract_dev.flightstats")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)

# COMMAND ----------

quarantined_df.write.mode("overwrite").saveAsTable("flightstats_historical_dev_azr_westus.data_contract_dev.flightstats_quarantined")
