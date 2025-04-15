# Databricks notebook source
pip install databricks-labs-dqx

# COMMAND ----------

pip install soda-core-spark-df

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import yaml
import json

# COMMAND ----------


l_quality =[]
# Load YAML from a file
with open('/Volumes/flightstats_historical_dev_azr_westus/data_contract_test/contracts/odcs-dqx.yml', 'r') as file:
    data = yaml.safe_load(file)
schemas = data.get("schema")
dqx_final_checks = []
for schema in schemas:
    lq = schema.get("quality")
    if lq is None:
        continue
    for item in lq:
        if item.get('engine') == 'DQX':
            dqx_checks = item.get("implementation")
            dqx_final_checks = dqx_final_checks + dqx_checks
        else:
            l_quality.append(item)
    schema['quality'] = l_quality

s_odcs = json.dumps(data)

with open('dqx-checks.yml', 'w') as file:
    yaml.dump(dqx_final_checks, file, default_flow_style=False)

# COMMAND ----------



# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks_from_local_file("dqx-checks.yml")
input_df = spark.read.table("flightstats_historical_dev_azr_westus.data_contract_dev.flightstats")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)

# COMMAND ----------

quarantined_df.display()

# COMMAND ----------

from datacontract.data_contract import DataContract

data_contract = DataContract(data_contract_str=s_odcs, spark=spark)
run = data_contract.test()
results = []
for check in run.checks:
    if check.field is None:
        field = check.implementation
    else:
        field = check.field
    r = {"table": check.model, "name": check.name, "result": str(check.result).split(".")[1] if check.result is not None else 'unknown', "field": field if field is not None else 'unknown'}
    results.append(r)

spark.createDataFrame(results).display()
