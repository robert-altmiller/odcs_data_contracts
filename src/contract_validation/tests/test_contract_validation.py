import yaml
from contract_validation.contract_models import DataContractMetadata
from pydantic import ValidationError
with open("src/contract_validation/tests/hive_metastore__default.yaml", "r") as f:
    data = yaml.safe_load(f)
try:
    contract = DataContractMetadata(**data)
except ValidationError as e:
    error_types = set()
    for error in e.errors():
        error_types.add(error.get("type"))
    print(f"Error types: {error_types}")
    raise e
