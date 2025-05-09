import re
from typing import Callable
from pydantic_core import PydanticCustomError
from contract_validation.enums import ServerType


def validate_regex_pattern(pattern: str, error_message: str) -> Callable[[str], str]:
    def validate_string(string: str) -> str:
        if not re.match(pattern, string):
            raise PydanticCustomError("regex_error", error_message)
        return string

    return validate_string


def check_quality_dimensions(table_list: list) -> None:
    def _add_quality_dimensions(quality_list: list, quality_dimensions: set) -> None:
        if quality_list is not None:
            for quality in quality_list:
                quality_dimensions.add(quality.dimension.value)

    quality_dimensions = set()
    for table in table_list:
        _add_quality_dimensions(table.quality, quality_dimensions)
        for property in table.properties:
            _add_quality_dimensions(property.quality, quality_dimensions)
    if len(quality_dimensions) < 4:
        raise PydanticCustomError(
            "quality_dimensions_error",
            f"At least 4 different quality dimensions must be measured. Dimensions found: {quality_dimensions}",
        )


def validate_databricks_server(server_list: list) -> None:
    for server in server_list:
        if server.type == ServerType.DATABRICKS:
            if server.host is None:
                raise PydanticCustomError(
                    "databricks_host_error", "Databricks server must have a host"
                )
            if server.catalog is None:
                raise PydanticCustomError(
                    "databricks_catalog_error", "Databricks server must have a catalog"
                )
            if server.schema is None:
                raise PydanticCustomError(
                    "databricks_schema_error", "Databricks server must have a schema"
                )
