# from typing import Optional, Annotated, Self, Any
from typing import Annotated, Any, Optional

from pydantic import AfterValidator, BaseModel, Field, model_validator

from contract_validation.enums import (
    ContractDomain,
    ContractStatus,
    QualityDimension,
    QualityType,
    ServerType,
    ServiceLevelUnits,
    SupportScope,
    SupportTool,
)
from contract_validation.validation_functions import (
    check_quality_dimensions,
    validate_databricks_server,
    validate_regex_pattern,
)


class AuthoritativeDefinition(BaseModel):
    label: str = Field(description="The label of the authoritative definition")
    url: str = Field(description="The url of the authoritative definition")


class ContractDescription(BaseModel):
    purpose: str = Field(description="Intended purpose for the provided data.")
    limitations: Optional[str] = Field(
        default=None,
        description="Technical, compliance, and legal limitations for data use.",
    )
    usage: str = Field(description="Recommended usage of the data.")
    definitions: Optional[list[AuthoritativeDefinition]] = Field(
        default=None, description="A list of authoritative definitions for the contract"
    )


class PricingMetadata(BaseModel):
    priceAmount: float = Field(
        description="The amount of money to charge for the product"
    )
    priceCurrency: str = Field(description="The currency of the price")
    priceUnit: str = Field(description="The unit of the price")


class SupportMetadata(BaseModel):
    channel: str = Field(description="Channel name or identifier.")
    tool: SupportTool = Field(
        description="Name of the tool, value can be email, slack, teams, discord, ticket, or other."
    )
    scope: SupportScope = Field(
        description="Scope can be: interactive, announcements, issues."
    )
    url: Annotated[
        str,
        AfterValidator(
            validate_regex_pattern(
                r"^(https:\/\/|mailto:)[^\s]+$",
                "String must be a valid url scheme (https://, mailto:, etc.).",
            )
        ),
    ] = Field(description="Access URL using normal URL scheme (https, mailto, etc.).")
    description: Optional[str] = Field(
        default=None, description="The description of the support"
    )


class QualityMetadata(BaseModel):
    type: QualityType = Field(
        description="Type of DQ rule. Valid values are library (default), text, sql, and custom."
    )
    description: str = Field(description="Describe the quality check to be completed.")
    dimension: QualityDimension = Field(
        description="The key performance indicator (KPI) or dimension for data quality. Valid values are completeness, accuracy, consistency, timeliness, uniqueness, coverage, and conformity."
    )
    query: Optional[str] = Field(
        default=None,
        description="Required for sql DQ rules: the SQL query to be executed. Note that it should match the target SQL engine/database, no transalation service are provided here.",
    )
    mustBe: Optional[int | float] = None
    mustNotBe: Optional[int | float] = None
    mustBeGreaterThan: Optional[int | float] = None
    mustBeGreaterThan: Optional[int | float] = None
    mustBeLessThan: Optional[int | float] = None
    mustBeLessThan: Optional[int | float] = None
    mustBeBetween: Optional[tuple[int | float, int | float]] = None
    mustNotBeBetween: Optional[tuple[int | float, int | float]] = None

    @model_validator(mode="after")
    def check_valid_sql_type(self) -> "QualityMetadata":
        if self.type == "sql":
            must_be_fields = [
                getattr(self, field)
                for field in QualityMetadata.model_fields.keys()
                if field.startswith("must")
            ]
            if not any(field is not None for field in must_be_fields):
                raise ValueError(
                    "At least one operator property must be provided when type is 'sql'."
                )
            if self.query is None:
                raise ValueError("Query is required when type is 'sql'.")
        return self


class PropertyMetadata(BaseModel):
    name: str = Field(description="The name of the property")
    logicalType: str = Field(description="The logical type of the property")
    physicalType: str = Field(description="The physical type of the property")
    description: str = Field(
        description="The description of the property", min_length=5
    )
    tags: Optional[list[str]] = Field(
        description="The tags of the property", default=None
    )
    quality: Optional[list[QualityMetadata]] = Field(
        default=None, description="The quality of the property"
    )


class SchemaMetadata(BaseModel):
    name: str = Field(description="The name of the schema")
    physicalName: str = Field(description="The physical name of the schema")
    logicalType: str = Field(description="The logical type of the schema")
    physicalType: str = Field(description="The physical type of the schema")
    properties: list[PropertyMetadata] = Field(
        description="The properties of the schema"
    )
    description: str = Field(description="The description of the schema", min_length=5)
    tags: list[str] = Field(description="The tags of the schema", min_length=1)
    quality: Optional[list[QualityMetadata]] = Field(
        default=None, description="The quality of the schema"
    )


class ServiceLevelProperty(BaseModel):
    property: str = Field(
        description="Specific property in SLA, check the Data QoS periodic table. May requires units."
    )
    value: Any = Field(
        description="Agreement value. The label will change based on the property itself."
    )
    unit: Optional[ServiceLevelUnits] = Field(
        default=None,
        description="d, day, days for days; y, yr, years for years, etc. Units use the ISO standard.",
    )
    element: Optional[str] = Field(
        default=None,
        description="Element(s) to check on. Multiple elements should be extremely rare and, if so, separated by commas.",
    )


class ServerMetadata(BaseModel):
    server: str = Field(description="Identifier of the server.")
    type: ServerType = Field(
        description="Type of the server. Can be one of: api, athena, azure, bigquery, clickhouse, databricks, denodo, dremio, duckdb, glue, cloudsql, db2, informix, kafka, kinesis, local, mysql, oracle, postgresql, postgres, presto, pubsub, redshift, s3, sftp, snowflake, sqlserver, synapse, trino, vertica, custom."
    )
    host: Optional[str] = Field(description="The host of the server", default=None)
    catalog: Optional[str] = Field(
        description="The catalog of the server", default=None
    )
    schema: Optional[str] = Field(description="The schema of the server", default=None)


class DataContractMetadata(BaseModel):
    apiVersion: Annotated[
        str,
        AfterValidator(
            validate_regex_pattern(
                r"^v\d+\.\d+\.\d+$",
                "String must match the pattern vX.X.X. Example: v3.0.2",
            )
        ),
    ] = Field(
        description="Version of the standard used to build data contract.",
    )
    version: Annotated[
        str,
        AfterValidator(
            validate_regex_pattern(
                r"^\d+\.\d+\.\d+$",
                "String must match the pattern X.X.X. Example: 1.0.0",
            )
        ),
    ] = Field(description="Current version of the data contract.")
    domain: Optional[ContractDomain] = Field(
        None, description="Name of the logical data domain."
    )
    status: ContractStatus = Field(
        default=ContractStatus.DRAFT, description="The status of the contract"
    )
    dataProduct: str = Field(
        description="Name of the data product.", min_length=6, max_length=50
    )
    tenant: Optional[str] = Field(
        default=None,
        description="Indicates the property the data is primarily associated with. Value is case insensitive.",
    )
    description: ContractDescription = Field(
        description="The description of the contract"
    )
    tags: Optional[list[str]] = Field(
        description="The tags of the contract", default=None
    )
    price: Optional[PricingMetadata] = Field(
        default=None, description="The pricing details of the contract"
    )
    support: list[SupportMetadata] = Field(
        description="Support and Communication Channels", min_length=2
    )
    schema: Annotated[
        list[SchemaMetadata],
        AfterValidator(check_quality_dimensions),
    ] = Field(description="The schema of the contract")
    slaDefaultElement: Optional[str] = Field(
        description="The default element of the service level agreement (using the element path notation) to do the checks on.",
        default=None,
    )
    slaProperties: Optional[list[ServiceLevelProperty]] = Field(
        default=None,
        description="A list of key/value pairs for SLA specific properties. There is no limit on the type of properties.",
    )
    servers: Annotated[
        list[ServerMetadata],
        AfterValidator(validate_databricks_server),
    ] = Field(
        description="A list of servers to be used in the contract",
        min_length=1,
    )
