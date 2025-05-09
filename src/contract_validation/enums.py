from enum import Enum


class ContractStatus(Enum):
    ACTIVE = "active"
    DRAFT = "draft"
    PROPOSED = "proposed"
    DEPRECATED = "deprecated"
    RETIRED = "retired"

class ContractDomain(Enum):
    FLIGHT = "flight"
    WEATHER = "weather"
    TRAFFIC = "traffic"
    OTHER = "other"

class SupportTool(Enum):
    EMAIL = "email"
    TEAMS = "teams"
    SLACK = "slack"
    DISCORD = "discord"
    TICKET = "ticket"
    OTHER = "other"

class SupportScope(Enum):
    INTERACTIVE = "interactive"
    ANNOUNCEMENTS = "announcements"
    ISSUES = "issues"

class QualityType(Enum):
    LIBRARY = "library"
    TEXT = "text"
    SQL = "sql"
    CUSTOM = "custom"

class QualityDimension(Enum):
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    COVERAGE = "coverage"
    CONFORMITY = "conformity"

class ServiceLevelUnits(Enum):
    d = "days"
    day = "days"
    days = "days"
    y = "years"
    yr = "years"
    years = "years"
    
class ServerType(Enum):
    API = "api"
    ATHENA = "athena"
    AZURE = "azure"
    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DATABRICKS = "databricks"
    DENODO = "denodo"
    DREMIO = "dremio"
    DUCKDB = "duckdb"
    GLUE = "glue"
    CLOUDSQL = "cloudsql"
    DB2 = "db2"
    INFORMIX = "informix"
    KAFKA = "kafka"
    KINESIS = "kinesis"
    LOCAL = "local"
    MYSQL = "mysql"
    ORACLE = "oracle"
    POSTGRESQL = "postgresql"
    POSTGRES = "postgres"
    PRESTO = "presto"
    PUBSUB = "pubsub"
    REDSHIFT = "redshift"
    S3 = "s3"
    SFTP = "sftp"
    SNOWFLAKE = "snowflake"
    SQLSERVER = "sqlserver"
    SYNAPSE = "synapse"
    TRINO = "trino"
    VERTICA = "vertica"
    CUSTOM = "custom"
