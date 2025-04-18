# Input Data Details

- [Input Data Details](#input-data-details)
  - [Fundamentals (Contract Metadata)](#fundamentals-contract-metadata)
    - [Definitions:](#definitions)
  - [Schema](#schema)
    - [Definitions:](#definitions-1)
  - [Tags](#tags)
  - [Data Quality Rules](#data-quality-rules)
  - [Support and Communication Channels](#support-and-communication-channels)
  - [Pricing](#pricing)
  - [Team](#team)
  - [Roles](#roles)
  - [Service-Level Agreement](#service-level-agreement)
    - [QoS Periodic Table:](#qos-periodic-table)
  - [Infrastructure and Servers](#infrastructure-and-servers)
  


Note: In each section, additional key-value pairs beyond those used in this project are available per the ODCS specification and the definitions can be found in the documentation linked from each header. 

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#fundamentals">Fundamentals (Contract Metadata)</a>

This section contains general, top-level information about the contract.

```json
[
  {
    "name": "Flight Stats",
    "version": "1.0.0",    
    "domain": "flight",     
    "status": "active",     
    "dataproduct": "Passenger Flights Data Product",        --
    "tenant": "Altmiller Airlines",     --
    "description": {
      "purpose": "The data_contracts_dev schema contains information about passenger flights",      --
      "limitations": "No limitations",      --
      "usage": "All"        --
    },
    "tags": ["team:passenger_flights", "scope:test_scope"]
  }
]
```
[Source: contract_metadata.json](contract_metadata_input/contract_metadata.json)

### Definitions:
<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>name</td>
<td>Name of the data contract.</td>
</tr>
<tr>
<td>version</td>
<td>Version of the standard used to build data contract. Default value is v3.0.2.</td>
</tr>
<tr>
<td>domain</td>
<td>Name of the logical data domain.</td>
</tr>
<tr>
<td>status</td>
<td>Current status of the data contract. Examples are "proposed", "draft", "active", "deprecated", "retired".</td>
</tr>
<tr>
<td>dataproduct</td>
<td>Name of the data product.</td>
</tr>
<tr>
<td>tenant</td>
<td>Indicates the property the data is primarily associated with. Value is case insensitive.</td>
</tr>
<tr>
<td>description.purpose</td>
<td>Intended purpose for the provided data.</td>
</tr>
<tr>
<td>description.limitations</td>
<td>Technical, compliance, and legal limitations for data use.</td>
</tr>
<tr>
<td>description.usage</td>
<td>Recommended usage of the data.</td>
</tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#schema">Schema</a>

This section describes the schema of the data contract. It is the support for data quality, which is detailed in the next section. Schema supports both a business representation of your data and a physical implementation. It allows to tie them together.

In the case of the data-first approach to contract authoring, this section will be entirely generated from the Databricks workflow. 

Note the examples provided for authoring complex data types: the passengers and flight_log properties within the flights object.

```json
[
    "objects": [
        {
            "name": "flights",
            "description": "Passenger flight information",
            "properties": [
                {
                    "name": "flight_id",
                    "type": "bigint",
                    "required": true
                },
                {
                    "name": "arrival_time",
                    "type": "timestamp"
                },
                {
                    "name": "status",
                    "type": "string"
                },
                {
                    "name": "ingestion_datetime",
                    "type": "timestamp"
                },
                {
                    "name": "passengers",
                    "logicalType": "object",
                    "physicalType": "ARRAY<STRUCT<passenger_id: BIGINT, seat_number: STRING>>"
                },
                {
                    "name": "flight_log",
                    "logicalType": "object",
                    "physicalType": "STRUCT<blocks: ARRAY<STRUCT<text: STRING, title: STRING, type: STRING>>, column_additions: ARRAY<STRING>, column_deletions: ARRAY<STRING>, column_type_mismatches: STRUCT<array_of_structs: STRUCT<actual_type: STRING, expected_type: STRING>>, fail: STRUCT<greaterThan: DOUBLE, greaterThanOrEqual: DOUBLE, lessThan: DOUBLE>, missing_column_names: ARRAY<STRING>, preferredChart: STRING, present_column_names: ARRAY<STRING>, value: DOUBLE, valueLabel: STRING, valueSeries: STRUCT<values: ARRAY<STRUCT<label: STRING, outcome: STRING, value: BIGINT>>>>"
                }
            ]
        }
    ]
]
```
[Source: schema_metadata.json](/notebooks/input_data/schema_metadata_input/schema_metadata.json)

### Definitions:
<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>objects.name</td>
<td>Name of the table</td>
</tr>
<tr>
<td>objects.description</td>
<td>Description of the table</td>
</tr>
<tr>
<td>properties.name</td>
<td>Name of the field</td>
</tr>
<tr>
<td>properties.type</td>
<td>The datatype of the field. If only type is provided, the code sets logicalType and physicalType to the value of type.</td>
</tr>
<tr>
<td>properties.logicalType</td>
<td>The logical field datatype. One of string, date, number, integer, object, array or boolean.</td>
</tr>
<tr>
<td>properties.physicalType</td>
<td>The physical element data type in the data source. For example, VARCHAR(2), DOUBLE, INT.</td>
</tr>
<tr>
<td>properties.required</td>
<td>Indicates if the element may contain Null values; possible values are true and false. Default is false.</td>
</tr>
</tbody>
</table>

## Tags

While tags are fully supported by the ODCS specification and can be entered at any level of the contract, this implementation centralizes the authoring of tags inside of their own metadata file.

```json
[
  {
    "bronze": {"data_owner": "alice@databricks.com", "domain": "flights", "pii_data": "false"},
    "bronze.flight_data": {"retention_policy": "3_years", "domain": "customer_data"},
    "bronze.flight_data.flights": {"classification": "internal"},
    "bronze.flight_data.flights.origin": {"sensitivity": "medium", "pii": "false"},
    "bronze.flight_data.flights.destination": {"sensitivity": "medium", "pii": "false"}
  }
]
```
[Source: tags_metadata.json](/notebooks/input_data/tags_metadata_input/tags_metadata.json)

This implementation of tagging supports key:value pairs applied to a four-level namespace as follows: [catalog].[schema].[table].[column].

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#data-quality">Data Quality Rules</a>

This section describes data quality rules & parameters. They are tightly linked to the schema described in the previous section.

A number of different rule types are supported by ODCS and can be referenced in the link from the header, but only SQL rules are currently demonstrated within this framework.

```json
[
  {
    "passengers": {
      "quality": [
        {
          "type": "sql",
          "description": "Ensures passenger first name and last name are populated in the 'passengers' table",
          "query": "
            SELECT COUNT(*) AS null_record_count
            FROM passengers
            WHERE first_name IS NULL OR last_name IS NULL;",
          "mustBe": 0
        },
        {
          "type": "sql",
          "description": "Ensures phone numbers in the 'passengers' table are formatted correctly (XXX-XXX-XXXX)",
          "query": "
            SELECT DISTINCT phone_format_evaluation
            FROM 
            (
            SELECT 
                passenger_id,
                phone,
                CASE 
                    WHEN phone RLIKE '^[0-9]{3}-[0-9]{3}-[0-9]{4}$' 
                    THEN 0
                    ELSE 1
                END AS phone_format_evaluation
            FROM passengers
            );",
          "mustBe": 0
        }
      ]
    }
  }
]
```
[Source: data_quality_rules.json](/notebooks/input_data/data_quality_rules_input/data_quality_rules.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>type</td>
<td>Type of DQ rule. Valid values are library (default), text, sql, and custom.</td>
</tr>
<tr>
<td>description</td>
<td>Describe the quality check to be completed.</td>
</tr>
<tr>
<td>query</td>
<td>Required for sql DQ rules: the SQL query to be executed. Note that it should match the target SQL engine/database, no transalation service are provided here.</td>
</tr>
<tr>
<td>operator</td>
<td>mustBe is one example of a valid operator. The operator specifies the condition to validate the rule. Additional options are shown in the table below.</td>
</tr>
</tbody>
</table>

<table>
<thead>
<tr>
<th>Operator</th>
<th>Expected Value</th>
<th>Math Symbol</th>
<th>Example</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>mustBe</code></td>
<td>number</td>
<td><code>=</code></td>
<td><code>mustBe: 5</code></td>
</tr>
<tr>
<td><code>mustNotBe</code></td>
<td>number</td>
<td><code>&lt;&gt;</code>, <code>≠</code></td>
<td><code>mustNotBe: 3.14</code></td>
</tr>
<tr>
<td><code>mustBeGreaterThan</code></td>
<td>number</td>
<td><code>&gt;</code></td>
<td><code>mustBeGreaterThan: 59</code></td>
</tr>
<tr>
<td><code>mustBeGreaterOrEqualTo</code></td>
<td>number</td>
<td><code>&gt;=</code>, <code>≥</code></td>
<td><code>mustBeGreaterOrEqualTo: 60</code></td>
</tr>
<tr>
<td><code>mustBeLessThan</code></td>
<td>number</td>
<td><code>&lt;</code></td>
<td><code>mustBeLessThan: 1000</code></td>
</tr>
<tr>
<td><code>mustBeLessOrEqualTo</code></td>
<td>number</td>
<td><code>&lt;=</code>, <code>≤</code></td>
<td><code>mustBeLessOrEqualTo: 999</code></td>
</tr>
<tr>
<td><code>mustBeBetween</code></td>
<td>list of two numbers</td>
<td><code>⊂</code></td>
<td><code>mustBeBetween: [0, 100]</code></td>
</tr>
<tr>
<td><code>mustNotBeBetween</code></td>
<td>list of two numbers</td>
<td><code>⊄</code></td>
<td><code>mustNotBeBetween: [0, 100]</code></td>
</tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#support-and-communication-channels">Support and Communication Channels</a>

Support and communication channels help consumers find help regarding their use of the data contract.

```json
[ 
  {
    "channel": "Test Teams Channel (Interactive)",
    "tool": "teams",
    "scope": "interactive",
    "url": "https://teams.microsoft.com/channel/Test"
  },
  {
    "channel": "Test Teams Channel (Announcements)",
    "tool": "teams",
    "scope": "annoucements",
    "url": "https://teams.microsoft.com/channel/Test/announcements"
  },
  {
    "channel": "Test Email",
    "tool": "email",
    "scope": "announcements",
    "url": "mailto:test@altmiller.com",
    "description": "Team email for all team announcements"
  }
]
```
[Source: support_channel_metadata.json](/notebooks/input_data/support_channel_metadata_input/support_channel_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>channel</td>
<td>Channel name or identifier.</td>
</tr>
<tr>
<tr>
<td>tool</td>
<td>Name of the tool, value can be email, slack, teams, discord, ticket, or other.</td>
</tr>
<tr>
<tr>
<td>scope</td>
<td>Scope can be: interactive, announcements, issues.</td>
</tr>
<tr>
<tr>
<td>url</td>
<td>Access URL using normal URL scheme (https, mailto, etc.).</td>
</tr>
<tr>
<tr>
<td>description</td>
<td>Description of the channel, free text.</td>
</tr>
<tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#pricing">Pricing</a>

This section covers pricing when you bill your customer for using this data product.

```json
{
  "priceamount": "9.95",
  "pricecurrency": "USD",
  "priceunit": "megabyte"
}
```
[Source: pricing_metadata.json](/notebooks/input_data/pricing_metadata_input/pricing_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>priceAmount</td>
<td>Subscription price per unit of measure in <code>priceUnit</code>.</td>
</tr>
<tr>
<td>priceCurrency</td>
<td>Currency of the subscription price in <code>price.priceAmount</code>.</td>
</tr>
<tr>
<td>priceUnit</td>
<td>The unit of measure for calculating cost. Examples megabyte, gigabyte.</td>
</tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#team">Team</a>

This section lists team members and the history of their relation with this data contract.

```json
[
  {
    "username": "ceastwood",
    "role": "Data Scientist",
    "datein": "2024-08-02",
    "dateout": "2024-10-01",
    "replacedbyusername": "mhopper"
  },
  {
    "username": "mhopper",
    "role": "Data Scientist",
    "datein": "2024-10-01"
  },
  {
    "username": "rcrabtree",
    "role": "Data Engineer",
    "datein": "2024-10-01"
  },
  {
    "username": "daustin",
    "role": "Owner",
    "comment": "Full owner admin access",
    "name": "David Austin",
    "datein": "2024-10-01"
  }
]
```
[Source team_metadata.json](/notebooks/input_data/team_metadata_input/team_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>username</td>
<td>The user's username or email.</td>
</tr>
<tr>
<td>role</td>
<td>The user's job role; Examples might be owner, data steward. There is no limit on the role.</td>
</tr>
<tr>
<td>name</td>
<td>The user's name.</td>
</tr>
<tr>
<td>datein</td>
<td>The date when the user joined the team.</td>
</tr>
<tr>
<td>dateout</td>
<td>The date when the user ceased to be part of the team.</td>
</tr>
<tr>
<td>replacedbyusername</td>
<td>The username of the user who replaced the previous user</td>
</tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#roles">Roles</a>

This section lists the roles that a consumer may need to access the dataset depending on the type of access they require.

```json
[
  {
    "role": "data_contract_user",
    "access": "read",
    "firstlevelapprovers": "Product Domain Manager",
    "secondlevelapprovers": "Product Domain Engineer"
  },
  {
    "role": "data_contract_contributors",
    "access": "write",
    "firstlevelapprovers": "Product Domain Manager",
    "secondlevelapprovers": "Product Domain Engineer"
  },
  {
    "role": "data_contract_admins",
    "access": "admin",
    "firstlevelapprovers": "Product Domain Manager",
    "secondlevelapprovers": "Product Domain Engineer"
  },
  {
    "role": "data_contract_owners",
    "access": "owner",
    "firstlevelapprovers": "Product Domain Manager",
    "secondlevelapprovers": "Product Domain Engineer"
  }
]
```
[Source: roles_metadata.json](/notebooks/input_data/roles_metadata_input/roles_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>role</td>
<td>Name of the IAM role that provides access to the dataset.</td>
</tr>
<tr>
<td>access</td>
<td>The type of access provided by the IAM role.</td>
</tr>
<tr>
<td>firstlevelapprovers</td>
<td>The name(s) of the first-level approver(s) of the role.</td>
</tr>
<tr>
<td>secondlevelapprovers</td>
<td>The name(s) of the second-level approver(s) of the role.</td>
</tr>
</tbody>
</table>

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#service-level-agreement-sla">Service-Level Agreement</a>

This section describes the service-level agreements (SLA).

- Use the Object.Element to indicate the number to do the checks on, as in SELECT txn_ref_dt FROM tab1.
- Separate multiple object.element by a comma, as in table1.col1, table2.col1, table1.col2.
- If there is only one object in the contract, the object name is not required.

```json
[
    {
        "property": "data_freshness",
        "value": 7,
        "unit": "d",
        "element": "flights"
    },
    {
        "property": "retention",
        "value": 3,
        "unit": "y",
        "element": "flights.ingestion_datetime"
    },
    {
        "property": "frequency",
        "value": 1,
        "valueExt": 1,
        "unit": "d",
        "element": "flights.ingestion_datetime"
    },
    {
        "property": "timeOfAvailability",
        "value": "08:00-08:00",
        "element": "flights.ingestion_datetime",
        "driver": "analytics"
    }
]
```
[Source: sla_metadata.json](/notebooks/input_data/sla_metadata_input/sla_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>property</td>
<td>Specific property in SLA, check the <a href="https://medium.com/data-mesh-learning/what-is-data-qos-and-why-is-it-critical-c524b81e3cc1">Data QoS periodic table</a>. May requires units.</td>
</tr>
<tr>
<td>value</td>
<td>Agreement value. The label will change based on the property itself.</td>
</tr>
<tr>
<td>valueExt</td>
<td>Extended agreement value. The label will change based on the property itself.</td>
</tr>
<tr>
<td>unit</td>
<td>d, day, days for days; y, yr, years for years, etc. Units use the ISO standard.</td>
</tr>
<tr>
<td>element</td>
<td>Element(s) to check on. Multiple elements should be extremely rare and, if so, separated by commas.</td>
</tr>
<tr>
<td>driver</td>
<td>Describes the importance of the SLA from the list of: regulatory, analytics, or operational.</td>
</tr>
</tbody>
</table>


### QoS Periodic Table:

![QoS Periodic Table](https://miro.medium.com/v2/resize:fit:720/format:webp/1*-8fcQN6tHf0gMLZp3q_pdg.png)

## Infrastructure and Servers

The servers element describes where the data protected by this data contract is physically located. That metadata helps to know where the data is so that a data consumer can discover the data and a platform engineer can automate access.

An entry in servers describes a single dataset on a specific environment and a specific technology. The servers element can contain multiple servers, each with its own configuration.

The typical ways of using the top level servers element are as follows: - Single Server: The data contract protects a specific dataset at a specific location. Example: a CSV file on an SFTP server. - Multiple Environments: The data contract makes sure that the data is protected in all environments. Example: a data product with data in a dev(elopment), UAT, and prod(uction) environment on Databricks. - Different Technologies: The data contract makes sure that regardless of the offered technology, it still holds. Example: a data product offers its data in a Kafka topic and in a BigQuery table that should have the same structure and content. - Different Technologies and Multiple Environments: The data contract makes sure that regardless of the offered technology and environment, it still holds. Example: a data product offers its data in a Kafka topic and in a BigQuery table that should have the same structure and content in dev(elopment), UAT, and prod(uction).

```json
[
  {
    "server": "development",
    "type": "databricks",
    "host": "https://adb-4191419936804633.13.azuredatabricks.net/",
    "catalog": "hive_metastore",
    "schema": "default"
  }
]
```
[Source: server_metadata.json](/notebooks/input_data/server_metadata_input/server_metadata.json)

<table>
<thead>
<tr>
<th>Key</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>server</td>
<td>Identifier of the server.</td>
</tr>
<tr>
<td>type</td>
<td>Type of the server. Can be one of: api, athena, azure, bigquery, clickhouse, databricks, denodo, dremio, duckdb, glue, cloudsql, db2, informix, kafka, kinesis, local, mysql, oracle, postgresql, postgres, presto, pubsub, redshift, s3, sftp, snowflake, sqlserver, synapse, trino, vertica, custom.</td>
</tr>
<tr>
<td>host</td>
<td>The Databricks host</td>
</tr>
<tr>
<td>catalog</td>
<td>The name of the Hive or Unity catalog</td>
</tr>
<tr>
<td>schema</td>
<td>The schema name in the catalog</td>
</tr>
</tbody>
</table>