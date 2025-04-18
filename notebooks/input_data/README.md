# Input Data Details

- [Input Data Details](#input-data-details)
  - [Fundamentals (Contract Metadata)](#fundamentals-contract-metadata)
    - [Definitions:](#definitions)
  - [Schema](#schema)
    - [Definitions:](#definitions-1)
  - [Data Quality Rules](#data-quality-rules)
  - [Support and Communication Channels](#support-and-communication-channels)
  - [Pricing](#pricing)
  - [Team](#team)
  - [Roles](#roles)
  - [Service-Level Agreement](#service-level-agreement)
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
{
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
}
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

## <a href="https://bitol-io.github.io/open-data-contract-standard/latest/#data-quality">Data Quality Rules</a>

This section describes data quality rules & parameters. They are tightly linked to the schema described in the previous section.

A number of different rule types are supported by ODCS and can be referenced in the link from the header, but only SQL rules are currently demonstrated within this framework.

```
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

```
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

```
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

## Team

## Roles

## Service-Level Agreement

## Infrastructure and Servers