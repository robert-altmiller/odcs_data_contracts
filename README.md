# Databricks Data Contract Cookbook

Operational Data Contract Specification (ODCS) data contracts are structured agreements between data producers and consumers that define the expectations, structure, and quality of shared data. These contracts are written in YAML and follow a standardized specification to ensure consistency across teams and platforms. They include schema definitions, metadata, and semantic tags, and they serve as the source of truth for how data should look and behave—enabling validation, documentation, and automation across the data lifecycle. Within ODCS, data contracts help enforce schema governance, improve data quality, and streamline collaboration between data producers and consumers in large-scale data platforms and data meshes.

__Key Components of ODCS:__

- __Fundamentals__: Defines core metadata such as contract ID, name, version, domain, tenant, and descriptive fields like purpose and usage. ​
- __Schema__: Outlines the logical and physical structure of data assets (e.g. columns and datatypes), and supports various data models including relational tables and nested documents.
- __Data Quality__: Specifies quality rules and metrics, such as row counts, uniqueness constraints, and value validity. These can be expressed in plain text, SQL, or through integrations with tools like Soda and Great Expectations.
- __Support & Communication Channels__: Details contact information and support mechanisms for stakeholders involved in the data contract. ​
- __Pricing__: Provides optional information on the cost associated with accessing or using the data product (e.g. cost per megabyte)
- __Team and Roles__: Identifies stakeholders, their roles, and responsibilities.  Team and roles enable role-based access control and accountability. ​
- __Service Level Agreements (SLAs)__: Define expectations for data availability, latency, and other performance metrics to ensure reliable data delivery. ​
- __Infrastructure & Servers__: Describes the environments where data resides, supports multiple platforms and technologies such as Azure, Databricks, Kafka, and PostgreSQL. ​
- __Custom Properties__: Allows for the inclusion of additional metadata to accommodate specific organizational needs or tooling requirements. 

__ODCS Helpful Links:__

- [Introduction to Data Contracts](https://www.foundational.io/blog/introduction-data-contracts)
- [Bitol IO ODCS Documentation](https://bitol-io.github.io/open-data-contract-standard/latest/)
- [Data Contract Manager ODCS Documentation](https://datacontract-manager.com/learn/open-data-contract-standard)
- [Data Contract vs Data Product Specifications](https://medium.com/%40andrea_gioia/data-contract-vs-data-product-specifications-8ffa3cc16725)

__How do I use the Databricks Data Contract Cookbook:__

This repository is designed to automate the generation of Open Data Contract Standard (ODCS) YAML data contracts from a Databricks schema and all tables in the schema.  The automation is able to handle creating the entire schema, tables, columns, and datatypes - including complex nested struct types, arrays, and lists - in the data contract.  Automation also captures all the schema, table, and column comments, descriptions, and tags.  User defined inputs (e.g. json files) define the high level contract metadata, data quality rules, teams and roles, pricing, server details, and support channels.  These user inputs are added to the data contract after the base contract has been created.  

After the entire data contract has been successfully created from a Databricks schema and tables this data contract can be deployed using the Data Contract CLI to a __new__ Databricks 'target catalog' and 'target schema':

- The first step is to create the tables + columns + column datatypes + table/column comments and descriptions defined in the data contract in a Databricks target schema by running Data Contract CLI generated SQL DDLs.
- The second step is to deploy the tags to the created tables in the target schema.
- The third step is to load the data from the Databricks source schema tables to the Databricks target schema tables.  This includes loading all complex nested struct type data.
- The fourth step is to run the Data Contract CLI out of the box (OOB) and user-defined data quality SQL rules.  The OOB rules check to make sure all columns exists, correct datatypes have been assigned.  User-defined data quality rules are specified using Databricks SQL syntax.  For example, rules can be used to to check and make sure a table has data and no duplicates exist across all rows.

