# Databricks Data Contract Cookbook

#### Link to Github Repo: https://github.com/robert-altmiller/odcs_data_contracts

## What are data contracts?

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


## How do I use the Databricks Data Contract Cookbook?

This repository is designed to automate the generation of Open Data Contract Standard (ODCS) YAML data contracts from a Databricks schema and all tables in the schema.  The automation is able to handle creating the entire schema, tables, columns, and datatypes - including complex nested struct types, arrays, and lists - in the data contract.  Automation also captures all the schema, table, and column comments, descriptions, and tags.  [User defined inputs (e.g. json files)](/notebooks/input_data) define the following:

- [high-level contract metadata](/notebooks/input_data/contract_metadata_input/)
- [custom data quality rules](/notebooks/input_data/data_quality_rules_input/)
- [service level agreements](/notebooks/input_data/sla_metadata_input/)
- [teams](/notebooks/input_data/team_metadata_input/)
- [roles](/notebooks/input_data/roles_metadata_input/)
- [support channels](/notebooks/input_data/support_channel_metadata_input/)
- [pricing](/notebooks/input_data/pricing_metadata_input/)
- [server details](/notebooks/input_data/server_metadata_input/)

These user inputs are added to the data contract after the base contract has been created.  After the entire data contract has been successfully created from a Databricks schema and tables this data contract can be deployed using the Data Contract CLI to a __new__ Databricks 'target catalog' and 'target schema'.

Here are the steps outlined above:

- The [first step](/notebooks/s1_data_contract_generate.py) is to create a data contract from a 'source' Databricks schema and tables.
- The [second step](/notebooks/s2_data_contract_deploy_tables.py) is to create the tables + columns + column datatypes + table/column comments and descriptions defined in the data contract in a Databricks 'target' schema by running Data Contract CLI generated SQL DDLs.
- The [third step](/notebooks/s3_data_contract_deploy_tags.py) is to deploy the schema level tags, and table/column level tags to the created tables in the 'target' schema.
- The [fourth step](/notebooks/s4_data_contract_deploy_data.py) is to load the data from the Databricks 'source' schema tables to the Databricks 'target' schema tables.  This includes loading all complex nested struct type data.
- The [fifth step](/notebooks/s5_data_contract_dq_checks.py) is to run the Data Contract CLI out of the box (OOB) and user-defined data quality (DQ) SQL rules.  The OOB rules check to make sure all columns exists, and correct datatypes have been assigned.  User-defined data quality rules are specified using Databricks SQL syntax.  For example, custom rules can be used to check that a table has data and no duplicates exist across all rows.

## Automation with Databricks Asset Bundles (DABS) and CICD

We maintain both '__Github actions__' and '__Gitlab actions__' workflows for deploying steps 1-5 above using two Databricks [workflows](/resources/workflows/).

Here is the repo folder structure for using the [Github actions](/pipeline_files/github/) workflow:

![github_actions.png](/readme_images/github_actions.png)

Here is the repo folder structure for using the [Gitlab actions](/pipeline_files/gitlab/) workflow.  To use this Gitlab actions workflow you will need to add a '__period__' to the front of the filename.

![gitlab_actions.png](/readme_images/gitlab_actions.png)

The the [data_contract_create_template.yaml](/resources/workflows/data_contract_create_template.yaml) Databricks workflow executes ['step 1'](/notebooks/s1_data_contract_generate.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![data_contract_create_template.png](/readme_images/data_contract_create_template.png)

The Python script saves yaml template with updated variables as 'data_contract_create.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_create.yaml' is deployed to the Databricks workspace using DABs.

The the [data_contract_deploy_template.yaml](/resources/workflows/data_contract_deploy_template.yaml) Databricks workflow executes ['steps 2-5'](/notebooks/) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![data_contract_deploy_template.png](/readme_images/data_contract_deploy_template.png)

The Python script saves yaml template with updated variables as 'data_contract_deploy.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_deploy.yaml' is deployed to the Databricks workspace using DABs.

![resources_folder_structure.png](/readme_images/resources_folder_structure.png)