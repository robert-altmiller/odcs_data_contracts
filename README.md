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

This repository is designed to automate the generation of Open Data Contract Standard (ODCS) yaml data contracts built from a Databricks schema and all tables in the schema.  The automation is able to handle creating the entire schema, tables, columns, and datatypes - including complex nested struct types, arrays, and lists - in the data contract.  Automation also captures all the schema, table, and column comments, descriptions, and tags. Additional contract metadata from [User input files (e.g. json files)](/notebooks/input_data) are added to the data contract after the base contract has been created.  After the entire data contract has been successfully created from a Databricks schema and tables this data contract can be deployed using the 'Data Contract CLI' to a __new__ Databricks 'target catalog' and 'target schema'.

## What are the Data Contract Cookbook User Input Parameters?

If you are ready to create a data contract do the following:

- Fork this Github [repository](https://github.com/robert-altmiller/odcs_data_contracts), and create a feature branch off the main branch.
- Update all of these data contract [json metadata](/notebooks/input_data) user input files with your data product specifications and requirements, and commit the changes back to your feature branch.  Here are [definitions](http://www.google.com) for each of these JSON user input files.
    - [High-Level Contract Metadata](/notebooks/input_data/contract_metadata_input/)
    - [Custom Data Quality Rules](/notebooks/input_data/data_quality_rules_input/)
    - [Service Level Agreements](/notebooks/input_data/sla_metadata_input/)
    - [Teams](/notebooks/input_data/team_metadata_input/)
    - [Roles](/notebooks/input_data/roles_metadata_input/)
    - [Support Channels](/notebooks/input_data/support_channel_metadata_input/)
    - [Pricing](/notebooks/input_data/pricing_metadata_input/)
    - [Server Details](/notebooks/input_data/server_metadata_input/)
- Update the [base_params.yaml](/resources/python/base_params.yaml) with Unity Catalog (UC) volumes path where the data contract artifacts will be created.  Also update the '__git_source__', '__git_repo_url__', and '__git_branch__' with your forked repo requirements.

![base_params.png](/readme_images/base_params.png)

## What Databricks notebook steps are run to create and deploy a data contract?

Here are the required steps to create and deploy a data contract:

- The [first step](/notebooks/s1_data_contract_generate.py) is to create a data contract from a 'source' Databricks schema and tables.
    
    - If you wish to run this 'step 1' notebook manually simply update the 'source_catalog' and 'source_schema' widgets in the 'Workflow Widget Parameters' block in the notebook, and run the entire Databricks notebook.

        ![create_contracts_step1_params.png](/readme_images/create_contracts_step1_params.png)

    - After 'step 1' completes the output folders will look like the following.  The name of the data contract is '{catalog_name}__{schema_name}.yaml'.

        ![create_contracts_step1_output_fldrs.png](/readme_images/create_contracts_step1_output_fldrs.png)

- The [second step](/notebooks/s2_data_contract_deploy_tables.py) is to create the tables + columns + column datatypes + table/column comments and descriptions defined in the data contract in a Databricks 'target catalog' and 'target schema' by running Data Contract CLI generated SQL DDLs.

    - Since 'step 2' deploys the tables in the data contract we need to update the 'server' section in the data contract created from 'step 1' with the 'target catalog' and 'target schema' to deploy the tables to.

        ![update_data_contract_server.png](/readme_images/update_data_contract_server.png)

    - Next, to run this 'step 2' notebook manually simply update the 'source_catalog' and 'source_schema' widgets in the 'Workflow Widget Parameters' block in the notebook, and run the entire Databricks notebook.
        
        - ![deploy_contracts_step2_params.png](/readme_images/deploy_contracts_step2_params.png)

    - After the tables have been deployed from 'step 2' check the 'target catalog' and 'target schema' in Databricks to ensure the tables were created.  Check the column names, column data types, table and column level comments and descriptions.

- The [third step](/notebooks/s3_data_contract_deploy_tags.py) is to deploy the schema level tags, and table/column level tags to the created tables in the 'target' schema.

    - If you wish to run this 'step 3' notebook manually simply update the 'yaml_file_path' in the 'Workflow Widget Parameters' block in the notebook to point to the location of the created contract, and run the entire Databricks notebook.

        ![create_contracts_step3_params.png](/readme_images/create_contracts_step3_params.png)

- The [fourth step](/notebooks/s4_data_contract_deploy_data.py) is to load the data from the Databricks 'source' schema tables to the Databricks 'target' schema tables.  This includes loading all complex nested struct type data.
- The [fifth step](/notebooks/s5_data_contract_dq_checks.py) is to run the Data Contract CLI out of the box (OOB) and user-defined data quality (DQ) SQL rules.  The OOB rules check to make sure all columns exists, and correct datatypes have been assigned.  User-defined data quality rules are specified using Databricks SQL syntax.  For example, custom rules can be used to check that a table has data and no duplicates exist across all rows.

## Automation with Databricks Asset Bundles (DABS) and CICD

We maintain both '__Github actions__' and '__Gitlab actions__' workflows for deploying steps 1-5 above using two Databricks [workflows](/resources/workflows/).

Here is the repo folder structure for using the [Github actions](/pipeline_files/github/) workflow:

![github_actions.png](/readme_images/github_actions.png)

Here is the repo folder structure for using the [Gitlab actions](/pipeline_files/gitlab/) workflow.  To use this Gitlab actions workflow you will need to add a '__period__' to the front of the filename.

![gitlab_actions.png](/readme_images/gitlab_actions.png)

Here is the Databricks workflows repo folder structure:

![resources_folder_structure.png](/readme_images/resources_folder_structure.png)

The the [data_contract_create_template.yaml](/resources/workflows/data_contract_create_template.yaml) Databricks workflow executes [step 1](/notebooks/s1_data_contract_generate.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![data_contract_create_template.png](/readme_images/data_contract_create_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 'data_contract_create.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_create.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles.

The the [data_contract_deploy_template.yaml](/resources/workflows/data_contract_deploy_template.yaml) Databricks workflow executes [steps 2-5](/notebooks/) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![data_contract_deploy_template.png](/readme_images/data_contract_deploy_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 'data_contract_deploy.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_deploy.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles.