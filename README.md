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
- [Data Contract Command Line Interface (CLI)](https://github.com/datacontract/datacontract-cli)
- [Data Contract vs Data Product Specifications](https://medium.com/%40andrea_gioia/data-contract-vs-data-product-specifications-8ffa3cc16725)

## How do I use the Databricks Data Contract Cookbook?

This repository is designed to automate the generation of Open Data Contract Standard (ODCS) yaml data contracts built from a Databricks schema and all tables in the schema.  The automation is able to handle creating the entire schema, tables, columns, and datatypes - including complex nested struct types, arrays, and lists - in the data contract.  Automation also captures all the schema, table, and column comments, descriptions, and tags. Additional contract metadata from [User input files (e.g. json files)](/notebooks/input_data) are added to the data contract after the base contract has been created.  After the entire data contract has been successfully created from a Databricks schema and tables this data contract can be deployed using the 'Data Contract CLI' to a __new__ Databricks 'target catalog' and 'target schema'.

## What are the Data Contract Cookbook user input parameters?

If you are ready to create a data contract do the following:

- Fork this Github [repository](https://github.com/robert-altmiller/odcs_data_contracts), and create a feature branch off the main branch.
- Create a [Git folder](https://docs.databricks.com/aws/en/repos/repos-setup) in Databricks named 'odcs_data_contracts' under your '/Workspace/Users' folder, clone down the repository, and switch to the feature branch you just created. 
- Here are [definitions](/notebooks/input_data/README.md) for each of these data contract authoring user input files below.

- __--- DO NOT UPDATE these yet!  You will update after running the 'step 0' notebook below ---__
    
    - [High-Level Contract Metadata](/notebooks/input_data/contract_metadata_input/)
    - [Custom Data Quality Rules](/notebooks/input_data/data_quality_rules_input/)
    - [Service Level Agreements](/notebooks/input_data/sla_metadata_input/)
    - [Teams Details](/notebooks/input_data/team_metadata_input/)
    - [Roles Details](/notebooks/input_data/roles_metadata_input/)
    - [Support Channels](/notebooks/input_data/support_channel_metadata_input/)
    - [Pricing Details](/notebooks/input_data/pricing_metadata_input/)
    - [Server Details](/notebooks/input_data/server_metadata_input/)


- If you do not have any starting data (e.g. contract first approach) you will need to update the additional user input files.  The 'schema' user input file contains schema, tables, columns, column datatypes, and schema/table/column level tags and descriptions.  This 'schema' user input will be used to generate the base data contract prior to adding the metadata from the user input files listed above.

- __--- DO NOT UPDATE these yet!  You will update after running the 'step 0' notebook below ---__

    - [Schema Details](/notebooks/input_data/schema_metadata_input/)

- __PLEASE UPDATE__ the [base_params.yaml](/resources/python/base_params.yaml) with Unity Catalog (UC) volumes path where the data contract artifacts will be created.

    ![baseparams_steps.png](/readme_images/baseparams_steps.png)

## What Databricks notebook steps are run to create and deploy a data contract?

Here are the required steps to create and deploy a data contract:

- [Step 0](/notebooks/s0_data_contract_author_metadata.py) creates a set of contract authoring files for a Product Domain subject matter expert (SME) to fill out or update with data contract metadata which will be used in the next step (e.g. step 1) to create an ODCS data contract.
    
    - If you wish to run this 'step 0' notebook manually simply update the 'source_catalog' and 'source_schema' widgets in the 'Workflow Widget Parameters' block in the notebook, and run the entire Databricks 'step 0' notebook.

        ![create_contracts_step1_params.png](/readme_images/create_contracts_step1_params.png)

    - After 'step 0' completes there will be an output folder named 'sql_data' in the same location as the 'step 0' notebook if the 'source' catalog and schema have tables and data.  The 'sql_data' folder has Databricks SQL definitions (e.g. DDLs) for each table in the 'source_catalog' and 'source_schema'.  These SQL files are imported and used by the Data Contract CLI to generate the yaml 'schema' block in the 'schema' user input authoring file.

        ![sql_data_output_folder.png](/readme_images/sql_data_output_folder.png)

    - After 'step 0' completes the data contract authoring files can be found in the following location: '__/Workspace/Users/[YOUR USER EMAIL]/odcs_data_contracts_version/notebooks/input_data__'.  
    
    - __IMPORTANT__: After running 'step 0' __PLEASE UPDATE__ all the data contract authoring files to reflect your data product requirements prior to executing the next step (e.g. step 1).  You can also remove/delete any authoring user input files which do not apply to your data product requirements.  If you do this that deleted authoring user input metadata will be ommitted from the ODCS data contract after its created.
        
        - The authoring user input files which are '__MANDATORY__' and should __NOT__ be deleted are the following:
            - 'server_metadata_input'
            - 'contract_metadata_input'
            - 'schema_metadata_input'

        ![authoring_folders.png](/readme_images/authoring_folders.png)

- [Step 1](/notebooks/s1_data_contract_generate.py) creates a data contract using the authoring files created and updated in 'step 0'
    
    - If you wish to run this 'step 1' notebook manually simply update the 'source_catalog' and 'source_schema' widgets in the 'Workflow Widget Parameters' block in the notebook.  These widgets are used to construct the path to the ODCS data contract (e.g. yaml_file_path).  Next, run the entire Databricks 'step 1' notebook.

        ![create_contracts_step1_params.png](/readme_images/create_contracts_step1_params.png)

    - After 'step 1' completes there will be an output folder named 'data_contracts_data' in the same location as the 'step 1' notebook.  The name of the data contract is '{catalog_name}__{schema_name}.yaml'.

        ![data_contracts_data_folder.png](/readme_images/data_contracts_data_folder.png)

- [Step 2](/notebooks/s2_data_contract_deploy_tables.py) creates the tables + columns + column datatypes + table/column comments and descriptions defined in the data contract in a Databricks 'target catalog' and 'target schema' by running Data Contract CLI generated SQL DDLs.

    - Since 'step 2' deploys the tables in the data contract, we need to update the 'server' section in the data contract created from 'step 1' with the 'target catalog' and 'target schema' to deploy the tables to.

        ![update_data_contract_server.png](/readme_images/update_data_contract_server.png)

    - Next, to run this 'step 2' notebook manually simply update the 'source_catalog' and 'source_schema' widgets in the 'Workflow Widget Parameters' block in the notebook.  These widgets are used to construct the path to the ODCS data contract (e.g. yaml_file_path).  Next, run the entire Databricks 'step 2' notebook.
        
        ![create_contracts_step2_params.png](/readme_images/create_contracts_step2_params.png)

    - After the tables have been deployed from 'step 2' check the 'target catalog' and 'target schema' in Databricks to ensure the tables were created.  Check the column names, column data types, table and column level comments and descriptions.

- [Step 3](/notebooks/s3_data_contract_deploy_tags.py) deploys the schema level tags, and table/column level tags to the created tables in the 'target catalog' and 'target schema'.

    - If you wish to run this 'step 3' notebook manually simply update the 'source_catalog' and 'source_schema' in the 'Workflow Widget Parameters' block.  These widgets are used to construct the path to the ODCS data contract (e.g. yaml_file_path).  Next, run the entire Databricks 'step 3' notebook.

        ![create_contracts_step3_params.png](/readme_images/create_contracts_step3_params.png)

    - After the tags have been deployed from 'step 3' check all tables in the 'target catalog' and 'target schema' in Databricks to ensure all tags were added.  Check for schema, table, and column level tags.

- [Step 4](/notebooks/s4_data_contract_deploy_data.py) loads the data from the Databricks 'source schema' tables to the Databricks 'target schema' tables.  This includes loading all complex nested struct type data (e.g. json, arrays, lists).

    - If you wish to run this 'step 4' notebook manually simply update the 'source_catalog' and 'source_schema' in the 'Workflow Widget Parameters' block in the notebook.  These widgets are used to construct the path to the ODCS data contract (e.g. yaml_file_path).  Next, run the entire Databricks 'step 4' notebook.

        ![create_contracts_step4_params.png](/readme_images/create_contracts_step4_params.png)

    - After the data has been copied from the 'source schema' to the 'target schema' check all tables in the 'target catalog' and 'target schema' in Databricks to ensure they all have data.

- [Step 5](/notebooks/s5_data_contract_dq_checks.py) runs the Data Contract CLI out of the box (OOB) and user-defined data quality (DQ) SQL rules in the ODCS data contract.  The OOB data quality rules check to make sure all columns exists, and correct datatypes have been assigned.  User-defined data quality rules are specified using Databricks SQL syntax.  For example, custom data quality rules can be used to check that a table has data and no duplicates exist across all rows.

    - If you wish to run this 'step 5' notebook manually simply update the 'source_catalog' and 'source_schema' in the 'Workflow Widget Parameters' block in the notebook.  These widgets are used to construct the path to the ODCS data contract (e.g. yaml_file_path).  Next, run the entire Databricks 'step 5' notebook.

        ![create_contracts_step5_params.png](/readme_images/create_contracts_step5_params.png)

    - After the 'step 5' notebook has finished running using the Data Contract CLI test() method (e.g. data_contract_object.test()), we store the data quality test results in a Unity Catalog (UC) managed Delta table named 'odcs_data_quality' in the 'target catalog' and 'target schema'.

        ![create_contracts_step5_dq_table.png](/readme_images/create_contracts_step5_dq_table.png)

## How do I run steps 1-5 above using Databricks workflows?

If you desire to run 'step 0' above in a 's0_data_contract_author_metadata' Databricks workflow, 'step 1' above in a 's1_create_data_contract' Databricks workflow, 'steps 2-3' in a 's2_data_contract_deploy_tables_tags' Databricks workflow, 'step 3' in a 's3_data_contract_deploy_data' Databricks workflow, and 'step 5' in a 's4_data_contract_run_dq_rules' Databricks workflow we have built automation using Databricks Asset Bundles (DABs) + Github / Gitlab CICD pipelines to automate the deployment and creation of data product data contracts across environments (e.g. development, test, and production).  Continue reading to learn more about the data contract DABs + CICD integration.

![dbricks_workflows_deployed_with_dabs.png](/readme_images/dbricks_workflows_deployed_with_dabs.png)

## Automation with Databricks Asset Bundles (DABS) and CICD Overview

We maintain both '__Github actions__' and '__Gitlab actions__' workflows for deploying steps 0-5 above using four Databricks [workflows](/resources/workflows/).

Here is the repo folder structure for using the [Github actions](/pipeline_files/github/) workflow:

![github_actions.png](/readme_images/github_actions.png)

Here is the repo folder structure for using the [Gitlab actions](/pipeline_files/gitlab/) workflow.  To use this Gitlab actions workflow you will need to add a '__period__' to the front of the filename.

![gitlab_actions.png](/readme_images/gitlab_actions.png)

Here is the Databricks workflows repo folder structure.

![resources_flder_structure.png](/readme_images/resources_flder_structure.png)

The the [s0_data_contract_author_metadata_template.yaml](/resources/workflows/s0_data_contract_author_metadata_template.yaml) Databricks workflow executes [step 0](/notebooks/s0_data_contract_author_metadata.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![s0_data_contract_author_metadata_template.png](/readme_images/s0_data_contract_author_metadata_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 's0_data_contract_author_metadata.yaml' during the CICD pipeline run, and this Databricks workflow 's0_data_contract_author_metadata.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles (DABs).

The the [s1_data_contract_create_template.yaml](/resources/workflows/s1_data_contract_create_template.yaml) Databricks workflow executes [step 1](/notebooks/s1_data_contract_generate.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![s1_data_contract_create_template.png](/readme_images/s1_data_contract_create_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 's1_data_contract_create.yaml' during the CICD pipeline run, and this Databricks workflow 's1_data_contract_create.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles (DABs).

The the [s2_data_contract_deploy_tables_tags_template.yaml](/resources/workflows/s2_data_contract_deploy_tables_tags_template.yaml) Databricks workflow executes [steps 2-3](/notebooks/) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![s2_data_contract_deploy_tables_tags_template.png](/readme_images/s2_data_contract_deploy_tables_tags_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 's2_data_contract_deploy_tables_tags.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_deploy_tables_tags.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles (DABs).

The the [s3_data_contract_deploy_data_template.yaml](/resources/workflows/s3_data_contract_deploy_data_template.yaml) Databricks workflow executes [step 4](/notebooks/s4_data_contract_deploy_data.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![s3_data_contract_deploy_data_template.png](/readme_images/s3_data_contract_deploy_data_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 's3_data_contract_deploy_data.yaml' during the CICD pipeline run, and this Databricks workflow 'data_contract_deploy_data.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles (DABs).

The the [s4_data_contract_run_dq_rules_template.yaml](/resources/workflows/s4_data_contract_run_dq_rules_template.yaml) Databricks workflow executes [step 5](/notebooks/s5_data_contract_dq_checks.py) in the previous section.  This yaml template workflow is parameterized (see below), and variables from the [base_params.yaml](/resources/python/base_params.yaml) are injected into this yaml template using the [inject_base_params.py](/resources/python/inject_base_params.py) Python script.  

![s4_data_contracts4_data_contract_run_dq_rules_template_deploy_data_template.png](/readme_images/s4_data_contract_run_dq_rules_template.png)

The 'inject_base_params.py' Python script saves yaml template with updated variables as 's4_data_contract_run_dq_rules.yaml' during the CICD pipeline run, and this Databricks workflow 's4_data_contract_run_dq_rules.yaml' is deployed to the Databricks workspace using Databricks Asset Bundles (DABs).

## How do I run the DABS + CICD deployment?

__Prerequisites__: After you have forked this repository, created a feature branch, updated [UC volumes base_path](/resources/python/base_params.yaml), it is time to run the CICD pipeline to deploy the DAB and workflows to your Databricks workspace.

- Steps if running using Github:

    - Go to your forked repo, and click on 'actions'

        ![run_github_actions_workflow_s1.png](/readme_images/run_github_actions_workflow_s1.png)
        
    - Click on the workflow 'Deploy Databricks Contracts Workflows'

        ![run_github_actions_workflow_s2.png](/readme_images/run_github_actions_workflow_s2.png)

    - Fill out the 'Databricks Host URL', 'Databricks Personal Access Token (PAT)', 'Deploy Environment', 'User Email Address', 'ODCS Contract Source Catalog', and 'ODCS Contract Source Schema', and then click 'Run workflow'

        ![run_github_actions_workflow_s3.png](/readme_images/run_github_actions_workflow_s3.png)

## How do I verify a successful DABS + CICD deployment?

After the Github or Gitlab CICD pipeline run finishes a '.bundle' folder and five workflows - 's0 - data_contract_author metadata', 's1 - data_contract_create', 's2 - data_contract_deploy_tables_tags', 's3 - data_contract_deploy_data', and 's4 - data_contract_run_dq_rules' - will be created in the Databricks workspace (see below):

![bundle_deploy_step1.png](/readme_images/bundle_deploy_step1.png)

![bundle_deploy_step2.png](/readme_images/bundle_deploy_step2.png)
