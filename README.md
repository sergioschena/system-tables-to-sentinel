# Databricks System Tables to Microsoft Sentinel

> [!WARNING]
> **Experimental** Work in progress.

This repo contains a simple implementation for a Structured Streaming job to push Databricks System Tables to Microsoft Sentinel.

## Usage

The (main notebook)[sentinel_synchronizer/main.ipynb] is responsible for reading the data from a system table and push it to the relevant Data Collection Endpoint (and so to the relevant Log Analytics table).
The notebooks run a Structured Streaming job, either in triggered mode (i.e., `availableNow`) or continous mode (i.e., `processingTime`)

Notebook parameters:
* `system_table_name` (string, required) - Full name of the source, e.g., `system.access.audit`
* `log_analytics_table_name` (string, required) - Destination Log Analytics table name, e.g., `AdbSystemAccessAudit`
* `checkpoint_volume_location` (string, required) - Full volume path for the checkpoint folder, e.g., `Volume/main/default/checkpoints/sentinel_audit_log`
* `starting_datetime` (string, required) - Start date time, used to filter the source table, e.g., `2024-12-01 00:00:00`
* `workspace_ids` (string, optional) - JSON string containing a list of workspace IDs, used for filtering, e.g., `["1231232423432"]`. If empty, no filtering is applied.
* `processing_time` (string, optional) - Processing time for **continous mode**, e.g., `30 seconds`, or empty for **trigger mode**. Default to `1 minute`.
* `tenant_id` (string, required) - Service Principal tenant ID
* `sp_client_id` (string, required) - Service Principal client ID
* `sp_client_secret_scope` (string, required) - secret scope name containing the SP client secret
* `sp_client_secret_key` (string, required) - secret key name containing the SP client secret
* `subscription_id` (string, required) - subscription ID of monitoring resources
* `resource_group_name` (string, required) - resource group name for DCEs and DCRs

## Setup

### Unity Catalog

To read data from system tables, you need at least the following permissions:
- `USE CATALOG` on `system` catalog
- `USE SCHEMA` on the table schema (e.g. `system.access`)
- `SELECT` on the table.

You will also need a UC Volume to save the Structured Streaming checkpoints.

### Azure Resources

Befor you run the project, you need to have:

* Entra ID Service Principal, and store its client secret in a secret scope
* Log Analytics Workspace

You also need to assign the correct roles to the service principal as following:
* **Log Analytics Contributor** and **Monitoring Contributor**, to setup tables and create Data Collection Endpoints (DCE) and Data Collection Rules (DCR)
* **Monitoring Metrics Publisher**, to publish metrics on the DCEs.

You can use a single service principal or different ones.

### Tables

Currently, we support the following system tables:

| Table        | Full Name                        | Setup notebook |
|  ---         |     ---                          | ---            |
| Audit Log    | `system.access.audit`            | [audit_setup_dcr_with_dce](sentinel_synchronizer/audit_setup_dcr_with_dce.ipynb)  | 
| SEG Log      | `system.access.outbound_network` | [network_setup_dcr_with_dce](sentinel_synchronizer/network_setup_dcr_with_dce.ipynb) |

The setup notebooks perform the following tasks:
1. Create the table in Log Analytics
1. Create a dedicated Data Collection Endpoint for the table
1. Create a Data Collection Rule, targeting the table in Log Analytics and using the dedicated DCE

Notebook parameters:
* `tenant_id` (string, required) - Service Principal tenant ID
* `sp_client_id` (string, required) - Service Principal client ID
* `sp_client_secret_scope` (string, required) - secret scope name containing the SP client secret
* `sp_client_secret_key` (string, required) - secret key name containing the SP client secret
* `subscription_id` (string, required) - subscription ID of monitoring resources
* `resource_group_name` (string, required) - resource group name for DCEs and DCRs
* `location` (string, required) - location for DCEs and DCRs
* `log_analytics_workspace_name` (string, required) - Log Analytics workspace name
* `log_analytics_resource_group_name` (string, required) - Log Analytics resource group name
* `table_name` (string, optional) - Log Analytics table name

## TODOs

- [] Adopt [custom spark connector](https://github.com/alexott/cyber-spark-data-connectors)
- [] Table names from a fixed configuration in the library

## Authors

* Sergio Schena - [sergio.schena@databricks.com](mailto:sergio.schena@databricks.com)
* Mattia Zeni - [mattia.zeni@databricks.com](mailto:mattia.zeni@databricks.com)