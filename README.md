# Databricks System Tables to Azure Monitor

> [!WARNING]
> **Experimental** Work in progress.

This project contains a solution allowing to push to [Azure Monitor](https://learn.microsoft.com/en-us/azure/azure-monitor/) the content of Databricks [System Tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/), like [Audit logs](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs) or [Serverless Egress control logs](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/outbound-network)

The solution leverages the [Logs Ingestion APIs](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview) to push the event logs to a custom table in a Azure Logs Analytics workspace, wrapped in a [custom pyspark data source](src/sentinel_helpers/spark.py). The logs are processed incrementally, in a Lakeflow Declarative Pipeline or in a Structured Streaming job, running on serverless compute.

Today you can already push Audit events with a native [Diagnotic log delivery](https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/audit-log-delivery) solution that you can configure from the Azure Databricks workspace resource in the Azure Portal.

Databricks diagnostic has evolved incredibly in the last years with the addition of System Tables. There are 26 System Tables today that contain telemetry data of many Databricks services such as Unity Catalog, Delta Sharing, SEG Networking, among others. The native Diagnotic log delivery solution only supports 1 table, Audit events. And among such table, only workspace-level events.

This project allows you to extend the coverage of which telemetry events you can send to Azure Monitor.

We recommend analyzing telemetry data directly within Databricks rather than exporting it to external tools. This approach ensures a single source of truth for your data, avoids the costs and complexity of data synchronization, and reduces operational overhead.

That said, there are scenarios where exporting telemetry can provide significant value. One example involves Serverless Networking logs. In many organizations, cybersecurity teams are responsible for monitoring all inbound and outbound network traffic to identify potential threats. These teams often rely on centralized security tools, such as Azure Sentinel. Splitting monitoring responsibilities across different platforms can create data silos and reduce efficiency. In this case, forwarding Serverless Networking events to Azure Monitor or Azure Sentinel is a valid use case. It allows the cybersecurity team to maintain a unified view of network activity across both Classic and Serverless Databricks compute environments.

## Setup

This section will guide you through the setup of the Azure and Databricks resources to successfully deploy and run the solution.

### Unity Catalog

To read data from system tables, you need at least the following permissions:
- `USE CATALOG` on `system` catalog
- `USE SCHEMA` on the table schema (e.g. `system.access`)
- `SELECT` on the table.

> [!TIP]
> If you want to use the Structured Streaming job version of the solution, you also need to create or provide an existing UC Volume to checkpoints.

### Azure Resources

Before you run the project, you need to provision:

* an Entra ID Service Principal, and store its client secret in a secret scope
* a Log Analytics Workspace.

You also need the following roles to be assigned to the service principal:
* **Log Analytics Contributor** and **Monitoring Contributor**, to setup tables and create Data Collection Endpoints (DCE) and Data Collection Rules (DCR).
* **Monitoring Metrics Publisher**, to publish metrics on the DCEs.

You can use a single service principal or different ones, one for the setup of the resources and one for publishing the logs to the endpoints.

### Project Environment

The project contains a Databricks Asset Bundle. You can work with the bundle in [your Databricks Workspace](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/workspace) or in your computer.

#### Databricks Workspace

1. Create a [Git Folder](https://learn.microsoft.com/en-us/azure/databricks/repos/), cloning this repo.

1. Adjust configuration in the [`databricks.yml`](databricks.yml) file (more details in the next section).

1. Deploy the bundle.

1. Run the `Azure Monitor Setup` job.

1. Start the `System Tables to Azure Monitor` pipeline (recommended) or job.

#### Local Development

1. Install the latest version of [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).

2. Install [`uv`](https://docs.astral.sh/uv/getting-started/installation/).

3. Authenticate to your Databricks workspace, if not done already, and assign a profile name:

```sh
databricks auth login --host <your-workspace-url>
```

4. Set `DATABRICKS_CONFIG_PROFILE` environment variable to the name of Databricks CLI profile you configured:

```sh
export DATABRICKS_CONFIG_PROFILE=<your-profile-name>
```

5. Adjust configuration in the [`databricks.yml`](databricks.yml) file (more details in the next section).

6. Deploy the bundle:

```sh
databricks bundle deploy
```

7. Run the `Azure Monitor Setup` job:
```sh
databricks bundle run azure_monitor_setup
```

8. Start the `System Tables to Azure Monitor` pipeline (recommended) or job:

```sh
# pipeline - recommended
databricks bundle run system_tables_to_azure_monitor_pipeline
# job
databricks bundle run system_table_to_azure_monitor_job
```

## Configuration

You can adjust the configuration in the [`databricks.yml`](databricks.yml) file. 

The following variables apply to all the jobs:
* `azure_tenant_id`: the tenant ID of the Service Principal.
* `sp_client_id`: the Service Principal ID.
* `sp_client_secret_scope`: the name of the secret scope containing the Service Principal client secret.
* `sp_client_secret_key`: the name of the secret key containing the Service Principal client secret.
* `subscription_id`: the ID of the subscription containing all the resources (Log Analytics workspace, DCEs and DCRs).
* `resource_group_name`: the name of the resource group containing the DCEs and DCRs.
* `audit_log_table_name`: the name of the table for audit log in the Log Analytics workspace, default to `AdbSystemAccessAudit`.
* `outbound_network_table_name`: the name of the table for outbound network logs in the Log Analytics workspace, default to `AdbSystemAccessOutboundNetwork`.

The following variables apply to the setup job only:
* `log_analytics_resource_group_name`: the name of the resource group containing the Log Analytics workspace.
* `log_analytics_workspace_name`: the name of the Log Analytics workspace hosting the tables.
* `location`: the location where DCEs and DCRs are created in.

The following variables apply to both the job and the pipeline:
* `starting_datetime`: the datetime for filtering the events, e.g. `2025-08-11` or `2025-08-11`.
* `include_workspace_ids`: a JSON string, containing the list for filtering the workspace ID to include in the process.
* `exclude_workspace_ids`: a JSON string, containing the list for filtering the workspace ID to exclude from the process.

The following variables apply to thee pipeline only:
* `pipeline_continuous_mode`: if `true`, the pipeline runs continuously. It defaults to `false`, i.e. the pipeline processes all the available records and then stops.
* `pipeline_processing_time`: if specified, it overrides the default pipeline processing time. Applicable only if the pipeline runs in continuous mode.
* `catalog_name`: the pipeline's catalog name. 
* `schema_name`: the pipeline's schema name.

The following variable applies to the job only:
* `checkpoint_root_location`: the checkpoint location for the structured streaming checkpoints, i.e. a path to a Unity Catalog volume.


## Supported Tables

Currently, we support the following system tables:

| Table        | Full Name                        | Setup notebook |
|  ---         |     ---                          | ---            |
| Audit Logs    | `system.access.audit`            | [audit_setup](src/audit_setup.ipynb)  | 
| Serverless Egress Control Logs      | `system.access.outbound_network` | [network_setup](src/network_setup.ipynb) |


## Costs

We run the pipeline and collected the following figures:

| Mode          | # Events      |  DBUs     | List Cost     | Ingestion Rate    |
|  ---          |  ---          |  --       |   ---         |   ---             |
| Triggered     |  43M          |  2        |   $ 1.00      | 11M rows / minute, 12k requests / minute |
| Continous     |  800k / h     |  5 / h    |   $ 2.30 / h  | 200k rows / minute, 200 requests / minute |


## TODOs

- [x] Adopt [custom spark connector](https://github.com/alexott/cyber-spark-data-connectors)
- [x] Check if we need to remove the rate limit options in `readStream`
- [x] Trigger interval in the pipeline
- [x] Not IN workspace_id
- [ ] Use environments instead of installing the wheel explicitely

## Authors

* Sergio Schena - [sergio.schena@databricks.com](mailto:sergio.schena@databricks.com)
* Mattia Zeni - [mattia.zeni@databricks.com](mailto:mattia.zeni@databricks.com)

## Credits

Thanks to **Alex Ott** ([alexey.ott@databricks.com](mailto:alexey.ott@databricks.com)), for inspiring the usage of a custom pyspark data source, instead of foreachBatch. 
In particular:
* [Cyber Spark Data Connectors](https://github.com/alexott/cyber-spark-data-connectors/blob/main/cyber_connectors/MsSentinel.py)
* [Cybersecurity Playground](https://github.com/alexott/databricks-cybersecurity-playground/blob/main/dlt_modern_stuff/src/detections.py)
