from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceNotFoundError

from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.mgmt.loganalytics.models import Table, Schema, Column, ColumnTypeEnum

from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import (
    DataCollectionEndpointResource,
    DataCollectionEndpointNetworkAcls,
    KnownPublicNetworkAccessOptions,
    DataCollectionRuleResource,
    StreamDeclaration,
    DataCollectionRuleDestinations,
    DataFlow,
    ColumnDefinition,
    KnownColumnDefinitionType,
    LogAnalyticsDestination,
)

_COLUMN_TYPE_MAP: dict[ColumnTypeEnum, KnownColumnDefinitionType] = {
    ColumnTypeEnum.BOOLEAN: KnownColumnDefinitionType.BOOLEAN,
    ColumnTypeEnum.DATE_TIME: KnownColumnDefinitionType.DATETIME,
    ColumnTypeEnum.DYNAMIC: KnownColumnDefinitionType.DYNAMIC,
    ColumnTypeEnum.GUID: KnownColumnDefinitionType.STRING,
    ColumnTypeEnum.INT: KnownColumnDefinitionType.INT,
    ColumnTypeEnum.LONG: KnownColumnDefinitionType.LONG,
    ColumnTypeEnum.REAL: KnownColumnDefinitionType.REAL,
    ColumnTypeEnum.STRING: KnownColumnDefinitionType.STRING,
}


def get_table(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    table_name: str,
) -> Table | None:
    log_analytics_client = LogAnalyticsManagementClient(credentials, subscription_id)
    try:
        return log_analytics_client.tables.get(
            resource_group_name=resource_group_name,
            workspace_name=workspace_name,
            table_name=table_name,
        )
    except ResourceNotFoundError:
        return None


def create_table(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    table_name: str,
    table: Table,
) -> None:
    log_analytics_client = LogAnalyticsManagementClient(credentials, subscription_id)
    log_analytics_client.tables.begin_create_or_update(
        resource_group_name=resource_group_name,
        workspace_name=workspace_name,
        table_name=table_name,
        parameters=table,
    ).result()


def get_dce(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    dce_name: str,
) -> DataCollectionEndpointResource | None:
    monitor_management_client = MonitorManagementClient(
        credentials, subscription_id, api_version="2022-06-01"
    )
    try:
        return monitor_management_client.data_collection_endpoints.get(
            resource_group_name, dce_name
        )
    except ResourceNotFoundError:
        return None


def create_dce(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    dce_name: str,
    location: str,
) -> DataCollectionEndpointResource:
    monitor_management_client = MonitorManagementClient(
        credentials, subscription_id, api_version="2022-06-01"
    )
    data_collection_endpoint = DataCollectionEndpointResource(
        location=location,
        network_acls=DataCollectionEndpointNetworkAcls(
            public_network_access=KnownPublicNetworkAccessOptions.ENABLED
        ),
    )
    return monitor_management_client.data_collection_endpoints.create(
        resource_group_name=resource_group_name,
        data_collection_endpoint_name=dce_name,
        body=data_collection_endpoint,
    )


def get_dcr(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    dcr_name: str,
) -> DataCollectionRuleResource | None:
    monitor_management_client = MonitorManagementClient(
        credentials, subscription_id, api_version="2022-06-01"
    )
    try:
        return monitor_management_client.data_collection_rules.get(
            resource_group_name, dcr_name
        )
    except ResourceNotFoundError:
        return None


def create_dcr(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    dce_name: str,
    dcr_name: str,
    location: str,
    log_analytics_resource_group_name: str,
    log_analytics_workspace_name: str,
    log_analytics_table_schema: Schema,
    log_analytics_table_name: str,
    raw_stream_declaration_name: str,
) -> DataCollectionRuleResource:
    log_analytics_client = LogAnalyticsManagementClient(credentials, subscription_id)
    log_analytics_workspace_id = log_analytics_client.workspaces.get(
        resource_group_name=log_analytics_resource_group_name,
        workspace_name=log_analytics_workspace_name,
    ).id

    dce_id = get_dce(credentials, subscription_id, resource_group_name, dce_name).id

    raw_stream_declaration_columns = [
        ColumnDefinition(name=c.name, type=_COLUMN_TYPE_MAP[c.type])
        for c in log_analytics_table_schema.columns
        if c.name != "TimeIngested"
    ]
    raw_stream_declaration = StreamDeclaration(columns=raw_stream_declaration_columns)

    destinations = DataCollectionRuleDestinations(
        log_analytics=[
            LogAnalyticsDestination(
                workspace_resource_id=log_analytics_workspace_id,
                name=log_analytics_workspace_name,
            )
        ]
    )

    dataflows = [
        DataFlow(
            streams=[raw_stream_declaration_name],
            destinations=[log_analytics_workspace_name],
            transform_kql="source | extend TimeIngested = now()",
            output_stream=f"Custom-{log_analytics_table_name}",
        )
    ]

    data_collection_rule = DataCollectionRuleResource(
        location=location,
        stream_declarations={raw_stream_declaration_name: raw_stream_declaration},
        destinations=destinations,
        data_flows=dataflows,
        data_collection_endpoint_id=dce_id,
    )

    monitor_management_client = MonitorManagementClient(
        credentials, subscription_id, api_version="2022-06-01"
    )
    return monitor_management_client.data_collection_rules.create(
        resource_group_name=resource_group_name,
        data_collection_rule_name=dcr_name,
        body=data_collection_rule,
    )


def ensure_azure_resources(
    credentials: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    location: str,
    log_analytics_resource_group_name: str,
    log_analytics_workspace_name: str,
    table_name: str,
    table_schema: Schema,
    retention_in_days: int = 180,
) -> None:
    """Idempotently creates the Log Analytics table, DCE, and DCR for a system table pipeline."""
    log_analytics_table_name = f"{table_name}_CL"
    dce_name = f"{table_name}-dce"
    dcr_name = f"{table_name}-dcr"
    raw_stream_declaration_name = f"Custom-{table_name}RawData"

    table = Table(retention_in_days=retention_in_days, schema=table_schema)
    if not get_table(credentials, subscription_id, log_analytics_resource_group_name, log_analytics_workspace_name, log_analytics_table_name):
        create_table(credentials, subscription_id, log_analytics_resource_group_name, log_analytics_workspace_name, log_analytics_table_name, table)
        print(f"✅ Table {log_analytics_table_name} created!")
    else:
        print(f"⏩ Table creation skipped: {log_analytics_table_name} already exists.")

    if not get_dce(credentials, subscription_id, resource_group_name, dce_name):
        create_dce(credentials, subscription_id, resource_group_name, dce_name, location)
        print(f"✅ DCE {dce_name} created!")
    else:
        print(f"⏩ DCE creation skipped: {dce_name} already exists.")

    if not get_dcr(credentials, subscription_id, resource_group_name, dcr_name):
        create_dcr(credentials, subscription_id, resource_group_name, dce_name, dcr_name, location, log_analytics_resource_group_name, log_analytics_workspace_name, table_schema, log_analytics_table_name, raw_stream_declaration_name)
        print(f"✅ DCR {dcr_name} created!")
    else:
        print(f"⏩ DCR creation skipped: {dcr_name} already exists.")
