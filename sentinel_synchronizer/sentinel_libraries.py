from azure.identity import ClientSecretCredential, DefaultAzureCredential
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


def get_table(
    credentials,
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
    table_name: str,
):
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
    table_name: str,
    table: Table,
    credentials,
    subscription_id: str,
    resource_group_name: str,
    workspace_name: str,
):
    log_analytics_client = LogAnalyticsManagementClient(credentials, subscription_id)
    log_analytics_client.tables.begin_create_or_update(
        resource_group_name=resource_group_name,
        workspace_name=workspace_name,
        table_name=table_name,
        parameters=table,
    ).result()


def get_dce(credentials, subscription_id: str, resource_group_name: str, dce_name: str):
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
    location: str,
    credentials,
    subscription_id: str,
    resource_group_name: str,
    dce_name: str,
):
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


def get_dcr(credentials, subscription_id: str, resource_group_name: str, dcr_name: str):
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
    log_analytics_resource_group_name: str,
    log_analytics_workspace_name: str,
    log_analytics_table_schema: Schema,
    raw_stream_declaration_name: str,
    log_analytics_table_name: str,
    location: str,
    credentials,
    subscription_id: str,
    resource_group_name: str,
    dcr_name: str,
    dce_name: str,
):
    type_map = {
        ColumnTypeEnum.BOOLEAN: KnownColumnDefinitionType.BOOLEAN,
        ColumnTypeEnum.DATE_TIME: KnownColumnDefinitionType.DATETIME,
        ColumnTypeEnum.DYNAMIC: KnownColumnDefinitionType.DYNAMIC,
        ColumnTypeEnum.GUID: KnownColumnDefinitionType.STRING,
        ColumnTypeEnum.INT: KnownColumnDefinitionType.INT,
        ColumnTypeEnum.LONG: KnownColumnDefinitionType.LONG,
        ColumnTypeEnum.REAL: KnownColumnDefinitionType.REAL,
        ColumnTypeEnum.STRING: KnownColumnDefinitionType.STRING,
    }

    log_analytics_client = LogAnalyticsManagementClient(credentials, subscription_id)
    log_analytics_workspace_id = log_analytics_client.workspaces.get(
        resource_group_name=log_analytics_resource_group_name,
        workspace_name=log_analytics_workspace_name,
    ).id

    dce = get_dce(credentials, subscription_id, resource_group_name, dce_name)
    dce_id = dce.id

    raw_stream_declaration_columns = [
        ColumnDefinition(name=c.name, type=type_map[c.type])
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