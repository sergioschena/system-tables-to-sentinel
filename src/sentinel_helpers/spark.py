
import json
from dataclasses import dataclass
from datetime import date, datetime

from azure.monitor.ingestion import LogsIngestionClient
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


class DateTimeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime) or isinstance(o, date):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


class AzureMonitorDataSource(DataSource):
    """Data source for Azure Monitor. Right now supports writing to Azure Monitor via REST API.

    Write options:
    - dce_url: data collection endpoint URL
    - dcr_id: data collection rule ID
    - dcs: data collection stream name
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret
    - body_col: (Optional) Column name of the JSON message body
    """

    @classmethod
    def name(cls):
        return "azure-monitor"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return AzureMonitorStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return AzureMonitorBatchWriter(self.options)


# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-ingestion-readme?view=azure-python
class AzureMonitorWriter:
    def __init__(self, options):
        self.options = options
        self.dce_url = self.options.get("dce_url")  # data_collection_endpoint
        self.dcr_id = self.options.get("dcr_id")  # data_collection_rule_id
        self.dcs = self.options.get("dcs")  # data_collection_stream
        self.tenant_id = self.options.get("tenant_id")
        self.client_id = self.options.get("client_id")
        self.client_secret = self.options.get("client_secret")
        self.body_col = self.options.get("body_col")
        self.batch_size_rows = int(self.options.get("batch_size_rows", "10000"))
        self.batch_size_bytes = int(self.options.get("batch_size_bytes", "1000000"))
        for required in ("dce_url", "dcr_id", "dcs", "tenant_id", "client_id", "client_secret"):
            if self.options.get(required) is None:
                raise ValueError(f"Missing required option: '{required}'")
        if self.batch_size_bytes > 1_000_000:
            raise ValueError(f"batch_size_bytes ({self.batch_size_bytes}) exceeds the Azure Monitor limit of 1,000,000 bytes")

    def _send_to_sentinel(self, s: LogsIngestionClient, msgs: list):
        from azure.core.exceptions import HttpResponseError

        try:
            s.upload(rule_id=self.dcr_id, stream_name=self.dcs, logs=msgs)
        except HttpResponseError as e:
            print(f"Upload failed: {e}")

    def write(self, iterator):
        """Writes the data, then returns the commit message of that partition. Library imports must be within the method."""
        import json

        from azure.identity import ClientSecretCredential
        from azure.monitor.ingestion import LogsIngestionClient
        from pyspark import TaskContext

        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        logs_client = LogsIngestionClient(self.dce_url, credential)

        msgs = []
        current_batch_size = 0
        cnt = 0

        context = TaskContext.get()
        partition_id = context.partitionId()

        for row in iterator:
            if self.body_col is None:
                #  Workaround to convert datetime/date to string
                row_str = json.dumps(row.asDict(), cls=DateTimeJsonEncoder)
            else:
                row_str = row[self.body_col]

            if (len(row_str) > (self.batch_size_bytes - current_batch_size)) or (len(msgs) >= self.batch_size_rows):
                self._send_to_sentinel(logs_client, msgs)
                current_batch_size = 0
                msgs = []
            
            # Add the current row to the batch
            msgs.append(json.loads(row_str))
            current_batch_size += len(row_str)
            cnt += 1

        if current_batch_size > 0:
            self._send_to_sentinel(logs_client, msgs)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class AzureMonitorBatchWriter(AzureMonitorWriter, DataSourceWriter):
    pass


class AzureMonitorStreamWriter(AzureMonitorWriter, DataSourceStreamWriter):
    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        pass