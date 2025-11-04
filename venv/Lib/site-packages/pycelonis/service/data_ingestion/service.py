import logging
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional, Union

from pycelonis_core.base.base_model import (PyCelonisBaseEnum,
                                            PyCelonisBaseModel)
from pycelonis_core.client.client import Client
from pycelonis_core.utils.ml_workbench import TRACKING_LOGGER

try:
    from pydantic.v1 import (Field, StrictBool, StrictInt,  # type: ignore
                             StrictStr)
except ImportError:
    from pydantic import (Field, StrictBool, StrictInt,  # type: ignore
                          StrictStr)


logger = logging.getLogger(TRACKING_LOGGER)


class ColumnType(PyCelonisBaseEnum):
    INTEGER = "INTEGER"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    STRING = "STRING"


class DataPushUpsertStrategy(PyCelonisBaseEnum):
    UPSERT_WITH_UNCHANGED_METADATA = "UPSERT_WITH_UNCHANGED_METADATA"
    UPSERT_WITH_NULLIFICATION = "UPSERT_WITH_NULLIFICATION"


class JobStatus(PyCelonisBaseEnum):
    NEW = "NEW"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELED = "CANCELED"


class JobType(PyCelonisBaseEnum):
    REPLACE = "REPLACE"
    DELTA = "DELTA"


class UploadFileType(PyCelonisBaseEnum):
    PARQUET = "PARQUET"
    CSV = "CSV"


class ChunkType(PyCelonisBaseEnum):
    UPSERT = "UPSERT"
    DELETE = "DELETE"


class ValidationExceptionDescriptor(PyCelonisBaseModel):
    errors: Optional['List[Optional[ValidationError]]'] = Field(None, alias="errors")


class ValidationError(PyCelonisBaseModel):
    attribute: Optional['str'] = Field(None, alias="attribute")
    error: Optional['str'] = Field(None, alias="error")
    error_code: Optional['str'] = Field(None, alias="errorCode")
    additional_info: Optional['str'] = Field(None, alias="additionalInfo")


class ExceptionReference(PyCelonisBaseModel):
    reference: Optional['str'] = Field(None, alias="reference")
    message: Optional['str'] = Field(None, alias="message")
    short_message: Optional['str'] = Field(None, alias="shortMessage")


class CsvColumnParsingOptions(PyCelonisBaseModel):
    column_name: Optional['str'] = Field(None, alias="columnName")
    date_format: Optional['str'] = Field(None, alias="dateFormat")
    thousands_separator: Optional['str'] = Field(None, alias="thousandsSeparator")
    decimal_separator: Optional['str'] = Field(None, alias="decimalSeparator")


class CsvParsingOptions(PyCelonisBaseModel):
    escape_sequence: Optional['str'] = Field(None, alias="escapeSequence")
    quote_sequence: Optional['str'] = Field(None, alias="quoteSequence")
    separator_sequence: Optional['str'] = Field(None, alias="separatorSequence")
    line_ending: Optional['str'] = Field(None, alias="lineEnding")
    char_set: Optional['str'] = Field(None, alias="charSet")
    decimal_separator: Optional['str'] = Field(None, alias="decimalSeparator")
    thousand_separator: Optional['str'] = Field(None, alias="thousandSeparator")
    date_format: Optional['str'] = Field(None, alias="dateFormat")
    additional_column_options: Optional['List[Optional[CsvColumnParsingOptions]]'] = Field(
        None, alias="additionalColumnOptions"
    )


class ColumnTransport(PyCelonisBaseModel):
    column_name: Optional['str'] = Field(None, alias="columnName")
    column_type: Optional['ColumnType'] = Field(None, alias="columnType")
    field_length: Optional['int'] = Field(None, alias="fieldLength")
    decimals: Optional['int'] = Field(None, alias="decimals")
    pk_field: Optional['bool'] = Field(None, alias="pkField")


class DataPushJob(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    target_name: Optional['str'] = Field(None, alias="targetName")
    last_modified: Optional['datetime'] = Field(None, alias="lastModified")
    last_ping: Optional['datetime'] = Field(None, alias="lastPing")
    status: Optional['JobStatus'] = Field(None, alias="status")
    type_: Optional['JobType'] = Field(None, alias="type")
    file_type: Optional['UploadFileType'] = Field(None, alias="fileType")
    target_schema: Optional['str'] = Field(None, alias="targetSchema")
    upsert_strategy: Optional['DataPushUpsertStrategy'] = Field(None, alias="upsertStrategy")
    fallback_varchar_length: Optional['int'] = Field(None, alias="fallbackVarcharLength")
    data_pool_id: Optional['str'] = Field(None, alias="dataPoolId")
    connection_id: Optional['str'] = Field(None, alias="connectionId")
    post_execution_query: Optional['str'] = Field(None, alias="postExecutionQuery")
    allow_duplicate: Optional['bool'] = Field(None, alias="allowDuplicate")
    foreign_keys: Optional['str'] = Field(None, alias="foreignKeys")
    keys: Optional['List[Optional[str]]'] = Field(None, alias="keys")
    logs: Optional['List[Optional[str]]'] = Field(None, alias="logs")
    table_schema: Optional['TableTransport'] = Field(None, alias="tableSchema")
    csv_parsing_options: Optional['CsvParsingOptions'] = Field(None, alias="csvParsingOptions")
    mirror_target_names: Optional['List[Optional[str]]'] = Field(None, alias="mirrorTargetNames")
    change_date: Optional['datetime'] = Field(None, alias="changeDate")


class TableTransport(PyCelonisBaseModel):
    table_name: Optional['str'] = Field(None, alias="tableName")
    columns: Optional['List[Optional[ColumnTransport]]'] = Field(None, alias="columns")


class DataPushChunk(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    creation_date: Optional['datetime'] = Field(None, alias="creationDate")
    type_: Optional['ChunkType'] = Field(None, alias="type")
    push_job_id: Optional['str'] = Field(None, alias="pushJobId")
    checksum: Optional['str'] = Field(None, alias="checksum")


ValidationExceptionDescriptor.update_forward_refs()
ValidationError.update_forward_refs()
ExceptionReference.update_forward_refs()
CsvColumnParsingOptions.update_forward_refs()
CsvParsingOptions.update_forward_refs()
ColumnTransport.update_forward_refs()
DataPushJob.update_forward_refs()
TableTransport.update_forward_refs()
DataPushChunk.update_forward_refs()


class DataIngestionService:
    @staticmethod
    def get_api_v1_data_push_pool_id_jobs_id(client: Client, pool_id: str, id: str, **kwargs: Any) -> DataPushJob:
        logger.debug(
            f"Request: 'GET' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}'",
            extra={
                "request_type": "GET",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}",
            parse_json=True,
            type_=DataPushJob,
            **kwargs,
        )

    @staticmethod
    def post_api_v1_data_push_pool_id_jobs_id(
        client: Client, pool_id: str, id: str, duplicate_removal_column: Optional['str'] = None, **kwargs: Any
    ) -> None:
        logger.debug(
            f"Request: 'POST' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}'",
            extra={
                "request_type": "POST",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if duplicate_removal_column is not None:
            if isinstance(duplicate_removal_column, PyCelonisBaseModel):
                params.update(duplicate_removal_column.json_dict(by_alias=True))
            elif isinstance(duplicate_removal_column, dict):
                params.update(duplicate_removal_column)
            else:
                params["duplicateRemovalColumn"] = duplicate_removal_column
        return client.request(
            method="POST", url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}", params=params, **kwargs
        )

    @staticmethod
    def delete_api_v1_data_push_pool_id_jobs_id(client: Client, pool_id: str, id: str, **kwargs: Any) -> None:
        logger.debug(
            f"Request: 'DELETE' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}'",
            extra={
                "request_type": "DELETE",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(method="DELETE", url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}", **kwargs)

    @staticmethod
    def post_api_v1_data_push_pool_id_jobs_id_chunks_upserted(
        client: Client, pool_id: str, id: str, request_body: Dict[str, Any], **kwargs: Any
    ) -> None:
        logger.debug(
            f"Request: 'POST' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/upserted'",
            extra={
                "request_type": "POST",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/upserted",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/upserted",
            request_body=request_body,
            **kwargs,
        )

    @staticmethod
    def post_api_v1_data_push_pool_id_jobs_id_chunks_deleted(
        client: Client, pool_id: str, id: str, request_body: Dict[str, Any], **kwargs: Any
    ) -> None:
        logger.debug(
            f"Request: 'POST' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/deleted'",
            extra={
                "request_type": "POST",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/deleted",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks/deleted",
            request_body=request_body,
            **kwargs,
        )

    @staticmethod
    def get_api_v1_data_push_pool_id_jobs(client: Client, pool_id: str, **kwargs: Any) -> List[Optional[DataPushJob]]:
        logger.debug(
            f"Request: 'GET' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/'",
            extra={
                "request_type": "GET",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/",
            parse_json=True,
            type_=List[Optional[DataPushJob]],
            **kwargs,
        )

    @staticmethod
    def post_api_v1_data_push_pool_id_jobs(
        client: Client, pool_id: str, request_body: DataPushJob, **kwargs: Any
    ) -> DataPushJob:
        logger.debug(
            f"Request: 'POST' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/'",
            extra={
                "request_type": "POST",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/",
            request_body=request_body,
            parse_json=True,
            type_=DataPushJob,
            **kwargs,
        )

    @staticmethod
    def get_api_v1_data_push_pool_id_jobs_id_chunks(
        client: Client, pool_id: str, id: str, **kwargs: Any
    ) -> List[Optional[DataPushChunk]]:
        logger.debug(
            f"Request: 'GET' -> '/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks'",
            extra={
                "request_type": "GET",
                "path": "/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/data-ingestion/api/v1/data-push/{pool_id}/jobs/{id}/chunks",
            parse_json=True,
            type_=List[Optional[DataPushChunk]],
            **kwargs,
        )
