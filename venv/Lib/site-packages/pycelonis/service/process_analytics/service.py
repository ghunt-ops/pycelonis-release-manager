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


class KpiSource(PyCelonisBaseEnum):
    LOCAL = "LOCAL"
    KNOWLEDGE_MODEL = "KNOWLEDGE_MODEL"


class DataPermissionStrategy(PyCelonisBaseEnum):
    AND = "AND"
    OR = "OR"


class FrontendHandledBackendError(PyCelonisBaseModel):
    frontend_error_key: Optional['str'] = Field(None, alias="frontendErrorKey")
    error_information: Optional['Any'] = Field(None, alias="errorInformation")


class AnalysisPackageConfig(PyCelonisBaseModel):
    root_node_key: Optional['str'] = Field(None, alias="rootNodeKey")
    id: Optional['str'] = Field(None, alias="id")
    parent_node_key: Optional['str'] = Field(None, alias="parentNodeKey")
    parent_node_id: Optional['str'] = Field(None, alias="parentNodeId")
    name: Optional['str'] = Field(None, alias="name")
    data_model_id: Optional['str'] = Field(None, alias="dataModelId")
    key: Optional['str'] = Field(None, alias="key")
    root_node_id: Optional['str'] = Field(None, alias="rootNodeId")
    knowledge_model_key: Optional['str'] = Field(None, alias="knowledgeModelKey")
    change_default_event_log: Optional['bool'] = Field(None, alias="changeDefaultEventLog")
    event_log: Optional['str'] = Field(None, alias="eventLog")
    custom_dimension: Optional['str'] = Field(None, alias="customDimension")


class AnalysisPackageTransport(PyCelonisBaseModel):
    analysis: Optional['AnalysisTransport'] = Field(None, alias="analysis")
    kpis: Optional['List[Optional[KpiTransport]]'] = Field(None, alias="kpis")
    draft: Optional['DraftTransport'] = Field(None, alias="draft")
    data_model_id: Optional['str'] = Field(None, alias="dataModelId")
    knowledge_model_key: Optional['str'] = Field(None, alias="knowledgeModelKey")
    next_draft_creation_date_time: Optional['datetime'] = Field(None, alias="nextDraftCreationDateTime")
    change_default_event_log: Optional['bool'] = Field(None, alias="changeDefaultEventLog")
    event_log: Optional['str'] = Field(None, alias="eventLog")
    custom_dimension: Optional['str'] = Field(None, alias="customDimension")


class AnalysisTransport(PyCelonisBaseModel):
    permissions: Optional['List[Optional[str]]'] = Field(None, alias="permissions")
    id: Optional['str'] = Field(None, alias="id")
    tenant_id: Optional['str'] = Field(None, alias="tenantId")
    name: Optional['str'] = Field(None, alias="name")
    key: Optional['str'] = Field(None, alias="key")
    description: Optional['str'] = Field(None, alias="description")
    deleted: Optional['bool'] = Field(None, alias="deleted")
    transport_id: Optional['str'] = Field(None, alias="transportId")
    last_published_draft_id: Optional['str'] = Field(None, alias="lastPublishedDraftId")
    auto_save_id: Optional['str'] = Field(None, alias="autoSaveId")
    process_id: Optional['str'] = Field(None, alias="processId")
    create_date: Optional['datetime'] = Field(None, alias="createDate")
    favourite: Optional['bool'] = Field(None, alias="favourite")
    content_id: Optional['str'] = Field(None, alias="contentId")
    content_version: Optional['int'] = Field(None, alias="contentVersion")
    tags: Optional['List[Optional[Tag]]'] = Field(None, alias="tags")
    application_id: Optional['str'] = Field(None, alias="applicationId")
    global_app: Optional['bool'] = Field(None, alias="globalApp")
    public_link: Optional['bool'] = Field(None, alias="publicLink")
    last_published_date: Optional['datetime'] = Field(None, alias="lastPublishedDate")
    last_published_user: Optional['str'] = Field(None, alias="lastPublishedUser")
    parent_object_id: Optional['str'] = Field(None, alias="parentObjectId")
    published_draft_id: Optional['str'] = Field(None, alias="publishedDraftId")
    folder_id: Optional['str'] = Field(None, alias="folderId")
    object_id: Optional['str'] = Field(None, alias="objectId")
    application: Optional['bool'] = Field(None, alias="application")


class DraftTransport(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    title: Optional['str'] = Field(None, alias="title")
    document: Optional['Any'] = Field(None, alias="document")
    last_change_date: Optional['datetime'] = Field(None, alias="lastChangeDate")
    last_change_user_id: Optional['str'] = Field(None, alias="lastChangeUserId")
    last_change_user_name: Optional['str'] = Field(None, alias="lastChangeUserName")
    locked_until_date: Optional['datetime'] = Field(None, alias="lockedUntilDate")
    source_id: Optional['str'] = Field(None, alias="sourceId")


class KpiTransport(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    name: Optional['str'] = Field(None, alias="name")
    description: Optional['str'] = Field(None, alias="description")
    template: Optional['str'] = Field(None, alias="template")
    parameters: Optional['Any'] = Field(None, alias="parameters")
    parameter_count: Optional['int'] = Field(None, alias="parameterCount")
    source: Optional['KpiSource'] = Field(None, alias="source")
    unit: Optional['str'] = Field(None, alias="unit")
    value_format: Optional['str'] = Field(None, alias="valueFormat")


class Tag(PyCelonisBaseModel):
    name: Optional['str'] = Field(None, alias="name")


class DataCommand(PyCelonisBaseModel):
    cube_id: Optional['str'] = Field(None, alias="cubeId")
    commands: Optional['List[Optional[DataQuery]]'] = Field(None, alias="commands")


class DataCommandBatchRequest(PyCelonisBaseModel):
    variables: Optional['List[Optional[Variable]]'] = Field(None, alias="variables")
    requests: Optional['List[Optional[DataCommandBatchTransport]]'] = Field(None, alias="requests")


class DataCommandBatchTransport(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    request: Optional['DataCommand'] = Field(None, alias="request")


class DataQuery(PyCelonisBaseModel):
    computation_id: Optional['int'] = Field(None, alias="computationId")
    queries: Optional['List[Optional[str]]'] = Field(None, alias="queries")
    is_transient: Optional['bool'] = Field(None, alias="isTransient")


class Variable(PyCelonisBaseModel):
    name: Optional['str'] = Field(None, alias="name")
    type_: Optional['str'] = Field(None, alias="type")
    value: Optional['str'] = Field(None, alias="value")


class DataPermissionRule(PyCelonisBaseModel):
    values: Optional['List[Optional[str]]'] = Field(None, alias="values")
    column_id: Optional['str'] = Field(None, alias="columnId")
    table_id: Optional['str'] = Field(None, alias="tableId")


class Kpi(PyCelonisBaseModel):
    name: Optional['str'] = Field(None, alias="name")
    template: Optional['str'] = Field(None, alias="template")
    parameter_count: Optional['int'] = Field(None, alias="parameterCount")
    error: Optional['str'] = Field(None, alias="error")
    formula: Optional['str'] = Field(None, alias="formula")


class KpiInformation(PyCelonisBaseModel):
    kpis: Optional['Dict[str, Optional[Kpi]]'] = Field(None, alias="kpis")


class PostBatchQueryTransport(PyCelonisBaseModel):
    analysis_commands: Optional['List[Optional[DataCommandBatchTransport]]'] = Field(None, alias="analysisCommands")
    query_environment: Optional['QueryEnvironment'] = Field(None, alias="queryEnvironment")


class QueryEnvironment(PyCelonisBaseModel):
    accelerator_session_id: Optional['str'] = Field(None, alias="acceleratorSessionId")
    process_id: Optional['str'] = Field(None, alias="processId")
    user_id: Optional['str'] = Field(None, alias="userId")
    user_name: Optional['str'] = Field(None, alias="userName")
    load_script: Optional['str'] = Field(None, alias="loadScript")
    kpi_infos: Optional['KpiInformation'] = Field(None, alias="kpiInfos")
    data_permission_rules: Optional['List[Optional[DataPermissionRule]]'] = Field(None, alias="dataPermissionRules")
    data_permission_strategy: Optional['DataPermissionStrategy'] = Field(None, alias="dataPermissionStrategy")


FrontendHandledBackendError.update_forward_refs()
AnalysisPackageConfig.update_forward_refs()
AnalysisPackageTransport.update_forward_refs()
AnalysisTransport.update_forward_refs()
DraftTransport.update_forward_refs()
KpiTransport.update_forward_refs()
Tag.update_forward_refs()
DataCommand.update_forward_refs()
DataCommandBatchRequest.update_forward_refs()
DataCommandBatchTransport.update_forward_refs()
DataQuery.update_forward_refs()
Variable.update_forward_refs()
DataPermissionRule.update_forward_refs()
Kpi.update_forward_refs()
KpiInformation.update_forward_refs()
PostBatchQueryTransport.update_forward_refs()
QueryEnvironment.update_forward_refs()


class ProcessAnalyticsService:
    @staticmethod
    def post_analysis_v2_api_analysis(
        client: Client, request_body: AnalysisPackageConfig, **kwargs: Any
    ) -> AnalysisPackageTransport:
        logger.debug(
            f"Request: 'POST' -> '/process-analytics/analysis/v2/api/analysis'",
            extra={
                "request_type": "POST",
                "path": "/process-analytics/analysis/v2/api/analysis",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/process-analytics/analysis/v2/api/analysis",
            request_body=request_body,
            parse_json=True,
            type_=AnalysisPackageTransport,
            **kwargs,
        )

    @staticmethod
    def put_analysis_v2_api_analysis_analysis_id_autosave(
        client: Client,
        analysis_id: str,
        request_body: DraftTransport,
        release: Optional['bool'] = None,
        source_id: Optional['str'] = None,
        **kwargs: Any,
    ) -> DraftTransport:
        logger.debug(
            f"Request: 'PUT' -> '/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosave'",
            extra={
                "request_type": "PUT",
                "path": "/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosave",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if release is not None:
            if isinstance(release, PyCelonisBaseModel):
                params.update(release.json_dict(by_alias=True))
            elif isinstance(release, dict):
                params.update(release)
            else:
                params["release"] = release
        if source_id is not None:
            if isinstance(source_id, PyCelonisBaseModel):
                params.update(source_id.json_dict(by_alias=True))
            elif isinstance(source_id, dict):
                params.update(source_id)
            else:
                params["sourceId"] = source_id
        return client.request(
            method="PUT",
            url=f"/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosave",
            params=params,
            request_body=request_body,
            parse_json=True,
            type_=DraftTransport,
            **kwargs,
        )

    @staticmethod
    def put_analysis_v2_api_analysis_analysis_id_autosave_scope(
        client: Client, analysis_id: str, request_body: DraftTransport, source_id: Optional['str'] = None, **kwargs: Any
    ) -> None:
        logger.debug(
            f"Request: 'PUT' -> '/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosaveScope'",
            extra={
                "request_type": "PUT",
                "path": "/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosaveScope",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if source_id is not None:
            if isinstance(source_id, PyCelonisBaseModel):
                params.update(source_id.json_dict(by_alias=True))
            elif isinstance(source_id, dict):
                params.update(source_id)
            else:
                params["sourceId"] = source_id
        return client.request(
            method="PUT",
            url=f"/process-analytics/analysis/v2/api/analysis/{analysis_id}/autosaveScope",
            params=params,
            request_body=request_body,
            **kwargs,
        )

    @staticmethod
    def post_analysis_v2_api_analysis_analysis_id_kpi(
        client: Client, analysis_id: str, request_body: KpiTransport, **kwargs: Any
    ) -> KpiTransport:
        logger.debug(
            f"Request: 'POST' -> '/process-analytics/analysis/v2/api/analysis/{analysis_id}/kpi'",
            extra={
                "request_type": "POST",
                "path": "/process-analytics/analysis/v2/api/analysis/{analysis_id}/kpi",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/process-analytics/analysis/v2/api/analysis/{analysis_id}/kpi",
            request_body=request_body,
            parse_json=True,
            type_=KpiTransport,
            **kwargs,
        )

    @staticmethod
    def post_analysis_v2_api_analysis_analysis_id_data_command(
        client: Client, analysis_id: str, request_body: DataCommandBatchRequest, **kwargs: Any
    ) -> PostBatchQueryTransport:
        logger.debug(
            f"Request: 'POST' -> '/process-analytics/analysis/v2/api/analysis/{analysis_id}/data_command'",
            extra={
                "request_type": "POST",
                "path": "/process-analytics/analysis/v2/api/analysis/{analysis_id}/data_command",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/process-analytics/analysis/v2/api/analysis/{analysis_id}/data_command",
            request_body=request_body,
            parse_json=True,
            type_=PostBatchQueryTransport,
            **kwargs,
        )
