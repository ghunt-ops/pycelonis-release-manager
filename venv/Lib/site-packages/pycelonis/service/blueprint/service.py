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


JsonNode = Any


class BoardAssetType(PyCelonisBaseEnum):
    BOARD = "BOARD"
    BOARD_V2 = "BOARD_V2"


class InputDataType(PyCelonisBaseEnum):
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"
    PQL = "PQL"
    LIST = "LIST"
    DATE = "DATE"
    OBJECT = "OBJECT"


class InputScope(PyCelonisBaseEnum):
    USER_SPECIFIC = "USER_SPECIFIC"
    SYSTEM = "SYSTEM"


class ContentNodeType(PyCelonisBaseEnum):
    ASSET = "ASSET"
    PACKAGE = "PACKAGE"
    FOLDER = "FOLDER"
    IMAGE = "IMAGE"


class CascadeType(PyCelonisBaseEnum):
    DELETE = "DELETE"


class RelationType(PyCelonisBaseEnum):
    USES = "USES"
    DEPENDS_ON = "DEPENDS_ON"


class BoardUpsertRequest(PyCelonisBaseModel):
    board_asset_type: Optional['BoardAssetType'] = Field(None, alias="boardAssetType")
    configuration: Optional['str'] = Field(None, alias="configuration")
    id: Optional['str'] = Field(None, alias="id")
    input_variable_definitions: Optional['List[Optional[InputVariableDefinitionTransport]]'] = Field(
        None, alias="inputVariableDefinitions"
    )
    parent_node_id: Optional['str'] = Field(None, alias="parentNodeId")
    parent_node_key: Optional['str'] = Field(None, alias="parentNodeKey")
    root_node_key: Optional['str'] = Field(None, alias="rootNodeKey")


class InputVariableDefinitionTransport(PyCelonisBaseModel):
    data_type: Optional['InputDataType'] = Field(None, alias="dataType")
    default_value: Optional['str'] = Field(None, alias="defaultValue")
    description: Optional['str'] = Field(None, alias="description")
    display_name: Optional['str'] = Field(None, alias="displayName")
    key: Optional['str'] = Field(None, alias="key")
    propagate: Optional['bool'] = Field(None, alias="propagate")
    scope: Optional['InputScope'] = Field(None, alias="scope")


class ExceptionReference(PyCelonisBaseModel):
    message: Optional['str'] = Field(None, alias="message")
    reference: Optional['str'] = Field(None, alias="reference")
    short_message: Optional['str'] = Field(None, alias="shortMessage")


class ContentNodeTransport(PyCelonisBaseModel):
    activated_draft_id: Optional['str'] = Field(None, alias="activatedDraftId")
    asset: Optional['bool'] = Field(None, alias="asset")
    asset_metadata_transport: Optional['AssetMetadataTransport'] = Field(None, alias="assetMetadataTransport")
    asset_type: Optional['str'] = Field(None, alias="assetType")
    base: Optional['ContentNodeBaseTransport'] = Field(None, alias="base")
    change_date: Optional['datetime'] = Field(None, alias="changeDate")
    created_by_id: Optional['str'] = Field(None, alias="createdById")
    creation_date: Optional['datetime'] = Field(None, alias="creationDate")
    deleted_at: Optional['datetime'] = Field(None, alias="deletedAt")
    deleted_by: Optional['str'] = Field(None, alias="deletedBy")
    draft_id: Optional['str'] = Field(None, alias="draftId")
    embeddable: Optional['bool'] = Field(None, alias="embeddable")
    id: Optional['str'] = Field(None, alias="id")
    identifier: Optional['str'] = Field(None, alias="identifier")
    input_variable_definitions: Optional['List[Optional[InputVariableDefinitionTransport]]'] = Field(
        None, alias="inputVariableDefinitions"
    )
    invalid_content: Optional['bool'] = Field(None, alias="invalidContent")
    key: Optional['str'] = Field(None, alias="key")
    name: Optional['str'] = Field(None, alias="name")
    node_type: Optional['ContentNodeType'] = Field(None, alias="nodeType")
    object_id: Optional['str'] = Field(None, alias="objectId")
    order: Optional['int'] = Field(None, alias="order")
    parent_node_id: Optional['str'] = Field(None, alias="parentNodeId")
    parent_node_key: Optional['str'] = Field(None, alias="parentNodeKey")
    permissions: Optional['List[Optional[str]]'] = Field(None, alias="permissions")
    public_available: Optional['bool'] = Field(None, alias="publicAvailable")
    root: Optional['bool'] = Field(None, alias="root")
    root_node_id: Optional['str'] = Field(None, alias="rootNodeId")
    root_node_key: Optional['str'] = Field(None, alias="rootNodeKey")
    root_with_key: Optional['str'] = Field(None, alias="rootWithKey")
    schema_version: Optional['int'] = Field(None, alias="schemaVersion")
    serialization_type: Optional['str'] = Field(None, alias="serializationType")
    serialized_content: Optional['str'] = Field(None, alias="serializedContent")
    show_in_viewer_mode: Optional['bool'] = Field(None, alias="showInViewerMode")
    source: Optional['str'] = Field(None, alias="source")
    space_id: Optional['str'] = Field(None, alias="spaceId")
    tenant_id: Optional['str'] = Field(None, alias="tenantId")
    updated_by: Optional['str'] = Field(None, alias="updatedBy")
    working_draft_id: Optional['str'] = Field(None, alias="workingDraftId")


class FrontendHandledBackendError(PyCelonisBaseModel):
    error_information: Optional['Any'] = Field(None, alias="errorInformation")
    frontend_error_key: Optional['str'] = Field(None, alias="frontendErrorKey")


class AssetMetadataTransport(PyCelonisBaseModel):
    asset_usages: Optional['List[Optional[AssetUsage]]'] = Field(None, alias="assetUsages")
    hidden: Optional['bool'] = Field(None, alias="hidden")
    metadata: Optional['JsonNode'] = Field(None, alias="metadata")
    related_assets: Optional['List[Optional[RelatedAsset]]'] = Field(None, alias="relatedAssets")
    used_variables: Optional['List[Optional[VariableDefinition]]'] = Field(None, alias="usedVariables")


class AssetUsage(PyCelonisBaseModel):
    object_id: Optional['str'] = Field(None, alias="objectId")
    target_objects: Optional['List[Optional[TargetUsageMetadata]]'] = Field(None, alias="targetObjects")


class TargetUsageMetadata(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    source_objects: Optional['List[Optional[SourceUsageMetadata]]'] = Field(None, alias="sourceObjects")
    type_: Optional['str'] = Field(None, alias="type")


class SourceUsageMetadata(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")


class VariableDefinition(PyCelonisBaseModel):
    description: Optional['str'] = Field(None, alias="description")
    key: Optional['str'] = Field(None, alias="key")
    metadata: Optional['JsonNode'] = Field(None, alias="metadata")
    runtime: Optional['bool'] = Field(None, alias="runtime")
    source: Optional['str'] = Field(None, alias="source")
    type_: Optional['str'] = Field(None, alias="type")


class ContentNodeBaseTransport(PyCelonisBaseModel):
    external: Optional['bool'] = Field(None, alias="external")
    reference: Optional['str'] = Field(None, alias="reference")
    version: Optional['str'] = Field(None, alias="version")


class RelatedAsset(PyCelonisBaseModel):
    cascade_type: Optional['CascadeType'] = Field(None, alias="cascadeType")
    object_id: Optional['str'] = Field(None, alias="objectId")
    relation_type: Optional['RelationType'] = Field(None, alias="relationType")
    type_: Optional['str'] = Field(None, alias="type")


BoardUpsertRequest.update_forward_refs()
InputVariableDefinitionTransport.update_forward_refs()
ExceptionReference.update_forward_refs()
ContentNodeTransport.update_forward_refs()
FrontendHandledBackendError.update_forward_refs()
AssetMetadataTransport.update_forward_refs()
AssetUsage.update_forward_refs()
TargetUsageMetadata.update_forward_refs()
SourceUsageMetadata.update_forward_refs()
VariableDefinition.update_forward_refs()
ContentNodeBaseTransport.update_forward_refs()
RelatedAsset.update_forward_refs()


class Blueprint:
    @staticmethod
    def post_api_boards(
        client: Client,
        request_body: BoardUpsertRequest,
        should_activate: Optional['bool'] = None,
        should_publish: Optional['bool'] = None,
        validate_: Optional['bool'] = None,
        **kwargs: Any,
    ) -> ContentNodeTransport:
        logger.debug(
            f"Request: 'POST' -> '/blueprint/api/boards'",
            extra={
                "request_type": "POST",
                "path": "/blueprint/api/boards",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if should_activate is not None:
            if isinstance(should_activate, PyCelonisBaseModel):
                params.update(should_activate.json_dict(by_alias=True))
            elif isinstance(should_activate, dict):
                params.update(should_activate)
            else:
                params["shouldActivate"] = should_activate
        if should_publish is not None:
            if isinstance(should_publish, PyCelonisBaseModel):
                params.update(should_publish.json_dict(by_alias=True))
            elif isinstance(should_publish, dict):
                params.update(should_publish)
            else:
                params["shouldPublish"] = should_publish
        if validate_ is not None:
            if isinstance(validate_, PyCelonisBaseModel):
                params.update(validate_.json_dict(by_alias=True))
            elif isinstance(validate_, dict):
                params.update(validate_)
            else:
                params["validate"] = validate_
        return client.request(
            method="POST",
            url=f"/blueprint/api/boards",
            params=params,
            request_body=request_body,
            parse_json=True,
            type_=ContentNodeTransport,
            **kwargs,
        )

    @staticmethod
    def put_api_boards_board_id(
        client: Client,
        board_id: str,
        request_body: BoardUpsertRequest,
        should_activate: Optional['bool'] = None,
        should_publish: Optional['bool'] = None,
        validate_: Optional['bool'] = None,
        **kwargs: Any,
    ) -> ContentNodeTransport:
        logger.debug(
            f"Request: 'PUT' -> '/blueprint/api/boards/{board_id}'",
            extra={
                "request_type": "PUT",
                "path": "/blueprint/api/boards/{board_id}",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if should_activate is not None:
            if isinstance(should_activate, PyCelonisBaseModel):
                params.update(should_activate.json_dict(by_alias=True))
            elif isinstance(should_activate, dict):
                params.update(should_activate)
            else:
                params["shouldActivate"] = should_activate
        if should_publish is not None:
            if isinstance(should_publish, PyCelonisBaseModel):
                params.update(should_publish.json_dict(by_alias=True))
            elif isinstance(should_publish, dict):
                params.update(should_publish)
            else:
                params["shouldPublish"] = should_publish
        if validate_ is not None:
            if isinstance(validate_, PyCelonisBaseModel):
                params.update(validate_.json_dict(by_alias=True))
            elif isinstance(validate_, dict):
                params.update(validate_)
            else:
                params["validate"] = validate_
        return client.request(
            method="PUT",
            url=f"/blueprint/api/boards/{board_id}",
            params=params,
            request_body=request_body,
            parse_json=True,
            type_=ContentNodeTransport,
            **kwargs,
        )
