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


class PqlQueryType(PyCelonisBaseEnum):
    DIMENSION = "DIMENSION"
    FILTER = "FILTER"
    TABLE = "TABLE"
    PREAMBLE = "PREAMBLE"


class DiagnosticSeverity(PyCelonisBaseEnum):
    ERROR = "Error"
    WARNING = "Warning"
    INFORMATION = "Information"
    HINT = "Hint"


class FrontendHandledBackendError(PyCelonisBaseModel):
    frontend_error_key: Optional['str'] = Field(None, alias="frontendErrorKey")
    error_information: Optional['Any'] = Field(None, alias="errorInformation")


class ExceptionReference(PyCelonisBaseModel):
    reference: Optional['str'] = Field(None, alias="reference")
    message: Optional['str'] = Field(None, alias="message")
    short_message: Optional['str'] = Field(None, alias="shortMessage")


class PqlBasicBatchParams(PyCelonisBaseModel):
    batch: Optional['List[Optional[PqlBasicParams]]'] = Field(None, alias="batch")


class PqlBasicParams(PyCelonisBaseModel):
    query: Optional['str'] = Field(None, alias="query")
    query_type: Optional['PqlQueryType'] = Field(None, alias="queryType")
    data_model_id: Optional['str'] = Field(None, alias="dataModelId")


class PqlDiagnosticsBatchResponse(PyCelonisBaseModel):
    message: Optional['str'] = Field(None, alias="message")
    results: Optional['List[Optional[PqlDiagnosticsResponse]]'] = Field(None, alias="results")


class PqlDiagnosticsResponse(PyCelonisBaseModel):
    message: Optional['str'] = Field(None, alias="message")
    diagnostics: Optional['List[Optional[Diagnostic]]'] = Field(None, alias="diagnostics")


class Diagnostic(PyCelonisBaseModel):
    range: Optional['Range'] = Field(None, alias="range")
    severity: Optional['DiagnosticSeverity'] = Field(None, alias="severity")
    code: Optional['str'] = Field(None, alias="code")
    source: Optional['str'] = Field(None, alias="source")
    message: Optional['str'] = Field(None, alias="message")
    related_information: Optional['List[Optional[DiagnosticRelatedInformation]]'] = Field(
        None, alias="relatedInformation"
    )


class Position(PyCelonisBaseModel):
    line: Optional['int'] = Field(None, alias="line")
    character: Optional['int'] = Field(None, alias="character")


class Range(PyCelonisBaseModel):
    start: Optional['Position'] = Field(None, alias="start")
    end: Optional['Position'] = Field(None, alias="end")


class DiagnosticRelatedInformation(PyCelonisBaseModel):
    location: Optional['Location'] = Field(None, alias="location")
    message: Optional['str'] = Field(None, alias="message")


class Location(PyCelonisBaseModel):
    uri: Optional['str'] = Field(None, alias="uri")
    range: Optional['Range'] = Field(None, alias="range")


class PqlParseTreeResponse(PyCelonisBaseModel):
    message: Optional['str'] = Field(None, alias="message")
    root: Optional['PqlParseTreeNodeTransport'] = Field(None, alias="root")


class PqlParseTreeNodeTransport(PyCelonisBaseModel):
    rule_name: Optional['str'] = Field(None, alias="ruleName")
    begin: Optional['Position'] = Field(None, alias="begin")
    end: Optional['Position'] = Field(None, alias="end")
    children: Optional['List[Optional[PqlParseTreeNodeTransport]]'] = Field(None, alias="children")


FrontendHandledBackendError.update_forward_refs()
ExceptionReference.update_forward_refs()
PqlBasicBatchParams.update_forward_refs()
PqlBasicParams.update_forward_refs()
PqlDiagnosticsBatchResponse.update_forward_refs()
PqlDiagnosticsResponse.update_forward_refs()
Diagnostic.update_forward_refs()
Position.update_forward_refs()
Range.update_forward_refs()
DiagnosticRelatedInformation.update_forward_refs()
Location.update_forward_refs()
PqlParseTreeResponse.update_forward_refs()
PqlParseTreeNodeTransport.update_forward_refs()


class PqlLanguageService:
    @staticmethod
    def post_api_lsp_publish_diagnostics_batch(
        client: Client, request_body: PqlBasicBatchParams, **kwargs: Any
    ) -> PqlDiagnosticsBatchResponse:
        logger.debug(
            f"Request: 'POST' -> '/pql-language/api/lsp/publishDiagnostics/batch'",
            extra={
                "request_type": "POST",
                "path": "/pql-language/api/lsp/publishDiagnostics/batch",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/pql-language/api/lsp/publishDiagnostics/batch",
            request_body=request_body,
            parse_json=True,
            type_=PqlDiagnosticsBatchResponse,
            **kwargs,
        )

    @staticmethod
    def post_api_lsp_parse_tree(client: Client, request_body: PqlBasicParams, **kwargs: Any) -> PqlParseTreeResponse:
        logger.debug(
            f"Request: 'POST' -> '/pql-language/api/lsp/parseTree'",
            extra={
                "request_type": "POST",
                "path": "/pql-language/api/lsp/parseTree",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/pql-language/api/lsp/parseTree",
            request_body=request_body,
            parse_json=True,
            type_=PqlParseTreeResponse,
            **kwargs,
        )
