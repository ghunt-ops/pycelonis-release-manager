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


class CloudTeamPrivacyType(PyCelonisBaseEnum):
    PUBLIC = "PUBLIC"
    PUBLIC_TO_DOMAIN = "PUBLIC_TO_DOMAIN"
    PRIVATE = "PRIVATE"


class DataConsumptionStage(PyCelonisBaseEnum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ORANGE = "ORANGE"
    RED = "RED"


class PermissionsManagementMode(PyCelonisBaseEnum):
    STANDARD = "STANDARD"
    RESTRICTED_TO_ADMINS = "RESTRICTED_TO_ADMINS"


class ExceptionReference(PyCelonisBaseModel):
    reference: Optional['str'] = Field(None, alias="reference")
    message: Optional['str'] = Field(None, alias="message")
    short_message: Optional['str'] = Field(None, alias="shortMessage")


class ValidationError(PyCelonisBaseModel):
    attribute: Optional['str'] = Field(None, alias="attribute")
    error: Optional['str'] = Field(None, alias="error")
    error_code: Optional['str'] = Field(None, alias="errorCode")
    additional_info: Optional['str'] = Field(None, alias="additionalInfo")


class ValidationExceptionDescriptor(PyCelonisBaseModel):
    errors: Optional['List[Optional[ValidationError]]'] = Field(None, alias="errors")


class UserTransport(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    email: Optional['str'] = Field(None, alias="email")
    name: Optional['str'] = Field(None, alias="name")
    team_name: Optional['str'] = Field(None, alias="teamName")
    team_id: Optional['str'] = Field(None, alias="teamId")
    team_domain: Optional['str'] = Field(None, alias="teamDomain")
    api_token: Optional['str'] = Field(None, alias="apiToken")
    active: Optional['bool'] = Field(None, alias="active")
    token: Optional['str'] = Field(None, alias="token")
    account_created: Optional['bool'] = Field(None, alias="accountCreated")
    current: Optional['bool'] = Field(None, alias="current")
    language: Optional['str'] = Field(None, alias="language")
    avatar_url: Optional['str'] = Field(None, alias="avatarUrl")
    enable_notifications: Optional['bool'] = Field(None, alias="enableNotifications")
    notifications_time: Optional['str'] = Field(None, alias="notificationsTime")
    time_zone: Optional['str'] = Field(None, alias="timeZone")
    role: Optional['int'] = Field(None, alias="role")
    effective_role: Optional['int'] = Field(None, alias="effectiveRole")
    contentstore_admin: Optional['bool'] = Field(None, alias="contentstoreAdmin")
    backend_access: Optional['bool'] = Field(None, alias="backendAccess")
    last_log_in_date: Optional['datetime'] = Field(None, alias="lastLogInDate")
    is_first_log_in: Optional['bool'] = Field(None, alias="isFirstLogIn")
    is_celonis_user: Optional['bool'] = Field(None, alias="isCelonisUser")
    full_template_access: Optional['bool'] = Field(None, alias="fullTemplateAccess")
    group_ids: Optional['List[Optional[str]]'] = Field(None, alias="groupIds")
    cloud_admin: Optional['bool'] = Field(None, alias="cloudAdmin")
    migrated_to_idp: Optional['bool'] = Field(None, alias="migratedToIdp")
    name_or_email: Optional['str'] = Field(None, alias="nameOrEmail")
    analyst: Optional['bool'] = Field(None, alias="analyst")
    admin: Optional['bool'] = Field(None, alias="admin")
    member: Optional['bool'] = Field(None, alias="member")
    name_and_email: Optional['str'] = Field(None, alias="nameAndEmail")


class UserServicePermissionsTransport(PyCelonisBaseModel):
    service_name: Optional['str'] = Field(None, alias="serviceName")
    permissions: Optional['List[Optional[str]]'] = Field(None, alias="permissions")


class TeamTransport(PyCelonisBaseModel):
    id: Optional['str'] = Field(None, alias="id")
    name: Optional['str'] = Field(None, alias="name")
    domain: Optional['str'] = Field(None, alias="domain")
    privacy_type: Optional['CloudTeamPrivacyType'] = Field(None, alias="privacyType")
    allowed_domain: Optional['str'] = Field(None, alias="allowedDomain")
    open_signup_enabled: Optional['bool'] = Field(None, alias="openSignupEnabled")
    open_signup_code: Optional['str'] = Field(None, alias="openSignupCode")
    open_signup_default_group_id: Optional['str'] = Field(None, alias="openSignupDefaultGroupId")
    active: Optional['bool'] = Field(None, alias="active")
    active_until: Optional['datetime'] = Field(None, alias="activeUntil")
    accessible_from_ip: Optional['bool'] = Field(None, alias="accessibleFromIp")
    visible: Optional['bool'] = Field(None, alias="visible")
    member_limit: Optional['int'] = Field(None, alias="memberLimit")
    analyst_limit: Optional['int'] = Field(None, alias="analystLimit")
    action_engine_user_limit: Optional['int'] = Field(None, alias="actionEngineUserLimit")
    ml_workbenches_limit: Optional['int'] = Field(None, alias="mlWorkbenchesLimit")
    table_rows_limit: Optional['int'] = Field(None, alias="tableRowsLimit")
    data_consumption_limit_in_gigabytes: Optional['int'] = Field(None, alias="dataConsumptionLimitInGigabytes")
    data_pool_versions_limit: Optional['int'] = Field(None, alias="dataPoolVersionsLimit")
    current_data_consumption_in_bytes: Optional['int'] = Field(None, alias="currentDataConsumptionInBytes")
    data_push_job_submission_limit_per_sec: Optional['int'] = Field(None, alias="dataPushJobSubmissionLimitPerSec")
    data_push_job_submission_limit_per_hour: Optional['int'] = Field(None, alias="dataPushJobSubmissionLimitPerHour")
    data_consumptions_last_updated_at: Optional['datetime'] = Field(None, alias="dataConsumptionsLastUpdatedAt")
    data_consumption_stage: Optional['DataConsumptionStage'] = Field(None, alias="dataConsumptionStage")
    data_transfer_hybrid_to_cloud_enabled: Optional['bool'] = Field(None, alias="dataTransferHybridToCloudEnabled")
    tracking_enabled: Optional['bool'] = Field(None, alias="trackingEnabled")
    terms_of_use_url: Optional['str'] = Field(None, alias="termsOfUseUrl")
    terms_and_conditions_enabled: Optional['bool'] = Field(None, alias="termsAndConditionsEnabled")
    enforce_two_factor_authentication_enabled: Optional['bool'] = Field(
        None, alias="enforceTwoFactorAuthenticationEnabled"
    )
    lms_url: Optional['str'] = Field(None, alias="lmsUrl")
    permissions_management_mode: Optional['PermissionsManagementMode'] = Field(None, alias="permissionsManagementMode")
    request_date: Optional['datetime'] = Field(None, alias="requestDate")
    unlimited_action_engine_users: Optional['bool'] = Field(None, alias="unlimitedActionEngineUsers")
    unlimited_data_pool_versions_limit: Optional['bool'] = Field(None, alias="unlimitedDataPoolVersionsLimit")
    unlimited_data_push_job_submissions: Optional['bool'] = Field(None, alias="unlimitedDataPushJobSubmissions")
    unlimited_members: Optional['bool'] = Field(None, alias="unlimitedMembers")
    unlimited_analysts: Optional['bool'] = Field(None, alias="unlimitedAnalysts")
    unlimited_ml_workbenches: Optional['bool'] = Field(None, alias="unlimitedMlWorkbenches")
    unlimited_table_rows: Optional['bool'] = Field(None, alias="unlimitedTableRows")
    unlimited_data_consumption: Optional['bool'] = Field(None, alias="unlimitedDataConsumption")


ExceptionReference.update_forward_refs()
ValidationError.update_forward_refs()
ValidationExceptionDescriptor.update_forward_refs()
UserTransport.update_forward_refs()
UserServicePermissionsTransport.update_forward_refs()
TeamTransport.update_forward_refs()


class TeamService:
    @staticmethod
    def get_api_cloud(client: Client, **kwargs: Any) -> UserTransport:
        logger.debug(
            f"Request: 'GET' -> '/api/cloud'",
            extra={
                "request_type": "GET",
                "path": "/api/cloud",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(method="GET", url=f"/api/cloud", parse_json=True, type_=UserTransport, **kwargs)

    @staticmethod
    def get_api_cloud_permissions(client: Client, **kwargs: Any) -> List[Optional[UserServicePermissionsTransport]]:
        logger.debug(
            f"Request: 'GET' -> '/api/cloud/permissions'",
            extra={
                "request_type": "GET",
                "path": "/api/cloud/permissions",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/api/cloud/permissions",
            parse_json=True,
            type_=List[Optional[UserServicePermissionsTransport]],
            **kwargs,
        )

    @staticmethod
    def get_api_team(client: Client, **kwargs: Any) -> TeamTransport:
        logger.debug(
            f"Request: 'GET' -> '/api/team'",
            extra={
                "request_type": "GET",
                "path": "/api/team",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(method="GET", url=f"/api/team", parse_json=True, type_=TeamTransport, **kwargs)
