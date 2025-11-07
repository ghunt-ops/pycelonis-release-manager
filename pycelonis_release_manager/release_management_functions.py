# --- 🛠️ Core API Functions ---
import os
import uuid
import logging
from typing import Any, Dict, Optional, Union
from pycelonis.service.package_manager.service import ContentNodeTransport
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.client.client import Client
from pycelonis_core.utils.ml_workbench import TRACKING_LOGGER
from pycelonis import get_celonis

# Set up logger
logger = logging.getLogger(TRACKING_LOGGER)

def _handle_validation_params(validate_: Optional[Union['bool', Dict[Any, Any], PyCelonisBaseModel]]) -> Dict[str, Any]:
    """Helper function to standardize validation parameter handling."""
    params: Dict[str, Any] = {}
    if validate_ is not None:
        if isinstance(validate_, PyCelonisBaseModel):
            params.update(validate_.json_dict(by_alias=True))
        elif isinstance(validate_, dict):
            params.update(validate_)
        else:
            params["validate"] = validate_
    return params

def _log_api_request(method: str, path: str):
    """Helper function to standardize API request logging."""
    logger.debug(
        f"Request: '{method}' -> '{path}'",
        extra={
            "request_type": method,
            "path": path,
            "tracking_type": "API_REQUEST",
        },
    )

def post_api_copy_package(
    client: Client,
    request_body: Dict[Any, Any],
    source_package_id: str,
    validate_: Optional[Union['bool', Dict[Any, Any], PyCelonisBaseModel]] = None,
    **kwargs: Any
) -> ContentNodeTransport:
    """Copies a Celonis Studio package via API call."""
    path = "/package-manager/api/nodes"
    _log_api_request("POST", path)

    params = _handle_validation_params(validate_)
    
    return client.request(
        method="POST",
        url=f"{path}/{source_package_id}/copy",
        params=params,
        request_body=request_body,
        parse_json=True,
        type_=ContentNodeTransport,
        **kwargs,
    )

def put_api_hide_assets(
    client: Client,
    request_body: Dict[Any, Any],
    package_identifier: str, # Using identifier (key) which is common for visibility API
    validate_: Optional[Union['bool', Dict[Any, Any], PyCelonisBaseModel]] = None,
    **kwargs: Any
) -> ContentNodeTransport:
    """Hides or shows package assets via API call."""
    path = "/package-manager/api/nodes"
    _log_api_request("PUT", path)

    params = _handle_validation_params(validate_)

    # Note: The original code used package_id for POST but identifier for PUT. Sticking to original logic.
    return client.request(
        method="PUT",
        url=f"{path}/{package_identifier}/visibility",
        params=params,
        request_body=request_body,
        parse_json=False, # Original was False
        type_=ContentNodeTransport,
        **kwargs,
    )

# --- 📦 Package Management Logic ---

def get_or_create_package(space, package_name: str, packages) -> Any:
    """
    Attempts to find a package by name. If not found, creates and publishes a new one.
    """
    try:
        package = packages.find(package_name, "name")
        logger.info(f"Found existing package: {package_name}")
        try:
            package.publish()
            logger.info(f"Published existing package: {package_name}")
        except Exception as e:
            logger.warning(f"Could not publish existing package: {package_name}. Error: {e}")
    except:
        logger.info(f"Package {package_name} not found. Creating a new one.")
        # Use a consistent description
        description = f"{package_name.split('_')[0]} Package created via PyCelonis script."
        space.create_package(
            name=package_name, 
            description=description, 
            key=str(uuid.uuid4())
        )
        # Resync and refetch packages after creation
        space.sync() 
        packages = space.get_packages() 
        package = packages.find(package_name, "name")
        package.publish()
        logger.info(f"Created and published new package: {package_name}")
    return package

def create_copy_payload(TEAM_DOMAIN: str, source_package: Any, destination_package: Any, new_name: str) -> Dict[str, Any]:
    """Creates the request body payload for the package copy API."""
    return { 
        "nodeId": source_package.id,
        "nodeIdToReplace": "",
        "nodeKey": source_package.identifier,
        "rootKey": source_package.identifier,
        "teamDomain": TEAM_DOMAIN,
        "destinationRootId": destination_package.id,
        "destinationRootKey": destination_package.identifier,
        "destinationSpaceId": destination_package.space_id,
        "newName": new_name
    }

def hide_package_assets(client: Client, package: Any):
    """Hides all views within a package and publishes."""
    logger.info(f"Hiding views in package: {package.name}")
    assets = package.get_content_nodes()

    for asset in assets:
        logger.debug(f"Asset before hiding: ID={asset.id}, Name={asset.name}")
        visibility_json = [{"id": asset.id, "hide": True}]
        try:
            put_api_hide_assets(
                client, 
                visibility_json, 
                package.identifier, 
                {"flavor": "STUDIO"}
            )
            logger.debug(f"Successfully hid asset: ID={asset.id}, Name={asset.name}")
        except Exception as e:
            logger.error(f"Failed to hide asset: ID={asset.id}, Name={asset.name}. Error: {e}")

    # The original script published the package after hiding assets, which is good practice
    package.publish()
    logger.info(f"Hidden all assets and published package: {package.name}")


def setup_celonis_environment(source_url: str, api_key: str, key_type: str):
    """
    Sets the required environment variables for pycelonis.get_celonis().

    Args:
        source_url (str): The Celonis Team URL.
        api_key (str): The Celonis API Token.
        key_type (str): The Celonis Key Type.
    """
    os.environ['CELONIS_URL'] = source_url
    os.environ['CELONIS_API_TOKEN'] = api_key
    os.environ['CELONIS_KEY_TYPE'] = key_type
    logger.info("Celonis environment variables set.")

def connect_to_celonis_and_get_package(space_id: str, package_key: str):
    """
    Connects to Celonis, retrieves the specified Studio Space, and finds 
    the production package within it.

    Args:
        space_id (str): The ID of the Studio Space.
        package_key (str): The key of the original production package.

    Returns:
        tuple: A tuple containing (celonis_connection, client, production_package).
    """
    # 1. Connect to Celonis
    # Permissions=False often prevents unnecessary scope/permission checks on startup
    celonis = get_celonis(permissions=False)
    client = celonis.client # Store client for API calls

    # 2. Access Space and Packages
    space = celonis.studio.get_space(space_id)
    packages = space.get_packages()
    production_package = packages.find(package_key, "key")
    logger.info(f"Found Production Package: {production_package.name}")

    return celonis, client, space, packages, production_package

def generate_package_names(original_name: str) -> tuple[str, str]:
    """
    Creates standardized development and testing package names based on the 
    original package name.

    Args:
        original_name (str): The name of the original production package.

    Returns:
        tuple[str, str]: A tuple containing (dev_package_name, test_package_name).
    """
    # Creating a clean base name
    base_name = original_name.replace(" ", "_")
    development_package_name = f"DEVELOPMENT_{base_name}"
    testing_package_name = f"TESTING_{base_name}"
    logger.info(f"Generated names: Dev={development_package_name}, Test={testing_package_name}")
    return development_package_name, testing_package_name