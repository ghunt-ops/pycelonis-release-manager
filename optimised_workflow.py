import os
import logging
import uuid
from typing import Any, Dict, Optional, Union

# PyCelonis Imports
from pycelonis import get_celonis
from pycelonis.service.package_manager.service import ContentNodeTransport
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.client.client import Client
from pycelonis_core.utils.ml_workbench import TRACKING_LOGGER

# --- 🎯 User Configuration ---
SOURCE_TEAM_URL = 'https://3lko227e-2024-06-12.training.celonis.cloud/'
SOURCE_API_KEY = 'M2VjNDUxY2YtZjJjYS00OGNmLThlMjItMTM1NzgwNGMyNzUxOlRXdlVYWHdndFQ0dG1lQXRLQUh6WkZkL09LWDdxQThHbEdQWTZYdk14QlFX'
KEY_TYPE = 'USER_KEY'
SPACE_ID = "dcba7a38-50f0-4fcb-9eb3-d87a2a014803"
ORIGINAL_PACKAGE_KEY = "83173426_c3bd_48c5_8b68_cb15b2dccfa6"
TEAM_DOMAIN = "3lko227e-2024-06-12" # Extracted from URL for payload reuse

# Set up logger
logger = logging.getLogger(TRACKING_LOGGER)
JsonNode = Any

# --- 🔑 Environment Setup ---
# Setting environment variables for pycelonis.get_celonis()
os.environ['CELONIS_URL'] = SOURCE_TEAM_URL
os.environ['CELONIS_API_TOKEN'] = SOURCE_API_KEY
os.environ['CELONIS_KEY_TYPE'] = KEY_TYPE

# --- 🛠️ Core API Functions ---

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

def create_copy_payload(source_package: Any, destination_package: Any, new_name: str) -> Dict[str, Any]:
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
    assets = package.get_views()
    visibility_json = [{"id": asset.id, "hide": True} for asset in assets]

    put_api_hide_assets(
        client, 
        visibility_json, 
        package.identifier, 
        {"flavor": "STUDIO"}
    )
    # The original script published the package after hiding assets, which is good practice
    package.publish()
    logger.info(f"Hidden all views and published package: {package.name}")


# --- 🏃 Main Execution ---

# 1. Connect to Celonis
celonis = get_celonis(permissions=False)
client = celonis.client # Store client for API calls

# 2. Access Space and Packages
space = celonis.studio.get_space(SPACE_ID)
packages = space.get_packages()
production_package = packages.find(ORIGINAL_PACKAGE_KEY, "key")
logger.info(f"Found Production Package: {production_package.name}")

# 3. Define Names for Development and Testing
# Creating a clean base name
base_name = production_package.name.replace(" ", "_")
DEVELOPMENT_PACKAGE_NAME = f"DEVELOPMENT_{base_name}"
TESTING_PACKAGE_NAME = f"TESTING_{base_name}"
logger.info(f"Development Package Name: {DEVELOPMENT_PACKAGE_NAME}")
logger.info(f"Testing Package Name: {TESTING_PACKAGE_NAME}")

# 4. Create or Get Packages
# Replaced try/except with a reusable function
development_package = get_or_create_package(space, DEVELOPMENT_PACKAGE_NAME, packages)
testing_package = get_or_create_package(space, TESTING_PACKAGE_NAME, packages)

# 5. Copy Production Package to Development
development_request_payload = create_copy_payload(
    production_package, 
    development_package, 
    DEVELOPMENT_PACKAGE_NAME
)

logger.info("Copying Production Package to Development...")
post_api_copy_package(
    client, 
    development_request_payload, 
    production_package.id, # The source ID is used in the URL for the copy API
    {"flavor": "STUDIO"}
)

# 6. Hide Views and Publish Development Package
# Must sync/refetch after copy to get updated development package contents
space.sync()
packages = space.get_packages()
development_package = packages.find(DEVELOPMENT_PACKAGE_NAME, "name")

hide_package_assets(client, development_package)


# 7. Copy Development Package to Testing
# Must sync/refetch after hiding/publishing to ensure the development package is up-to-date
space.sync()
packages = space.get_packages()
development_package = packages.find(DEVELOPMENT_PACKAGE_NAME, "name") # Get the latest version

testing_request_payload = create_copy_payload(
    development_package, 
    testing_package, 
    TESTING_PACKAGE_NAME
)

logger.info("Copying Development Package to Testing...")
post_api_copy_package(
    client, 
    testing_request_payload, 
    development_package.id, # The source ID is used in the URL for the copy API
    {"flavor": "STUDIO"}
)

# 8. Final Publish of Testing Package
space.sync()
packages = space.get_packages()
testing_package = packages.find(TESTING_PACKAGE_NAME, "name")
testing_package.publish()
logger.info("Published final Testing Package.")