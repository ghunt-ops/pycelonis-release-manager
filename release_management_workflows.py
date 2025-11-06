# Import Modules
import logging
from typing import Any

# PyCelonis Imports
from pycelonis_core.utils.ml_workbench import TRACKING_LOGGER
from release_management_functions import *

def workflow_create_development_and_testing_packages_from_production(client: Any, space: Any, packages: Any, production_package: Any, DEVELOPMENT_PACKAGE_NAME: str, TESTING_PACKAGE_NAME: str, TEAM_DOMAIN: str):

    # 4. Create or Get Packages
    # Replaced try/except with a reusable function
    development_package = get_or_create_package(space, DEVELOPMENT_PACKAGE_NAME, packages)
    testing_package = get_or_create_package(space, TESTING_PACKAGE_NAME, packages)

    # 5. Copy Production Package to Development
    development_request_payload = create_copy_payload(
        TEAM_DOMAIN,
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
        TEAM_DOMAIN,
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


def workflow_copy_package_to(client: Any, packages: Any, SOURCE_PACKAGE_NAME: str, DESTINATION_PACKAGE_NAME: str, TEAM_DOMAIN: str):

    # --- 🏃 Main Execution ---
    # 1. Create or Get Packages
    # Replaced try/except with a reusable function
    source_package = packages.find(SOURCE_PACKAGE_NAME, "name")
    logger.info(f"Found Development Package: {source_package.name}")
    destination_package = packages.find(DESTINATION_PACKAGE_NAME, "name")
    logger.info(f"Found Testing Package: {destination_package.name}")

    # 2. Hide Views and Publish Development Package
    hide_package_assets(client, source_package)

    # 3. Copy Development Package to Testing
    testing_request_payload = create_copy_payload(
        TEAM_DOMAIN,
        source_package, 
        destination_package, 
        DESTINATION_PACKAGE_NAME
    )

    logger.info("Copying Production Package to Development...")
    post_api_copy_package(
        client, 
        testing_request_payload, 
        source_package.id, # The source ID is used in the URL for the copy API
        {"flavor": "STUDIO"}
    )

    destination_package.publish()
    logger.info("Published final Testing Package.")


