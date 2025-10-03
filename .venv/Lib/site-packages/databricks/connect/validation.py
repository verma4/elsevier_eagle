#
# DATABRICKS CONFIDENTIAL & PROPRIETARY
# __________________
#
# Copyright 2020-present Databricks, Inc.
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
# and its suppliers, if any.  The intellectual and technical concepts contained herein are
# proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
# patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
# or reproduction of this information is strictly forbidden unless prior written permission is
# obtained from Databricks, Inc.
#
# If you view or obtain a copy of this information and believe Databricks, Inc. may not have
# intended it to be made available, please promptly report it to Databricks Legal Department
# @ legal@databricks.com.
#

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from packaging.version import parse
import re
from typing import Tuple

from .debug import _setup_logging
from pyspark.dbconnect_version import __dbconnect_version__

logger = _setup_logging()


def parse_dbr_version(version: str) -> Tuple[int]:
    """
    Parses major and minor version from a Databricks Runtime version string.
    Works with custom image names, "major.x" image names, and standard image names.
    """
    pattern = r"(\d+(\.\d+)?\.x)"
    match = re.search(pattern, version)
    if match:
        return tuple(int(i) for i in match.group().split(".") if i.isnumeric())
    else:
        raise ValueError("Failed to parse minor & major version from Databricks Runtime" +
                         f"version string: {version}")


def validate_session_with_sdk(config: Config) -> None:
    """
    Validates the configuration by retrieving the used Databricks Runtime version with
    the Databricks SDK. Checks if there is an unsupported combination of Databricks Runtime
    & Databricks Connect versions. Throws an exception if the validation fails.

    Parameters
    ----------
    session: SparkSession
        The session to perform the validation with.
    """

    logger.debug("Validating configuration by using the Databricks SDK")

    cluster_id = config.cluster_id
    if cluster_id is None:
        raise Exception("Cluster ID not set.")

    workspace_client = WorkspaceClient(config=config)

    dbr_version_string = workspace_client.clusters.get(cluster_id).spark_version
    dbr_version = parse_dbr_version(dbr_version_string)
    db_connect_version = parse(__dbconnect_version__)
    if (dbr_version[0] < db_connect_version.major or
            len(dbr_version) > 1 and dbr_version[0] == db_connect_version.major and
            dbr_version[1] < db_connect_version.minor):
        displayed_dbr_version = ".".join(map(str, dbr_version)) if len(dbr_version) > 1 \
            else str(dbr_version[0]) + ".x"
        raise Exception("Unsupported combination of Databricks Runtime & " +
                        "Databricks Connect versions: " +
                        f"{displayed_dbr_version} (Databricks Runtime) " +
                        f"< {db_connect_version.base_version} (Databricks Connect).")

    logger.debug("Session validated successfully.")
