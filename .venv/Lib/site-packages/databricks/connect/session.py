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

import warnings
from importlib.metadata import version
from packaging.version import Version
import os
from typing import Any, Optional
import uuid

from databricks.sdk.core import Config
from .auth import DatabricksChannelBuilder
from .cache import HashableDict, cached_session
from .debug import _setup_logging
from .validation import validate_session_with_sdk
from pyspark.dbconnect_version import __dbconnect_version__
from pyspark.sql import SparkSession
from pyspark.sql.connect.session import SparkSession as RemoteSparkSession


# The same behavior can be achieved using @classmethod + @property combination
# in python 3.9 and 3.10.
# However, this is deprecated in 3.11 and will be removed in 3.13
# https://docs.python.org/3.11/whatsnew/3.11.html#language-builtins,
# so we have to rely on a custom implementation of classproperty.
class classproperty(property):
    def __get__(self, instance: Any, owner: Any = None) -> "DatabricksSession.Builder":
        # The "type: ignore" below silences the following error from mypy:
        # error: Argument 1 to "classmethod" has incompatible
        # type "Optional[Callable[[Any], Any]]";
        # expected "Callable[..., Any]"  [arg-type]
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore


logger = _setup_logging()


class DatabricksSession:
    """
    The entry point for Databricks Connect.
    Create a new Pyspark session which connects to a remote Spark cluster and executes specified
    DataFrame APIs.

    Examples
    --------
    >>> spark = DatabricksSession.builder.getOrCreate()
    >>> df = spark.range(10)
    >>> print(df)
    """
    class Builder:
        """
        Builder to construct connection parameters.
        An instance of this class can be created using DatabricksSession.builder.
        """
        def __init__(self):
            self._conn_string: Optional[str] = None
            self._host: Optional[str] = None
            self._cluster_id: Optional[str] = None
            self._serverless_mode: bool = False
            self._token: Optional[str] = None
            self._config: Optional[Config] = None
            self._headers: dict[str, str] = dict()
            self._user_agent: str = os.environ.get("SPARK_CONNECT_USER_AGENT", "")
            self._profile: Optional[str] = None
            self._validate_session_enabled: Optional[bool] = None

        def remote(
            self,
            conn_string: str = None,
            *,
            host: str = None,
            cluster_id: str = None,
            serverless: bool = False,
            token: str = None,
            user_agent: str = "",
            headers: Optional[dict[str, str]] = None,
        ) -> "DatabricksSession.Builder":
            """
            Specify connection and authentication parameters to connect to the remote Databricks
            cluster. Either the conn_string parameter should be specified, or a combination of the
            host, token parameters and cluster_id or serverless, but not both.

            Parameters
            ----------
            conn_string : str, optional
                The full spark connect connection string starting with "sc://".
                See https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md # noqa: E501
            host : str, optional
                The Databricks workspace URL
            cluster_id: str, optional
                The cluster identifier where the Databricks connect queries should be executed.
            token: str, optional
                The Databricks personal access token used to authenticate into the cluster and on
                whose behalf the queries are executed.
            user_agent: str, optional
                A user agent string identifying the application using the Databricks Connect module.
                Databricks Connect sends a set of standard information, such as, OS, Python version
                and the version of Databricks Connect included as a user agent to the service.
                The value provided here will be included along with the rest of the information.
                It is recommended to provide this value in the format "<product-name>/<version>"
                as described in https://datatracker.ietf.org/doc/html/rfc7231#section-5.5.3, but is
                not required.
            headers: dict[str, str], optional
                Headers to use while initializing Spark Connect (Databricks Internal Use).

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the values configured.

            Examples
            --------
            Using a simple conn_string
            >>> conn = "sc://foo-workspace.cloud.databricks.com/;token=dapi1234567890;x-databricks-cluster-id=0301-0300-abcdefab" # noqa: E501
            >>> spark = DatabricksSession.builder.remote(conn).getOrCreate()

            Using keyword parameters
            >>> conn = "sc://foo-workspace.cloud.databricks.com/;token=dapi1234567890;x-databricks-cluster-id=0301-0300-abcdefab" # noqa: E501
            >>> spark = DatabricksSession.builder.remote(
            >>>     host="foo-workspace.cloud.databricks.com",
            >>>     token="dapi1234567890",
            >>>     cluster_id="0301-0300-abcdefab"
            >>> ).getOrCreate()
            """
            self._conn_string = conn_string
            self.host(host)
            self.clusterId(cluster_id)
            self.serverless(serverless)
            self.token(token)
            self.userAgent(user_agent)
            self.headers(headers)
            return self

        def host(self, host: str) -> "DatabricksSession.Builder":
            """
            The Databricks workspace URL.

            Parameters
            ----------
            host: str
                The Databricks workspace URL.

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the host configured.
            """
            self._host = host
            return self

        def clusterId(self, clusterId: str) -> "DatabricksSession.Builder":
            """
            The cluster identifier where the Databricks connect queries should be executed.
            Can't be used with serverless(enabled=True) at the same time.

            Parameters
            ----------
            clusterId: str
                The cluster identifier where the Databricks connect queries should be executed.

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the clusterId configured.
            """
            self._cluster_id = clusterId
            return self

        def serverless(self, enabled: bool = True) -> "DatabricksSession.Builder":
            """
            Connect to the serverless endpoint of the workspace.
            Can't be used with clusterId at the same time.

            Parameters
            ----------
            enabled: bool
                Boolean flag that enables serverless mode.

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the serverless mode configured.
            """
            self._serverless_mode = enabled
            return self

        def headers(self, headers: dict[str, str]) -> "DatabricksSession.Builder":
            """
            Headers to use while initializing Spark Connect (Databricks Internal Use).
            This method is cumulative (can be called repeatedly to add more headers).

            Parameters
            ----------
            headers: dict[str, str]
                Headers, as dictionary from header name to header value

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with headers configured.
            """

            if headers:
                self._headers.update(headers)

            return self

        def header(self, header_name: str, header_value: str) -> "DatabricksSession.Builder":
            """
            Adds a header to use while initializing Spark Connect (Databricks Internal Use).
            This method is cumulative (can be called repeatedly to add more headers).

            Parameters
            ----------
            header_name: str
                Name of the header to set

            header_value: str
                The value to set

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with a header set.
            """

            self._headers[header_name] = header_value

            return self

        def token(self, token: str) -> "DatabricksSession.Builder":
            """
            The Databricks personal access token used to authenticate into the cluster and on whose
            behalf the queries are executed.

            Parameters
            ----------
            token: str
                The Databricks personal access token used to authenticate into the cluster and on
                whose behalf the queries are executed.

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the token configured.
            """
            self._token = token
            return self

        def sdkConfig(self, config: Config) -> "DatabricksSession.Builder":
            """
            The Databricks SDK Config object that should be used to pick up connection and
            authentication parameters.
            See https://pypi.org/project/databricks-sdk/#authentication for how to configure
            connection and authentication parameters with the SDK.

            Parameters
            ----------
            config: Config
                The Databricks SDK Config object that contains the connection and authentication
                parameters.

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the token configured.
            """
            self._config = config
            return self

        def profile(self, profile: str) -> "DatabricksSession.Builder":
            """
            Set the profile to use in Databricks SDK configuration

            Parameters
            ----------
            profile: str
                Configuration profile to use in Databricks SDK

            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the config profile set.
            """
            self._profile = profile
            return self

        def userAgent(self, userAgent: str) -> "DatabricksSession.Builder":
            """
            A user agent string identifying the application using the Databricks Connect module.
            Databricks Connect sends a set of standard information, such as, OS, Python version and
            the version of Databricks Connect included as a user agent to the service.
            The value provided here will be included along with the rest of the information.
            It is recommended to provide this value in the format "<product-name>/<product-version>"
            as described in https://datatracker.ietf.org/doc/html/rfc7231#section-5.5.3, but is not
            required.
            Parameters
            ----------
            userAgent: str
                The user agent string identifying the application that is using this module.
            Returns
            -------
            DatabricksSession.Builder
                The same instance of this class with the user agent configured.
            """
            if len(userAgent) > 2048:
                raise Exception("user agent should not exceed 2048 characters.")
            self._user_agent = userAgent
            return self

        def validateSession(self, enabled: bool = True) -> "DatabricksSession.Builder":
            """
            Setting this option will run validations and throw an error if any fail. Validations
            are run only when connecting to a Databricks cluster.
            * the specified connection parameters are valid and can communicate with the cluster
            * the Databricks Runtime version of the cluster is greater than or equal to the
              Databricks Connect version.

            By default, these validations are run and a warning is logged. Unsetting this option
            will turn off these validations. Skipping validations for connection strings.
            """
            self._validate_session_enabled = enabled
            return self

        def getOrCreate(self) -> SparkSession:
            """
            Create a new :class:SparkSession that can then be used to execute DataFrame APIs on.

            Returns
            -------
            SparkSession
                A spark session initialized with the provided connection parameters. All DataFrame
                queries to this spark session are executed remotely.
            """

            # Immediately, set connect mode to true. All operations after this will be connect
            # based, and Spark classic paths should be ignored.
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"

            nb_session = self._try_get_notebook_session()
            new_notebook_session_msg = (
                "Ignoring the default notebook Spark session and creating a new Spark Connect "
                "session. To use the default notebook Spark session, use "
                "DatabricksSession.builder.getOrCreate() with no additional parameters."
            )
            if self._conn_string is not None:
                if nb_session is not None:
                    warnings.warn(new_notebook_session_msg)
                logger.debug("Parameter conn_string is specified")
                if (
                    self._host is not None
                    or self._token is not None
                    or self._cluster_id is not None
                    or self._serverless_mode is True
                ):
                    # conn_string and other parameters should not both be specified
                    raise Exception(
                        "conn_string must not be set when connection "
                        "parameters are explicitly configured."
                    )

                if self._headers:
                    raise Exception("Can't insert custom headers when using connection string")

                logger.debug(f"Using connection string: {self._conn_string}")
                return self._from_connection_string(self._conn_string)

            if (
                self._host is not None
                or self._token is not None
                or self._cluster_id is not None
                or self._serverless_mode is True
            ):
                if nb_session is not None:
                    warnings.warn(new_notebook_session_msg)
                logger.debug(
                    "Some parameters detected during initialization. "
                    f"host={self._host} | token={self._token} | cluster id={self._cluster_id} "
                    f"| serverless={self._serverless_mode}"
                )
                if self._config is not None:
                    # both sdk config and parameters cannot be both specified
                    raise Exception(
                        "sdkConfig must not be set when connection "
                        "parameters are explicitly configured."
                    )
                if self._cluster_id is not None and self._serverless_mode is True:
                    raise Exception("Can't set both cluster id and serverless.")

                config = Config(
                    host=self._host,
                    token=self._token,
                    profile=self._profile,
                )
                if self._serverless_mode:
                    config.as_dict().pop("cluster_id", None)
                    config.serverless_compute_id = "auto"
                elif self._cluster_id:
                    config.as_dict().pop("serverless_compute_id", None)
                    config.cluster_id = self._cluster_id
                return self._from_sdkconfig(
                    config, self._gen_user_agent(), self._headers, self._validate_session_enabled)

            if self._config is not None:
                if nb_session is not None:
                    warnings.warn(new_notebook_session_msg)
                # if the SDK config is explicitly configured
                logger.debug("SDK Config is explicitly configured.")

                if self._profile is not None:
                    raise Exception("Can't set profile when SDK Config is explicitly configured.")
                return self._from_sdkconfig(
                    self._config, self._gen_user_agent(), self._headers,
                    self._validate_session_enabled)

            if self._profile is not None:
                if nb_session is not None:
                    warnings.warn(new_notebook_session_msg)
                logger.debug("SDK Config profile is explicitly configured")
                config = Config(profile=self._profile)
                return self._from_sdkconfig(
                    config, self._gen_user_agent(), self._headers, self._validate_session_enabled)

            if nb_session is not None:
                # Running in a Databricks env - notebook/job.
                logger.debug("Detected running in a notebook.")
                return nb_session

            if "SPARK_REMOTE" in os.environ:
                logger.debug(f"SPARK_REMOTE configured: {os.getenv('SPARK_REMOTE')}")
                return self._from_spark_remote()

            # use the default values that may be supplied from the SDK Config
            logger.debug("Falling back to default configuration from the SDK.")
            config = Config()
            return self._from_sdkconfig(
                config, self._gen_user_agent(), self._headers, self._validate_session_enabled)

        @staticmethod
        def _try_get_notebook_session() -> Optional[SparkSession]:
            # check if we are running in a Databricks notebook
            try:
                import IPython  # noqa
            except ImportError:
                # IPython is always included in Notebook. If not present,
                # then we are not in a notebook.
                logger.debug("IPython module is not present.")
                return None

            def _is_dlt(user_ns) -> bool:
                # It is a short-term solution to work around ES-1170622, and this needs to be
                # replaced with a function checking if the current environment contains a
                # Databricks-owned REPL.
                return "dlt_patch_collect_deprecated_fn" in user_ns

            logger.debug("IPython module is present.")
            user_ns = getattr(IPython.get_ipython(), "user_ns", {})
            if "spark" in user_ns and ("sc" in user_ns or _is_dlt(user_ns)):
                return user_ns["spark"]
            return None

        @staticmethod
        @cached_session(lambda: os.environ["SPARK_REMOTE"])
        def _from_spark_remote() -> SparkSession:
            logger.debug("Creating SparkSession from Spark Remote")
            return SparkSession.builder.create()

        @staticmethod
        @cached_session(lambda s: s)
        def _from_connection_string(conn_string) -> SparkSession:
            logger.debug("Creating SparkSession from connection string")
            return SparkSession.builder.remote(conn_string).create()

        @staticmethod
        def _cache_from_sdk_config(config: Config, user_agent: str, headers: dict[str, str],
                                   validate_session: Optional[bool]):
            return (HashableDict(config.as_dict()), user_agent,
                    HashableDict(headers), validate_session)

        @staticmethod
        @cached_session(_cache_from_sdk_config.__get__(object))
        def _from_sdkconfig(
            config: Config,
            user_agent: str,
            headers: dict[str, str],
            validate_session: Optional[bool],
        ) -> SparkSession:
            DatabricksSession.Builder._verify_sdk_version()

            cluster_id = None
            serverless_mode = False
            if config.serverless_compute_id is not None and config.cluster_id is not None:
                raise Exception("Can't set both cluster id and serverless_compute_id in SDK config.")
            elif config.serverless_compute_id is not None:
                if config.serverless_compute_id == "auto":
                    serverless_mode = True
                else:
                    raise ValueError(
                        "Invalid serverless_compute_id value. Must be 'auto'.")
            elif config.cluster_id is not None:
                cluster_id = config.cluster_id

            if (
                cluster_id is None
                and not serverless_mode
                and "x-databricks-session-id" not in headers
            ):
                # host and token presence are validated by the SDK. cluster id is not.
                raise Exception("Cluster id or serverless are required but were not specified.")
            elif serverless_mode is True or "x-databricks-session-id" in headers:
                if "x-databricks-session-id" not in headers:
                    headers["x-databricks-session-id"] = str(uuid.uuid4())
                logger.debug("Using serverless compute")
                logger.debug("Using SparkSession with remote session id: "
                             f"{headers['x-databricks-session-id']}")
                # Disable session validation for remote session id
                validate_session = False
                warnings.warn(f"DB Connect {__dbconnect_version__} is unsupported with serverless. Use with caution!\n"
                              "DB Connect 15.1 is recommended. Install the recommended version by running "
                              "'pip install --upgrade \"databricks-connect==15.1.*\"'.")
            elif cluster_id is not None:
                logger.debug(f"Using cluster: {cluster_id}")
                logger.debug(f"Creating SparkSession from SDK config: {config}")

            # validate the session for None and True
            # only fail if validateSession is explicitly enabled
            if validate_session is not False:
                try:
                    validate_session_with_sdk(config)
                except Exception as e:
                    if validate_session:
                        raise
                    else:
                        logger.warn(str(e))

            channel_builder = DatabricksChannelBuilder(config, user_agent, headers=headers)
            return RemoteSparkSession.builder.channelBuilder(channel_builder).create()

        def _gen_user_agent(self) -> str:
            # pyspark.sql.connect.client.core will set most version information,
            # such as version of pyspark and dbconnect.
            #
            # Here we additionally set userAgent as provided by client
            # indication that 'dbconnect-session' api was used and
            # version of databricks-sdk

            user_agent = " ".join(
                [
                    f"{self._user_agent}",
                    "databricks-session",
                    f"databricks-sdk/{version('databricks-sdk')}",
                ]
            )
            return user_agent.strip()

        @staticmethod
        def _verify_sdk_version():
            """
            This check is added for Databricks Notebooks compatibility on DBR 15 clusters,
            since they use an older version of the SDK by default.

            When databricks-connect is installed from PyPi, the latest version of the SDK will be
            downloaded.
            In notebooks using DBR 15 clusters, the user has to upgrade the SDK manually,
            if they want to create a new sessions instead of using the default session that the
            notebook creates.
            """
            sdk_version = version('databricks-sdk')
            if Version(sdk_version) < Version("0.29.0"):
                raise Exception(
                    f"Unsupported SDK version {sdk_version}. Please upgrade to version 0.29.0 or "
                    "higher. You can use `pip install --upgrade databricks-sdk`")


    # This is a class property used to create a new instance of :class:DatabricksSession.Builder.
    # Autocompletion for VS Code breaks when usign the custom @classproperty decorator. Hence,
    # we initialize this later, by explicitly calling the classproperty decorator.
    builder: "DatabricksSession.Builder"
    """Create a new instance of :class:DatabricksSession.Builder"""


DatabricksSession.builder = classproperty(lambda cls: cls.Builder())
