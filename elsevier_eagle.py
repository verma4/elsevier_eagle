from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.utils import *
from pyspark.sql.catalog import *
from pyspark.sql.streaming import *

import time
import socket
import uuid
import html
import io
import re
import json
import requests
import pytz
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W
import pyspark.sql.utils as U
import pyspark.sql.catalog as C
import pyspark.sql.streaming as S

# Create a timezone object for IST
ist = pytz.timezone('Asia/Kolkata')


#Uat cassandra:
#orgtool-uat-cassandra-1.np-mhub2.elsst.com
#orgtool-uat-cassandra-2.np-mhub2.elsst.com
#orgtool-uat-cassandra-3.np-mhub2.elsst.com

#Prod cassandra:
#orgtool-prod-cassandra-1.mhub.elsevier.com
#orgtoo1-prod-cassandra-2.mhub.elsevier.com
#orgtool-prod-cassandra-3.mhub.elsevier.com

#elsevier:, -acmaffiliation, -affiliation, -anidata, -corrapitaskaudit, -deltabatchesinfo, -deptprofile, -master, -mhubaffiliation, -orgprofile, -patentaffiliation, -rdmaffiliation, -releasesupportfiles
#orgdb:, -acmrelease, -archived_orgs, -archived_workspaces, -collection_entries, -configurationlimits, -counters, -deltabatchesinfo, -insthierarchydetails, -insthierarchydetailsrelease, -job_collection_entries, -jobs, -jobs_excecution_time, -locked_orgs, -lockedorginfo, -org_note, -orgaudit, -orgauditbydate, -orgs, -orgtool_scheduler, -orgtoolproperties, -orgxmlaudit, -release, -useraudit, -userauditbydate, -users, -variantsource, -varianttypes, -working_copy, -workspace_comments, -workspace_history, -workspaces

#---------------------------------------------------------------------------------------------- Cassandra Informations ----------------------------------------------------------------------------------
#----------------------------------------------- Uat Cassandra Connection Testing -----------------------------------------
def uat_cassandra_connection(host='orgtool-uat-cassandra-2.np-mhub2.elsst.com', port=9042, timeout=5):
    """
    Check if Cassandra server is reachable over TCP.
    Returns True if successful, False otherwise.
    """
    spark = SparkSession.builder.getOrCreate()
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(timeout)
        client_socket.connect((host, port))
        client_socket.close()
        return True
    except socket.error:
        return False
    
#----------------------------------------------- Prod Cassandra Connection Testing -----------------------------------------
def prod_cassandra_connection(host='orgtool-prod-cassandra-2.mhub.elsevier.com', port=9042, timeout=5):
    """
    Check if Cassandra server is reachable over TCP.
    Returns True if successful, False otherwise.
    """
    spark = SparkSession.builder.getOrCreate()
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(timeout)
        client_socket.connect((host, port))
        client_socket.close()
        return True
    except socket.error:
        return False
    
#----------------------------------------------- Uat and Prod function for data extraction from orgdb DB -----------------------------------------
def cassandra_orgdb(query: str, env: str = 'prod'):
    """
    Connect to Cassandra (UAT or PROD), execute a CQL query, and return the result as a list of dictionaries.
    Parameters:
        query (str): CQL query to execute (e.g., SELECT ...)
        env (str): Environment name, either 'uat' or 'prod'
    Returns:
        List[Dict]: Query result as a list of dicts
    Query:
        query = "SELECT * FROM collectionmetrics ALLOW FILTERING;"
        data = cassandra_orgdb(query, env='prod')
        display(data)
    """
    spark = SparkSession.builder.getOrCreate()
    schema_map = {
        "collectionmetrics": StructType([
        StructField("collection_name", StringType()),
        StructField("id", StringType()),
        StructField("affiliation_count", IntegerType()),

        StructField("clusterids", ArrayType(
            StructType([
                StructField("cluster_id", IntegerType()),
                StructField("affiliation_count", IntegerType())
            ])
        )),
        StructField("comments", ArrayType(StringType())),
        StructField("curation_type", ArrayType(StringType())),
        StructField("end_time", TimestampType()),
        StructField("forcelinked_ids", ArrayType(
            StructType([
                StructField("org_id", IntegerType()),
                StructField("forcelinks", ArrayType(
                    StructType([
                        StructField("affiliation_id", StringType()),
                        StructField("reason_for_change", StringType())
                    ])
                ))
            ])
        )),
        StructField("matcherinfo", ArrayType(
            StructType([
                StructField("jobid", StringType()),
                StructField("jobinfo", StructType([
                    StructField("submitteddate", StringType()),
                    StructField("schedulerpicktime", StringType()),
                    StructField("matchableorgsdataloadtime", StringType()),
                    StructField("inmemoryloadtime", StringType()),
                    StructField("matcherruntime", StringType()),
                    StructField("processeddate", StringType())
                ]))
            ])
        )),
        StructField("orgid", LongType()),
        StructField("project", StringType()),
        StructField("role", StringType()),
        StructField("start_time", TimestampType()),
        StructField("username", StringType()),
    ]),
        "collection_entries" : StructType([
        StructField("owner", StringType()),
        StructField("collection", StringType()),
        StructField("id", StringType()),
        StructField("acmaffiliation", IntegerType()),
        StructField("affil", StringType()),
        StructField("affiladdress", StringType()),
        StructField("affilcity", StringType()),
        StructField("affilcitygroup", StringType()),
        StructField("affilcountry", StringType()),
        StructField("affiliationid", LongType()),
        StructField("affilorgs", ArrayType(StringType())),
        StructField("affilpostalcode", StringType()),
        StructField("affilstate", StringType()),
        StructField("afttaggedjson", StringType()),
        StructField("ani_pubtype", StringType()),
        StructField("aniid", LongType()),
        StructField("badinput", BooleanType()),
        StructField("canonicaldeptid", LongType()),
        StructField("city", StringType()),
        StructField("correspondence", BooleanType()),
        StructField("country", StringType()),
        StructField("countrycode", StringType()),
        StructField("debug", BooleanType()),
        StructField("debugmessage", StringType()),
        StructField("deptname", StringType()),
        StructField("documentid", StringType()),
        StructField("doi", StringType()),
        StructField("dpm_deptprofileid", LongType()),
        StructField("forcedlinkedaffil", IntegerType()),
        StructField("forcelink_audit", StructType([
            StructField("newcomer_parsed_opmm_list", ArrayType(StringType())),
            StructField("removed_parsed_opmm_list", ArrayType(StringType())),
            StructField("newcomer_incompatible_opmm_list", ArrayType(StringType())),
            StructField("initial_incompatible_opmm_list", ArrayType(StringType())),
            StructField("initial_parsed_opmm_list", ArrayType(StringType()))
        ])),
        StructField("frozen", BooleanType()),
        StructField("groupid", LongType()),
        StructField("incompatibleopmm_list", ArrayType(
            StructType([
                StructField("orgprofileid", LongType())
            ])
        )),
        StructField("irrelevant", BooleanType()),
        StructField("ismultiaffiliation", BooleanType()),
        StructField("isprimary", IntegerType()),
        StructField("isprofilable", IntegerType()),
        StructField("mapped", BooleanType()),
        StructField("matchedaniid", LongType()),
        StructField("matchednearestmainorgid", LongType()),
        StructField("matchednearestmainorgname", StringType()),
        StructField("matchednearestmainurl", StringType()),
        StructField("matchedorgdborg", ArrayType(StructType([
            StructField("orgname", StringType()),
            StructField("nearestorgid", LongType()),
            StructField("nearestorgbestname", StringType()),
            StructField("generic", BooleanType()),
            StructField("potentiallybadvariant", BooleanType()),
            StructField("varianttype", StringType()),
            StructField("cities", ArrayType(StringType())),
            StructField("states", ArrayType(StringType())),
            StructField("twodigitcountrycodes", ArrayType(StringType())),
            StructField("subsuperorg", BooleanType())
        ]))),
        StructField("matchedorgprofileids", ArrayType(LongType())),
        StructField("matchedseqnum", IntegerType()),
        StructField("matchedstatus", StringType()),
        StructField("matchedtaggertype", StringType()),
        StructField("multipleunrelatedorgs", IntegerType()),
        StructField("nearestmain", LongType()),
        StructField("opm_orgprofileid", LongType()),
        StructField("opmm", StringType()), #this should be check frozen<orgdb.opmm>
        StructField("orgname", StringType()),
        StructField("parityaffiladdress", StringType()),
        StructField("parityaffilcity", StringType()),
        StructField("parityaffilcountry", ArrayType(StringType())),
        StructField("parityaffilorgs", ArrayType(StringType())),
        StructField("parityaffilpostalcode", StringType()),
        StructField("parityaffilstate", StringType()),
        StructField("paritynormalizedcity", StringType()),
        StructField("parsedopm_list", ArrayType(
            StructType([
                StructField("orgprofileid", LongType()),
                StructField("orgname", StringType())
            ])
        )),
        StructField("parsedopmm_list", ArrayType(StructType([
            StructField("orgprofileid", LongType())
            ])
        )),
        StructField("patentaffiliationid", StringType()),
        StructField("patentid", StringType()),
        StructField("profilable", BooleanType()),
        StructField("seqnum", IntegerType()),
        StructField("solr_query", StringType()),
        StructField("sourcexml", StringType()),
        StructField("sourcexmltaggedjson", StringType()),
        StructField("tagged", IntegerType()),
        StructField("taggedcity", IntegerType()),
        StructField("taggedxml", StringType()),
        StructField("target_forcelinked_ids", ArrayType(LongType())),
        StructField("type", StringType()),
        StructField("unifiedaffilid", StringType()),
        StructField("unifiedid", StringType()),
        StructField("vendortaggedjson", StringType()),
        StructField("xml", StringType()),
        StructField("year", LongType())
    ]),
        "orgs" : StructType([
        StructField("id", LongType(), True),
        StructField("acronyms", StringType(), True),
        StructField("address_primary", BooleanType(), True),
        StructField("addresses", StringType(), True),
        StructField("annotation", StructType([
            StructField("org_history", StringType(), True),
            StructField("org_location", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("source", StructType([
                StructField("value", StringType(), True),
                StructField("id", StringType(), True),
            ])),
        ]), True),
        StructField("audit", StringType(), True),
        StructField("autype", StringType(), True),
        StructField("certainty_scores", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("dacategory", StringType(), True),
        StructField("dahierarchylevel", LongType(), True),
        StructField("daontstatus", StringType(), True),
        StructField("daorglevelcount", LongType(), True),
        StructField("dastatus", StringType(), True),
        StructField("deprecated", BooleanType(), True),
        StructField("domain", StringType(), True),
        StructField("email", StringType(), True),
        StructField("family", StringType(), True),
        StructField("generic", BooleanType(), True),
        StructField("is_primary", BooleanType(), True),
        StructField("main", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("official_name", StringType(), True),
        StructField("orgaddresses", ArrayType(StructType([
            StructField("addressline1", StringType(), True),
            StructField("addressline2", StringType(), True),
            StructField("isstandardaddress", BooleanType(), True),
            StructField("city", StringType(), True),
            StructField("subcity", StringType(), True),
            StructField("state", StringType(), True),
            StructField("substate", StringType(), True),
            StructField("country", StringType(), True),
            StructField("threelettercountry", StringType(), True),
            StructField("alternatecountry", StringType(), True),
            StructField("postalcode", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("fax", StringType(), True),
            StructField("isactive", BooleanType(), True),
            StructField("source", ArrayType(StringType()), True),
            StructField("target", ArrayType(StringType()), True),
            StructField("permission", StringType(), True),
        ])), True),
        StructField("orgcreatedate", StringType(), True), # Date
        StructField("orgcredits", StringType(), True),
        StructField("orgendyear", StringType(), True), # Date
        StructField("orgids", ArrayType(StructType([
            StructField("value", StringType(), True),
            StructField("type", StringType(), True),
        ])), True),
        StructField("orglevel", StringType(), True),
        StructField("orglevelcount", LongType(), True),
        StructField("orgnames", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("isstandardname", BooleanType(), True),
            StructField("isformername", BooleanType(), True),
            StructField("isnativelanguage", BooleanType(), True),
            StructField("istransliteration", BooleanType(), True),
            StructField("isnamevariant", BooleanType(), True),
            StructField("scriptcode", StringType(), True),
            StructField("languagecode", StringType(), True),
            StructField("source", ArrayType(StringType()), True),
            StructField("target", ArrayType(StringType()), True),
            StructField("permission", StringType(), True),
            StructField("isdaorgname", StringType(), True),
        ])), True),
        StructField("orgproductionstatus", StructType([
            StructField("value", StringType(), True),
            StructField("date", StringType(), True),
        ]), True),
        StructField("orgrels", StringType(), True),
        StructField("orgsectors", StringType(), True),
        StructField("orgservice", StringType(), True),
        StructField("orgstartyear", StringType(), True),
        StructField("orgurls", StringType(), True),
        StructField("permission", StringType(), True),
        StructField("quality", LongType(), True),
        StructField("ranks", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("solr_query", StringType(), True),
        StructField("source", ArrayType(StringType()), True),
        StructField("stand_alone", StringType(), True),
        StructField("state", StringType(), True),
        StructField("status", StringType(), True),
        StructField("superorgs", ArrayType(StructType([
            StructField("super_id", LongType(), True),
            StructField("rel_type", StringType(), True),
            StructField("is_specific", StringType(), True),
            StructField("from_date", StringType(), True),
            StructField("to_date", StringType(), True),
            StructField("source", ArrayType(StringType()), True),
            StructField("target", ArrayType(StringType()), True),
            StructField("startyear", StringType(), True),
            StructField("endyear", StringType(), True),
        ])), True),
        StructField("target", ArrayType(StringType()), True),
        StructField("types", StringType(), True),
        StructField("url", StringType(), True),
        StructField("val", StringType(), True),
        StructField("variants", StringType(), True),
    ]),
        "collections" : StructType([
        StructField("owner", StringType(), True),
        StructField("name", StringType(), True),
        StructField("approved", StringType(), True),
        StructField("approveddate", StringType(), True),
        StructField("approvedstatus", StringType(), True),
        StructField("approvedtime", TimestampType(), True),
        StructField("count", StringType(), True),
        StructField("createddate", TimestampType(), True),
        StructField("is_active", StringType(), True),
        StructField("lastupdateddate", TimestampType(), True),
        StructField("lock_holder", StringType(), True),
        StructField("lock_owner_history", StringType(), True),
        StructField("matcheriterationns", ArrayType(
            StructType([
                StructField("submitteddate", StringType(), True),
                StructField("processeddate", StringType(), True)
                ])), True),
        StructField("matcherprocessedtime", TimestampType(), True),
        StructField("matcherstatus", StringType(), True),
        StructField("message", StringType(), True),
        StructField("processed", StringType(), True),
        StructField("source", StringType(), True),
        StructField("taskids", StringType(), True),
    ]),
        # 🔸 Add more table schemas as needed
    }
    #Extract table name from query
    table_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not table_match:
        raise ValueError("Could not determine table name from query.")
    table_name = table_match.group(1).lower()
    if table_name not in schema_map:
        raise ValueError(f"No schema defined for table: {table_name}")
    selected_schema = schema_map[table_name]

    host_map = {
        'uat': 'orgtool-uat-cassandra-2.np-mhub2.elsst.com',
        'prod': 'orgtool-prod-cassandra-2.mhub.elsevier.com'
    }
    if env not in host_map:
        raise ValueError("Invalid environment. Choose 'uat' or 'prod'.")
    try:
        cluster = Cluster(
            [host_map[env]],
            port=9042,
            protocol_version=4,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())},
        )
        session = cluster.connect()
        session.set_keyspace('orgdb')
        rows = session.execute(query)
        data = []
        for row in rows:
            row_dict = {}
            for column, value in row._asdict().items():
                if isinstance(value, uuid.UUID):
                    row_dict[column] = str(value)
                elif isinstance(value, set):
                    row_dict[column] = list(sorted(value))
                else:
                    row_dict[column] = value
            data.append(row_dict)
        return spark.createDataFrame(data, schema=selected_schema)
    except Exception as e:
        print(f"Error fetching data from Cassandra ({env}), db: 'orgdb': {e}")
        return None
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

#----------------------------------------------- Uat and Prod function for data extraction from elsevier DB -----------------------------------------
def cassandra_elsevier(query: str, env: str = 'prod'):
    """
    Connect to Cassandra (UAT or PROD), execute a CQL query, and return the result as a list of dictionaries.
    Parameters:
        query (str): CQL query to execute (e.g., SELECT ...)
        env (str): Environment name, either 'uat' or 'prod'
    Returns:
        List[Dict]: Query result as a list of dicts
    Query:
        query = "SELECT * FROM table_name ALLOW FILTERING;"
        data = cassandra_orgdb(query, env='prod')
        display(data)
    """
    spark = SparkSession.builder.getOrCreate()
    schema_map = {
        "table_name": '',
        # 🔸 Add more table schemas as needed
    }
    #Extract table name from query
    table_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not table_match:
        raise ValueError("Could not determine table name from query.")
    table_name = table_match.group(1).lower()
    if table_name not in schema_map:
        raise ValueError(f"No schema defined for table: {table_name}")
    selected_schema = schema_map[table_name]

    host_map = {
        'uat': 'orgtool-uat-cassandra-2.np-mhub2.elsst.com',
        'prod': 'orgtool-prod-cassandra-2.mhub.elsevier.com'
    }
    if env not in host_map:
        raise ValueError("Invalid environment. Choose 'uat' or 'prod'.")
    try:
        cluster = Cluster(
            [host_map[env]],
            port=9042,
            protocol_version=4,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())},
        )
        session = cluster.connect()
        session.set_keyspace('elsevier')
        rows = session.execute(query)
        data = []
        for row in rows:
            row_dict = {}
            for column, value in row._asdict().items():
                if isinstance(value, uuid.UUID):
                    row_dict[column] = str(value)
                elif isinstance(value, set):
                    row_dict[column] = list(sorted(value))
                else:
                    row_dict[column] = value
            data.append(row_dict)
        return spark.createDataFrame(data, schema=selected_schema)
    except Exception as e:
        print(f"Error fetching data from Cassandra ({env}), db: 'orgdb': {e}")
        return None
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

#----------------------------------------------- Uat and Prod function for data extraction from elsevier, orgdb DB -----------------------------------------
def cassandra_spark(query: str, env: str = 'prod'):
    """
    Connects to Cassandra via Spark, infers keyspace.table from query,
    loads the table, and runs the query as Spark SQL.
    Args:
        query (str): Spark SQL query (must include keyspace.table in FROM).
        env (str): Environment (default: 'prod').
    Returns:
        DataFrame: Result of the query.
    """
    # Remove ALLOW FILTERING
    query_clean = re.sub(r'\s+ALLOW\s+FILTERING\s*', ' ', query, flags=re.IGNORECASE)
    # Extract keyspace and table
    match = re.search(r'from\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', query_clean, re.IGNORECASE)
    if not match:
        raise ValueError("Query must contain keyspace.table in FROM clause (e.g., orgdb.lockedorginfo)")
    keyspace, table = match.groups()
    # Pick Cassandra host
    if env == 'prod':
        cassandra_host = "orgtool-prod-cassandra-2.mhub.elsevier.com"
    elif env == 'uat':
        cassandra_host = "orgtool-uat-cassandra-2.mhub.elsevier.com"
    else:
        raise ValueError(f"Unknown env: {env}")
    spark = SparkSession.builder \
        .appName("CassandraFullExport") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.catalog.cassandracat", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()
    # Load Cassandra table
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    # Register as temp view
    df.createOrReplaceTempView(table)
    # Replace keyspace.table with just table for Spark SQL
    query_spark = re.sub(rf"{keyspace}\.{table}", table, query_clean, flags=re.IGNORECASE)
    # Run query
    result_df = spark.sql(query_spark)
    return result_df

#---------------------------------------------------------------------------------------------- MHUB Informations ----------------------------------------------------------------------------------
#----------------------------------------------- Mhub: Get Meta Data -----------------------------------------------
from pyspark.sql import DataFrame

def mhub_prod_oracle(query: str):
    """
    Reads data from Oracle DB using Spark JDBC and returns a DataFrame.
    Parameters:
        query (str): SQL query to execute.
    Returns:
        DataFrame: Spark DataFrame with the query result.
    """
    spark = SparkSession.builder.getOrCreate()
    jdbc_hostname = "pmhub.mhub.elsevier.com"
    jdbc_port = "1521"
    jdbc_sid = "pmhub"
    jdbc_username = "parity_india"
    jdbc_password = "elsevier"

    jdbc_url = f"jdbc:oracle:thin:@{jdbc_hostname}:{jdbc_port}:{jdbc_sid}"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", f"({query})") \
        .option("user", jdbc_username) \
        .option("password", jdbc_password) \
        .option("driver", "oracle.jdbc.OracleDriver") \
        .load()
    return df

#---------------------------------------------------------------------------------------------- Solr Informations ----------------------------------------------------------------------------------
#----------------------------------------------- Solr: Get affiliation count based on orgids -----------------------------------------------
def afcount_from_solr_for_orgids(env, type, ids):
    """
    Query:
        df = afcount_from_sol_for_orgids('prod', 'ani', ids)
        display(df)
    """
    spark = SparkSession.builder.getOrCreate()
    # Validate inputs
    if env not in ['prod', 'uat']:
        raise ValueError("Invalid environment. Use 'prod' or 'uat'")
    if type not in ['ani', 'ekb', 'grants', 'acm']:
        raise ValueError("Invalid type. Use 'ani', 'ekb', 'grants', or 'acm'")

    # Solr core mapping based on environment and type
    base_urls = {
        'prod': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.acmaffiliation/query'
        },
        'uat': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-acmaffiliation/query'
        }
    }

    # Extra filters per type
    extra_filters = {
        'ani': "AND isprimary:1 AND NOT correspondence:true",
        'ekb': "AND isprimary:1 AND NOT correspondence:true",
        'grants': "AND NOT isprimary:1 AND NOT correspondence:true",
        'acm': "AND (acmaffiliation:(1))"
    }

    extra_filter = extra_filters[type]
    base_url = base_urls[env][type]

    results = []

    for aff_id in ids:
        query = (
            f"((((nearestmain:{aff_id}) OR (opm_orgprofileid:{aff_id}) OR "
            f"{{!parent which='contentType:affiliation' v=parsedopm_list.orgprofileid:{aff_id}}}) AND NOT "
            f"{{!parent which='contentType:affiliation' v=incompatibleopmm_list.orgprofileid:{aff_id}}}) OR "
            f"{{!parent which='contentType:affiliation' v=parsedopmm_list.orgprofileid:{aff_id}}}) "
            f"{extra_filter}"
        )

        params = {
            'q': query,
            'rows': '1',
            'wt': 'json'
        }

        try:
            response = requests.get(base_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                numFound = data['response']['numFound']
                results.append((aff_id, numFound))
            else:
                print(f"Error for ID {aff_id}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for ID {aff_id}: {e}")

    return spark.createDataFrame(results, ["id", "count"])

#----------------------------------------------- Solr: Get affiliation data based on orgids -----------------------------------------------
def data_from_solr_for_orgids(env, type, ids):
    """
    Query:
        df = data_from_solr_for_orgids('prod', 'ani', ids)
        display(df)
    """
    spark = SparkSession.builder.getOrCreate()
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("affil", StringType(), True),
        StructField("affiladdress", StringType(), True),
        StructField("affilcity", StringType(), True),
        StructField("affilcountry", StringType(), True),
        StructField("affiliationid", LongType(), True),
        StructField("id", StringType(), True),
        StructField("affilorgs", ArrayType(StringType()), True),
        StructField("affilpostalcode", StringType(), True),
        StructField("affilseq", IntegerType(), True),
        StructField("affilstate", StringType(), True),
        StructField("ani_pubtype", StringType(), True),
        StructField("aniid", LongType(), True),
        StructField("canonicaldeptid", LongType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("countrycode", StringType(), True),
        StructField("documentid", StringType(), True),
        StructField("dpm_deptprofileid", LongType(), True),
        StructField("forcedlinkedaffil", LongType(), True),
        StructField("groupid", LongType(), True),
        StructField("isprimary", IntegerType(), True),
        StructField("isprofilable", IntegerType(), True),
        StructField("mapped", BooleanType(), True),
        StructField("multipleunrelatedorgs", LongType(), True),
        StructField("nearestmain", LongType(), True),
        StructField("opm_orgprofileid", LongType(), True),
        StructField("orgname", StringType(), True),
        StructField("tagged", IntegerType(), True),
        StructField("taggedcity", IntegerType(), True),
        StructField("taggedxml", StringType(), True),
        StructField("type", StringType(), True),
        StructField("xml", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("badinput", BooleanType(), True),
        StructField("vendortaggedjson", StringType(), True),
        StructField("afttaggedjson", StringType(), True),
        StructField("taggedstate", IntegerType(), True),
        StructField("ismultiaffiliation", BooleanType(), True),
        StructField("_version_", LongType(), True)
    ])

    # Validate inputs
    if env not in ['prod', 'uat']:
        raise ValueError("Invalid environment. Use 'prod' or 'uat'")
    if type not in ['ani', 'ekb', 'grants', 'acm']:
        raise ValueError("Invalid type. Use 'ani', 'ekb', 'grants', or 'acm'")

    # Solr core mapping based on environment and type
    base_urls = {
        'prod': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.acmaffiliation/query'
        },
        'uat': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-acmaffiliation/query'
        }
    }

    # Extra filters per type
    extra_filters = {
        'ani': "AND isprimary:1 AND NOT correspondence:true",
        'ekb': "AND isprimary:1 AND NOT correspondence:true",
        'grants': "AND NOT isprimary:1 AND NOT correspondence:true",
        'acm': "AND (acmaffiliation:(1))"
    }

    extra_filter = extra_filters[type]
    base_url = base_urls[env][type]
    results = []
    for aff_id in ids:
        query = (
            f"((((nearestmain:{aff_id}) OR (opm_orgprofileid:{aff_id}) "
            f"OR {{!parent which='contentType:affiliation' v=parsedopm_list.orgprofileid:{aff_id}}}) "
            f"AND NOT {{!parent which='contentType:affiliation' v=incompatibleopmm_list.orgprofileid:{aff_id}}}) "
            f"OR {{!parent which='contentType:affiliation' v=parsedopmm_list.orgprofileid:{aff_id}}}) "
            f"{extra_filter}"
        )
        
        params = {
            'q': query,
            'wt': 'json'
        }
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            num_found = data['response']['numFound']
            params['rows'] = num_found
            
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            docs = data['response']['docs']
            
            for doc in docs:
                results.append({
                    "affil": doc.get("affil"),
                    "affiladdress": doc.get("affiladdress"),
                    "affilcity": doc.get("affilcity"),
                    "affilcountry": doc.get("affilcountry"),
                    "affiliationid": doc.get("affiliationid"),
                    "id": doc.get("id"),
                    "affilorgs": doc.get("affilorgs", []),
                    "affilpostalcode": doc.get("affilpostalcode"),
                    "affilseq": doc.get("affilseq"),
                    "affilstate": doc.get("affilstate"),
                    "ani_pubtype": doc.get("ani_pubtype"),
                    "aniid": doc.get("aniid"),
                    "canonicaldeptid": doc.get("canonicaldeptid"),
                    "city": doc.get("city"),
                    "country": doc.get("country"),
                    "countrycode": doc.get("countrycode"),
                    "documentid": doc.get("documentid"),
                    "dpm_deptprofileid": doc.get("dpm_deptprofileid"),
                    "forcedlinkedaffil": doc.get("forcedlinkedaffil"),
                    "groupid": doc.get("groupid"),
                    "isprimary": doc.get("isprimary"),
                    "isprofilable": doc.get("isprofilable"),
                    "mapped": doc.get("mapped"),
                    "multipleunrelatedorgs": doc.get("multipleunrelatedorgs"),
                    "nearestmain": doc.get("nearestmain"),
                    "opm_orgprofileid": doc.get("opm_orgprofileid"),
                    "orgname": doc.get("orgname"),
                    "tagged": doc.get("tagged"),
                    "taggedcity": doc.get("taggedcity"),
                    "taggedxml": doc.get("taggedxml"),
                    "type": doc.get("type"),
                    "xml": doc.get("xml"),
                    "year": doc.get("year"),
                    "badinput": doc.get("badinput"),
                    "vendortaggedjson": doc.get("vendortaggedjson"),
                    "afttaggedjson": doc.get("afttaggedjson"),
                    "taggedstate": doc.get("taggedstate"),
                    "ismultiaffiliation": doc.get("ismultiaffiliation"),
                    "_version_": doc.get("_version_")
                })
        except requests.exceptions.RequestException as e:
            print(f"Request failed for ID {aff_id}: {e}")
        except KeyError:
            print(f"Unexpected response structure for ID {aff_id}: {data}")    
    if results:
        df = spark.createDataFrame(results, schema=schema)
        return df
    else:
        print("No results found.")
        return None
#----------------------------------------------- Solr: Get affiliation data based on affiliationids -----------------------------------------------
def data_from_solr_for_afid(env, type, afids):
    """
    afids should be list ['1','2','3','etc']
    Query:
        df = data_from_solr_for_afid('prod', 'ani', afids)
        display(df)
    """
    spark = SparkSession.builder.getOrCreate()
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("affil", StringType(), True),
        StructField("affiladdress", StringType(), True),
        StructField("affilcity", StringType(), True),
        StructField("affilcountry", StringType(), True),
        StructField("affiliationid", LongType(), True),
        StructField("id", StringType(), True),
        StructField("affilorgs", ArrayType(StringType()), True),
        StructField("affilpostalcode", StringType(), True),
        StructField("affilseq", IntegerType(), True),
        StructField("affilstate", StringType(), True),
        StructField("ani_pubtype", StringType(), True),
        StructField("aniid", LongType(), True),
        StructField("canonicaldeptid", LongType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("countrycode", StringType(), True),
        StructField("documentid", StringType(), True),
        StructField("dpm_deptprofileid", LongType(), True),
        StructField("forcedlinkedaffil", LongType(), True),
        StructField("groupid", LongType(), True),
        StructField("isprimary", IntegerType(), True),
        StructField("isprofilable", IntegerType(), True),
        StructField("mapped", BooleanType(), True),
        StructField("multipleunrelatedorgs", LongType(), True),
        StructField("nearestmain", LongType(), True),
        StructField("opm_orgprofileid", LongType(), True),
        StructField("orgname", StringType(), True),
        StructField("tagged", IntegerType(), True),
        StructField("taggedcity", IntegerType(), True),
        StructField("taggedxml", StringType(), True),
        StructField("type", StringType(), True),
        StructField("xml", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("badinput", BooleanType(), True),
        StructField("vendortaggedjson", StringType(), True),
        StructField("afttaggedjson", StringType(), True),
        StructField("taggedstate", IntegerType(), True),
        StructField("ismultiaffiliation", BooleanType(), True),
        StructField("_version_", LongType(), True)
    ])

    # Validate inputs
    if env not in ['prod', 'uat']:
        raise ValueError("Invalid environment. Use 'prod' or 'uat'")
    if type not in ['ani', 'ekb', 'grants', 'acm']:
        raise ValueError("Invalid type. Use 'ani', 'ekb', 'grants', or 'acm'")

    # Solr core mapping based on environment and type
    base_urls = {
        'prod': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.acmaffiliation/query'
        },
        'uat': {
            'ani': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-affiliation/query',
            'ekb': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-ekbaffiliation/query',
            'grants': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-grantsaffiliation/query',
            'acm': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier-acmaffiliation/query'
        }
    }

    # Extra filters per type
    extra_filters = {
        'ani': "AND isprimary:1 AND NOT correspondence:true",
        'ekb': "AND isprimary:1 AND NOT correspondence:true",
        'grants': "AND NOT isprimary:1 AND NOT correspondence:true",
        'acm': "AND (acmaffiliation:(1))"
    }

    extra_filter = extra_filters[type]
    base_url = base_urls[env][type]

    results = []
    chunks = [afids[i:i + 100] for i in range(0, len(afids), 100)]
    for chunk in chunks:
        query = f"(({ ') OR ('.join([f'affiliationid:{id}' for id in chunk]) })) {extra_filter}"        
        params = {
            'q': query,
            'rows': 100,
            'wt': 'json'
        }
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            docs = data['response']['docs']
            
            for doc in docs:
                results.append({
                    "affil": doc.get("affil"),
                    "affiladdress": doc.get("affiladdress"),
                    "affilcity": doc.get("affilcity"),
                    "affilcountry": doc.get("affilcountry"),
                    "affiliationid": doc.get("affiliationid"),
                    "id": doc.get("id"),
                    "affilorgs": doc.get("affilorgs", []),
                    "affilpostalcode": doc.get("affilpostalcode"),
                    "affilseq": doc.get("affilseq"),
                    "affilstate": doc.get("affilstate"),
                    "ani_pubtype": doc.get("ani_pubtype"),
                    "aniid": doc.get("aniid"),
                    "canonicaldeptid": doc.get("canonicaldeptid"),
                    "city": doc.get("city"),
                    "country": doc.get("country"),
                    "countrycode": doc.get("countrycode"),
                    "documentid": doc.get("documentid"),
                    "dpm_deptprofileid": doc.get("dpm_deptprofileid"),
                    "forcedlinkedaffil": doc.get("forcedlinkedaffil"),
                    "groupid": doc.get("groupid"),
                    "isprimary": doc.get("isprimary"),
                    "isprofilable": doc.get("isprofilable"),
                    "mapped": doc.get("mapped"),
                    "multipleunrelatedorgs": doc.get("multipleunrelatedorgs"),
                    "nearestmain": doc.get("nearestmain"),
                    "opm_orgprofileid": doc.get("opm_orgprofileid"),
                    "orgname": doc.get("orgname"),
                    "tagged": doc.get("tagged"),
                    "taggedcity": doc.get("taggedcity"),
                    "taggedxml": doc.get("taggedxml"),
                    "type": doc.get("type"),
                    "xml": doc.get("xml"),
                    "year": doc.get("year"),
                    "badinput": doc.get("badinput"),
                    "vendortaggedjson": doc.get("vendortaggedjson"),
                    "afttaggedjson": doc.get("afttaggedjson"),
                    "taggedstate": doc.get("taggedstate"),
                    "ismultiaffiliation": doc.get("ismultiaffiliation"),
                    "_version_": doc.get("_version_")
                })
        except requests.exceptions.RequestException as e:
            print(f"Request failed for Chunk {chunk}: {e}")
        except KeyError:
            print(f"Unexpected response structure for Chunk {chunk}: {data}")

    if results:
        df = spark.createDataFrame(results, schema=schema)
        return df
    else:
        print("No results found.")
        return None

#----------------------------------------------- Solr: Get collection data based on collection name -----------------------------------------------
def collection_data_from_solr(env, collection_name):
    """
    Query:
        df = collection_data_from_solr('prod', 'collection_name')
        display(df)
    """
    spark = SparkSession.builder.getOrCreate()
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("collection", StringType(), True),
        StructField("affiliationid", LongType(), True),
        StructField("matchedorgprofileids", ArrayType(StringType()), True),
        StructField("parsedopm_list_orgprofileids", ArrayType(StringType()), True),
        StructField("parsedopmm_list_orgprofileids", ArrayType(StringType()), True),
        StructField("incompatibleopmm_list_orgprofileids", ArrayType(StringType()), True),
        StructField("newcomer_parsed_opmm_list", ArrayType(StringType()), True),
        StructField("newcomer_incompatible_opmm_list", ArrayType(StringType()), True)
    ])

    # Validate inputs
    if env not in ['prod', 'uat']:
        raise ValueError("Invalid environment. Use 'prod' or 'uat'")

    # Solr core mapping based on environment and type
    base_urls = {
        'prod': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/orgdb.collectionentry/select',
        'uat': 'http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/orgdb.collectionentry/select'
    }

    base_url = base_urls[env]
    results = []
    for name in collection_name:
        params = {
            'q': f"collection:{name}",
            'fl': "collection,affiliationid,matchednearestmainorgid,matchedorgprofileids,[parentFilter='contentType:collectionentry' child childFilter='parsedopm_list.orgprofileid: [* TO *]' limit=2147483647],[parentFilter='contentType:collectionentry' child childFilter='incompatibleopmm_list.orgprofileid: [* TO *]' limit=2147483647],[parentFilter='contentType:collectionentry' child childFilter='parsedopmm_list.orgprofileid: [* TO *]' limit=2147483647],[parentFilter='contentType:collectionentry' child childFilter='forcelink_audit.newcomer_parsed_opmm_list: [* TO *]' limit=2147483647]",
            'wt': "json"
        }
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            num_found = data['response']['numFound']
            params['rows'] = num_found
            
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            docs = data['response']['docs']
            
            for doc in docs:
                collection = doc.get("collection")
                affiliationid = doc.get("affiliationid")
                matchedorgprofileids = doc.get("matchedorgprofileids", [])
                parsedopm_orgprofileids = []
                parsedopmm_orgprofileids = []
                incompatibleopmm_orgprofileids = []
                newcomer_parsed_opmm_list = []
                newcomer_incompatible_opmm_list = []
                
                # Loop through child documents if any
                for child in doc.get("_childDocuments_", []):
                    content_type = child.get("contentType")
                    
                    if content_type == "parsedopm_list":
                        parsedopm_orgprofileids.append(child.get("parsedopm_list.orgprofileid"))
                    
                    elif content_type == "parsedopmm_list":
                        parsedopmm_orgprofileids.append(child.get("parsedopmm_list.orgprofileid"))

                    elif content_type == "incompatibleopmm_list":
                        incompatibleopmm_orgprofileids.append(child.get("incompatibleopmm_list.orgprofileid"))
                    
                    elif content_type == "forcelink_audit":
                        # These fields are direct keys under the forcelink_audit contentType child
                        newcomer_parsed_opmm_list.extend(child.get("forcelink_audit.newcomer_parsed_opmm_list", []))
                        newcomer_incompatible_opmm_list.extend(child.get("forcelink_audit.newcomer_incompatible_opmm_list", []))
                
                results.append({
                    "collection": collection,
                    "affiliationid": affiliationid,
                    "matchedorgprofileids": matchedorgprofileids,
                    "parsedopm_list_orgprofileids": parsedopm_orgprofileids,
                    "parsedopmm_list_orgprofileids": parsedopmm_orgprofileids,
                    "incompatibleopmm_list_orgprofileids": incompatibleopmm_orgprofileids,
                    "newcomer_parsed_opmm_list": newcomer_parsed_opmm_list,
                    "newcomer_incompatible_opmm_list": newcomer_incompatible_opmm_list
                })
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        except KeyError:
            print(f"Unexpected response structure: {data}")
    if results:
        df = spark.createDataFrame(results, schema=schema)
        df = df.withColumn("old_parsedopmm_list_orgprofileids", array_except(col("parsedopmm_list_orgprofileids"), col("newcomer_parsed_opmm_list")))
        df = df.withColumn("old_incompatibleopmm_list_orgprofileids", array_except(col("incompatibleopmm_list_orgprofileids"), col("newcomer_incompatible_opmm_list")))
        df = df.withColumn("existing_ids", array_except(array_distinct(array_union(col("parsedopm_list_orgprofileids"), col("old_parsedopmm_list_orgprofileids"))), col("old_incompatibleopmm_list_orgprofileids")))
        df = df.withColumn("new_ids", array_except(array_distinct(array_union(array_distinct(array_union(col("parsedopm_list_orgprofileids"), col("matchedorgprofileids"))), col("parsedopmm_list_orgprofileids"))), col("incompatibleopmm_list_orgprofileids")))
        df = df.drop("parsedopm_list_orgprofileids", "parsedopmm_list_orgprofileids", "incompatibleopmm_list_orgprofileids", "old_parsedopmm_list_orgprofileids", "old_incompatibleopmm_list_orgprofileids")
        return df
    else:
        print("No results found.")
        return None
#---------------------------------------------------------------------------------------------- OrgTool Informations ----------------------------------------------------------------------------------
#----------------------------------------------- OrgTool Collection Creation -----------------------------------------
def collection_creation_affiliationid(affiliationid_list):
    # Ensure sql is available (usually 'spark' in Databricks notebooks)
    spark = SparkSession.builder.appName("AuthRequestToDF").getOrCreate()

    # --- Configuration ---
    # auth_url = "http://10.164.39.24:3030/authentication"
    auth_url = "http://orgtool.mhub.elsevier.com:3030/authentication"

    # --- Credentials ---
    # !! IMPORTANT: Replace placeholders with secure retrieval method !!
    # Example using placeholders (NOT RECOMMENDED FOR PRODUCTION):
    scope_name = 'OrganizationDomain'
    username = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_username")
    password = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_password")

    # --- Build Payload and Headers ---
    payload = {
        "username": username,
        "password": password
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json' # Request JSON response
    }

    # --- Make the POST Request ---
    response_data = None # Initialize variable to hold parsed JSON response data
    #print(f"Making POST request to authentication endpoint: {auth_url}")
    try:
        response = requests.post(auth_url, headers=headers, json=payload, timeout=30) # Use json=payload

        # Check for HTTP errors (e.g., 401 Unauthorized, 403 Forbidden, 5xx Server Error)
        response.raise_for_status()

        print(f"SUCCESS: Authentication request successful (Status: {response.status_code})")
        # Try to parse the JSON response
        try:
            response_data = response.json()
            # Optional: Print received data for debugging
            # print("Response JSON:")
            # print(json.dumps(response_data, indent=2))
        except json.JSONDecodeError:
            print("FAILED: Response received, but it was not valid JSON.")
            print("Response Text:", response.text)
            response_data = None # Ensure it's None if parsing failed

    # --- Handle Request Errors ---
    except requests.exceptions.Timeout:
        print(f"FAILED: Request timed out connecting to {auth_url}.")
    except requests.exceptions.ConnectionError as e:
        print(f"FAILED: Could not connect to the server. Check URL and network connectivity.")
        print(f"Error details: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"FAILED: HTTP Error occurred during authentication.")
        print(f"Status Code: {e.response.status_code}")
        print("Response Body:")
        print(e.response.text) # Print error details from server
    except requests.exceptions.RequestException as e:
        print(f"FAILED: An unexpected error occurred during the request.")
        print(f"Error details: {e}")

    # --- Create DataFrame from Response ---
    auth_df = None # Initialize DataFrame variable
    access_token = None # Initialize access_token variable
    if response_data is not None:
        #print("\nAttempting to create DataFrame from authentication response...")
        try:
            # Authentication responses are often single JSON objects (dict in Python)
            # spark.createDataFrame needs a list of dicts/rows, so wrap it in a list
            if isinstance(response_data, dict):
                data_for_df = [response_data]
                auth_df = spark.createDataFrame(data_for_df)

            # Less common: If the response is already a list of objects
            elif isinstance(response_data, list) and all(isinstance(item, dict) for item in response_data):
                auth_df = spark.createDataFrame(response_data)

            # Handle cases where response is not suitable for direct DataFrame creation
            else:
                print(f"Response data type ({type(response_data)}) cannot be directly converted to DataFrame.")
                # Optionally create a DataFrame with the raw JSON string
                # raw_json_string = json.dumps(response_data)
                # auth_df = spark.createDataFrame([(raw_json_string,)], ["raw_response_json"])

        except Exception as e:
            print(f"Error creating DataFrame from response data: {e}")
            print("Response data that caused error:", response_data)
    else:
        print("\nNo valid JSON response data received, DataFrame not created.")

    # --- Example: Use the DataFrame ---
    # You can now use 'auth_df' if it was created successfully.
    # For example, extract an access token (adjust column name based on actual response)
    if auth_df is not None and 'accessToken' in auth_df.columns:
        try:
            access_token = auth_df.select("accessToken").first()[0]
            #print(f"\nExtracted Access Token (first 10 chars): {access_token[:10]}...")
            # Use the token for subsequent API calls
        except Exception as e:
            print(f"Could not extract access_token from DataFrame: {e}")

    url = "http://orgtool.mhub.elsevier.com:3030/collections/affiliations"

    current_timestamp = datetime.now(ist)
    trigger_timestamp = current_timestamp.strftime("%Y%m%d_%H%M%S")

    collection_name = f"{trigger_timestamp}_fromDataBricks" # Example name, adjust as needed
    user_name = "AiPA" # Make dynamic or get from context if needed
    role_name = "USER"  # Ensure this is appropriate

    # --- Get Affiliation IDs from DataFrame ---

    # <<<<<<<<<<<< MODIFY THIS SECTION >>>>>>>>>>>>>>>>
    # Replace 'your_dataframe' with the actual name of your PySpark DataFrame
    # Replace 'your_id_column' with the actual name of the column containing affiliation IDs
    ids_df = affiliationid_list
    id_column_name = "affiliationid"
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    affiliation_ids_from_df = [] # Initialize empty list
    try:
        collected_rows = ids_df.select(id_column_name).distinct().collect()

        # 4. Extract the ID value from each Row and convert to string
        affiliation_ids_from_df = [str(row[id_column_name]) for row in collected_rows if row[id_column_name] is not None]

        print(f"Successfully collected {len(affiliation_ids_from_df)} unique IDs for {ORGID}.")
        print(f"Sample IDs: {affiliation_ids_from_df}") # Optional: print first 10 IDs

    except Exception as e:
        print(f"ERROR: Failed to collect affiliation IDs from DataFrame. Details: {e}")

    # --- Build the Dynamic Query String (using IDs from DataFrame) ---
    if not affiliation_ids_from_df:
        print("Warning: The list of affiliation IDs collected from the dataframe is empty, collection will not be created.")
        query_string = "() AND isprimary:1 AND NOT correspondence:true" # Or handle error upstream
    else:
        # --- Build the JSON Payload ---
        payload = {
            "collectionName": collection_name,
            "user": user_name,
            "role": role_name,
            "source": "Elsevier",
            "selectedSearchCore": "ANI",
            "affiliations": {
                "grouped": [],
                "affiliationIds": affiliation_ids_from_df
            },
            "d2mType": "",
            "isDebug": False,
            "project": "GoldProfileMaintenance",
            "curationType": [
                "Precision"
            ],
        }
        
        # --- Define Headers ---
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60) # Increased timeout slightly
            response.raise_for_status()

            #print(f"\nSUCCESS: Request successful!")
            print(f"Status Code: {response.status_code}")
            return collection_name
            try:
                response_json = response.json()
            except json.JSONDecodeError:
                print("Response Body (Non-JSON):")
                print(response.text)

        # --- Error Handling ---
        except requests.exceptions.Timeout:
            print(f"\nFAILED: Request timed out after 60 seconds.")
        except requests.exceptions.ConnectionError as e:
            print(f"\nFAILED: Could not connect to the server. Check URL and network connectivity.")
            print(f"Error details: {e}")
        except requests.exceptions.HTTPError as e:
            print(f"\nFAILED: HTTP Error occurred.")
            print(f"Status Code: {e.response.status_code}")
            print("Response Body:")
            print(e.response.text)
        except requests.exceptions.RequestException as e:
            print(f"\nFAILED: An unexpected error occurred during the request.")
            print(f"Error details: {e}")


#----------------------------------------------- Affiliationid exclude forcelinking and mapping in childs for particular orgid -----------------------------------------
def solr_exclude_forcelinking_and_child(aff_id):
    
    df_childlist = spark.read.table("orgdb_support.list_hierarchy")
    df_childlist = df_childlist.filter(df_childlist["toplevel_orgid"] == aff_id)
    df_childlist = df_childlist.filter(df_childlist["reltype"].isNotNull())
    df_childlist = df_childlist.filter(df_childlist["final_attribution"]=="include")
    df_childlist = [row["org_id"] for row in df_childlist.select("org_id").distinct().collect()]
    print(f"List of child orgs: {df_childlist}")

    solr_url = pysolr.Solr('http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation', timeout=10)
    # Build your query string
    query = (
        f"((((nearestmain:{aff_id}) OR (opm_orgprofileid:{aff_id}) "
        f"OR {{!parent which='contentType:affiliation' v=parsedopm_list.orgprofileid:{aff_id}}}) "
        f"AND NOT {{!parent which='contentType:affiliation' v=incompatibleopmm_list.orgprofileid:{aff_id}}}) "
        f"OR {{!parent which='contentType:affiliation' v=parsedopmm_list.orgprofileid:{aff_id}}}) "
        f"AND isprimary:1 AND NOT correspondence:true"
    )
    # Build your filter string
    filterlist = "*,[parentFilter='contentType:affiliation' child childFilter='parsedopm_list.orgprofileid: [* TO *]' limit=2147483647], [parentFilter='contentType:affiliation' child childFilter='incompatibleopmm_list.orgprofileid: [* TO *]' limit=2147483647], [parentFilter='contentType:affiliation' child childFilter='parsedopmm_list.orgprofileid: [* TO *]' limit=2147483647]"
    # Build your parameters
    params = {
        'q': query,
        'fl': filterlist,
        'wt': 'json'
    }
    # Parsed your query
    response = solr_url.search(**params)
    # Step 1: Get total number of matching documents
    num_found = response.hits
    # Assgined rows in the next request
    params['rows'] = num_found
    # Step 2: Fetch all matching documents
    response = solr_url.search(**params)

    # Convert the response to a DataFrame
    data = [doc for doc in response]
    df_data = spark.createDataFrame(data)
    df_data = df_data.select("affiliationid", "nearestmain", "opm_orgprofileid", "_childDocuments_")
    print(f"Total affiliations count: {df_data.count()}")

    # Explode for child orgs
    df_data = df_data.withColumn("child_doc", explode(col("_childDocuments_")))
    df_data = df_data.withColumn("parsedopm", col("child_doc")["parsedopm_list.orgprofileid"])
    df_data = df_data.withColumn("parsedopmm", col("child_doc")["parsedopmm_list.orgprofileid"])
    df_data = df_data.withColumn("incompatibleopmm", col("child_doc")["incompatibleopmm_list.orgprofileid"])
    df_data = df_data.groupBy("affiliationid").agg(collect_set("nearestmain").alias("nearestmain"), collect_set("opm_orgprofileid").alias("opm_orgprofileid"), collect_set("parsedopm").alias("parsedopm"), collect_set("parsedopmm").alias("parsedopmm"), collect_set("incompatibleopmm").alias("incompatibleopmm"))
    df_data = df_data.withColumn("parsedopm", col("parsedopm").cast(ArrayType(LongType())))
    df_data = df_data.withColumn("parsedopmm", col("parsedopmm").cast(ArrayType(LongType())))
    df_data = df_data.withColumn("incompatibleopmm", col("incompatibleopmm").cast(ArrayType(LongType())))

    # Calculated final orgids and removed childs orgs
    df_data = df_data.withColumn("final_main", array_union(col("nearestmain"), col("opm_orgprofileid")))
    df_data = df_data.withColumn("final_main", array_union(col("final_main"), col("parsedopm")))
    df_data = df_data.withColumn("final_main", array_union(col("final_main"), col("parsedopmm")))
    df_data = df_data.withColumn("final_main", array_except(col("final_main"), col("incompatibleopmm")))
    df_data = df_data.withColumn("child_list", lit(df_childlist).cast(ArrayType(LongType())))
    df_data = df_data.withColumn("found_childorgs", when(size(array_intersect(col("final_main"), col("child_list"))) > 0, "yes").otherwise("no"))
    df_data = df_data.filter(df_data["found_childorgs"] == "no")
    print(f"Affiliations count after remove childs: {df_data.count()}")

    # Removed forcelinking.
    df_data = df_data.withColumn("found_forcelinking", when(size(array_intersect(col("parsedopmm"), array(lit(aff_id)))) > 0, "yes").otherwise("no"))
    df_data = df_data.filter(df_data["found_forcelinking"] == "no")
    print(f"Affiliations count after remove forcelink: {df_data.count()}")

    return df_data





#---------------------------------------------------------------------------------------------- Scopus Informations ----------------------------------------------------------------------------------
#----------------------------------------------- Scopus: Get citation data -----------------------------------------------
def extract_scopus_citation_data(eid_list):
    spark = SparkSession.builder.getOrCreate()
    # Step 1: Find latest snapshot table name
    latest_snapshot = (
        spark.sql("SHOW TABLES IN scopus")
        .filter(col("tableName").startswith("ani_202"))
        .orderBy(desc("tableName"))
        .select("tableName")
        .first()[0]
    )
    print(f"Latest snapshot Date: {latest_snapshot}")
    
    # Step 2: Load the table
    df = spark.read.table(f"scopus.{latest_snapshot}")
    
    # Step 3: Filter only Scopus/MEDL documents
    df = df.withColumn(
        "ReadScopusData",
        when(
            array_contains(col("dbcollections"), "SCOPUS") |
            array_contains(col("dbcollections"), "MEDL"),
            "true"
        ).otherwise("false")
    ).filter(col("ReadScopusData") == "true")
    
    # Step 4: Explode citations
    df = df.select("Eid", "citation_title", posexplode("citations").alias("seq", "citations"))
    df = df.withColumn("seq", col("seq") + 1)

    # Step 5: Filter for matched citations only
    df = df.filter(col("citations").isin(eid_list)).distinct()

    # Step 6: Explode citation_title (array of structs)
    flat_df = (
        df.withColumn("citation_title_exploded", explode("citation_title"))
        .select(
            "Eid",
            "seq",
            "citations",
            col("citation_title_exploded.title").alias("title"),
            col("citation_title_exploded.lang").alias("language"),
            col("citation_title_exploded.original").alias("original_title")
        )
    )
    flat_df = flat_df.select(col("citations").alias("Citations_Eid"), col("seq").alias("Citations_Seq"), "Eid", col("title").alias("Citations_Title"), col("language").alias("Citations_Language"), col("original_title").alias("Citations_Original"))
    flat_df = flat_df.sort("Citations_Eid", "Citations_Seq", "Eid")
    return flat_df

#---------------------------------------------------------------------------------------------- Databricks and S3 Informations ----------------------------------------------------------------------------------
#----------------------------------------------- Read OrgDB Files from Catalog -----------------------------------------
def read_orgdb_xml_catalog(filename: str = None, snapshot: str = None):
    """
    Reads a Spark table and returns it in a dictionary with filename as the key.
    Query:
        df = read_orgdb_xml_catalog(filename="orgdb")
        df = read_orgdb_xml_catalog(snapshot="20250601")
        df = read_orgdb_xml_catalog("orgdb", "20250601")
    """
    spark = SparkSession.builder.getOrCreate()
    result = {}
    if filename:
        result.update({
            filename: spark.read.table(f'orgdb_support.{filename}')
        })
    
    if snapshot:
        result.update({
            "orgdb": spark.read.table(f'orgdb_support.orgdb_{snapshot}'),
            "address": spark.read.table(f'orgdb_support.address_{snapshot}'),
            "documentcount": spark.read.table(f'orgdb_support.documentcount_{snapshot}'),
            "hierarchy": spark.read.table(f'orgdb_support.hierarchy_{snapshot}'),
            "notes": spark.read.table(f'orgdb_support.notes_{snapshot}'),
            "orgname": spark.read.table(f'orgdb_support.orgname_{snapshot}'),
            "variants": spark.read.table(f'orgdb_support.variants_{snapshot}')
        })
    print(result.keys())
    if not result:
        raise ValueError("Please provide at least one of: filename or snapshot.")
    return result

#----------------------------------------------- Read Other files from Catalog -----------------------------------------
def read_file_from_catalog(dbname: str = None, filename: str = None):
    """
    Reads a Spark table and returns it in a dictionary with filename as the key.
    Query:
        df = read_file_from_catalog('evaluations', 'ai_list_orgs_with_allorgs_batch')
        display(df)
    """
    spark = SparkSession.builder.getOrCreate()
    if not dbname or not filename:
        raise ValueError("Please provide both: dbname and filename")
    try:
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.table(f'{dbname}.{filename}')
        return df
    except Exception as e:
        print(f"Error reading table {dbname}.{filename}: {e}")
        return None

#----------------------------------------------- Read Files from S3 -----------------------------------------
def read_file_s3_amarnath(filename: str):
    """
    Reads a Spark table and returns it in a dictionary with filename as the key.
    Query:
        df = read_file_s3_amarnath()
        display(df["list_state"])
    """
    spark = SparkSession.builder.getOrCreate()
    print("files_name: 'list_abbreviation', 'list_batchuploaded', 'list_cleanupids', 'list_country', 'list_country_capital', 'list_country_xml', 'list_country_xml_bkup', 'list_daterange', 'list_degreeofdifficulty', 'list_ekb_to_pui', 'list_evaluation', 'list_imp', 'list_scopus_coverage', 'list_state', 'list_status_frozenreview', 'list_stopword', 'list_three2two', 'list_ticketids', 'list_usersdetails'")
    return {
        filename: spark.read.csv(path=f'/mnt/els/evaluation/amarnath/{filename}', sep='\t', quote='', header=True, inferSchema=True),
        "list_abbreviation": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_abbreviation', sep='\t', quote='', header=True, inferSchema=True),
        "list_batchuploaded": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_batchuploaded', sep='\t', quote='', header=True, inferSchema=True),
        "list_cleanupids": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_cleanupids', sep='\t', quote='', header=True, inferSchema=True),
        "list_country": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_country', sep='\t', quote='', header=True, inferSchema=True),
        "list_country_capital": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_country_capital', sep='\t', quote='', header=True, inferSchema=True),
        "list_country_xml": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_country_xml', sep='\t', quote='', header=True, inferSchema=True),
        "list_country_xml_bkup": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_country_xml_bkup', sep='\t', quote='', header=True, inferSchema=True),
        "list_daterange": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_daterange', sep='\t', quote='', header=True, inferSchema=True),
        "list_degreeofdifficulty": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_degreeofdifficulty', sep='\t', quote='', header=True, inferSchema=True),
        "list_ekb_to_pui": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_ekb_to_pui', sep='\t', quote='', header=True, inferSchema=True),
        "list_evaluation": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_evaluation', sep='\t', quote='', header=True, inferSchema=True),
        "list_imp": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_imp', sep='\t', quote='', header=True, inferSchema=True),
        "list_scopus_coverage": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_scopus_coverage', sep='\t', quote='', header=True, inferSchema=True),
        "list_state": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_state', sep='\t', quote='', header=True, inferSchema=True),
        "list_status_frozenreview": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_status_frozenreview', sep='\t', quote='', header=True, inferSchema=True),
        "list_stopword": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_stopword', sep='\t', quote='', header=True, inferSchema=True),
        "list_three2two": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_three2two', sep='\t', quote='', header=True, inferSchema=True),
        "list_ticketids": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_ticketids', sep='\t', quote='', header=True, inferSchema=True),
        "list_usersdetails": spark.read.csv(path='/mnt/els/evaluation/amarnath/list_usersdetails', sep='\t', quote='', header=True, inferSchema=True),
    }

#---------------------------------------------------------------------------------------------- Extra functions ----------------------------------------------------------------------------------
#----------------------------------------------- First -----------------------------------------------
def decode_html_entities(html_string):
    if html_string:
        soup = BeautifulSoup(html_string, "html.parser")
        return soup.get_text()
    return html_string

#----------------------------------------------- Second -----------------------------------------------
def unescape_html(value):
    if isinstance(value, str):
        return html.unescape(value)
    return value
unescape_html_udf = udf(unescape_html, StringType())

#----------------------------------------------- Open AI Schema -----------------------------------------------
def get_schema_for_openai():
    return StructType([
    StructField("custom_id", StringType(), True),
    StructField("method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("body", StructType([
        StructField("model", StringType(), True),
        StructField("messages", ArrayType(StructType([
            StructField("role", StringType(), True),
            StructField("content", StringType(), True)
            ])), True),
        StructField("temperature", FloatType(), True),
        StructField("top_p", FloatType(), True),
        StructField("response_format", StructType([
            StructField("type", StringType(), True)
        ]), True)
    ]), True)
])

#----------------------------------------------- Open AI Prompt -----------------------------------------------
Prompt_OpenAi_PA = """You are an Organization Matching Validator specialized in preventing wrong matches between affiliations and organization profiles. ## Input: - OrgInfo: Contains canonical organization name and location. It can also include its former_name, departments or sub organizations, - AFT_JSON: Machine-tagged affiliation - Vendor_JSON: Human-tagged affiliation ## Core Matching Rules: - If the raw affiliation (assume AFT JSON's full raw string as raw) unambiguously represents the same organization in **OrgInfo**, set "Raw": true - If the JSON tagged by **AFT** can be disambiguated to match the organization and location in **OrgInfo**, set "Aft": true - If the JSON tagged by **Vendor** can be disambiguated to match the organization and location in **OrgInfo**, set "Vendor": true ## Matching guidelines to follow: - Only use the provided information while matching. DO NOT assume other locations and/or relationships ever - However, use the provided departmental info and consider it a match if department is mentioed or truly belongs to the main organization in **OrgInfo* - Parse and use location information for disambiguation even when incorrectly tagged (e.g., "orgs":[{"text":University A, City B}]" should be treated same as "orgs":[{"text":University A}],cities":["City B"]") - Ignore academic titles/positions tagged as <orgs> (e.g., "Professor") while still considering the actual institution name for matching ## Common Error Cases to Check: - Substring matches that could belong to multiple institutions - Similar named institutions in different locations - Basing matches solely on based of location - Generic institution names without distinguishing elements - Often in chinese medical affiliations, **hospital** affiliated to **university**, the match is wrongly determined university. But it should be **hospital** as that is where the author's did research and not in the affiliated university. These kind of hospitals are first class citizens in our products. Thus, in such cases, treat affiliated hospitals as independent entities from their parent universities - Not getting matched due to minor typos - Getting confused by multiple insitutes. We only care about whether the JSON matches the provided OrgInfo without ambiguity. ## Output Format: { "Raw": <true|false>, "Aft": <true|false>, "Vendor": <true|false>, "Reasoning": "<Clear explanation of match decision>"} Do not include any other text, analysis, or markdown formatting in your response. Return only the JSON object."""

#----------------------------------------------- Define replacements -----------------------------------------------
old_chars = "ÀÁÂÃÄÅÇÈÉÊËÍÎÐÑÓÔÖØÙÚÜÝàáâãäåçèéêëìíîïðñòóôõöøùúûüýưǧǵṛṭṈạảấầậếễệỉịọốồỗộờởủừỹӦӧŠŽšžŸČčěĞğīĭĺļŌōŚśŜŞşūŭůźŻżŽž‘’“”–ĀāăąćĐđēĕėęĠĢģİĶķŁłńņňŐőŘřŠšţťũűųŵŹȇȘș"
new_chars = "AAAAAACEEEEIIDNOOOOUUUYaaaaaaceeeeiiiionoooooouuuuyuggrtNaaaaaeeeiiooooooouuyOoSZszYCceGgiillOoSsSSsuuuzZzZz''""-AaaacDdeeeeGGgIKkLlnnnOoRrSsttuuuwZeSs"

nor_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ012345678)9("

special_chars1 = " ,©,ª,«,­,®,°,²,³,´,µ,·,¸,º,»,½,¾,À,Á,Â,Ã,Ä,Å,Æ,Ç,È,É,Ê,Ë,Í,Î,Ð,Ñ,Ó,Ô,Ö,Ø,Ù,Ú,Ü,Ý,ß,à,á,â,ã,ä,å,æ,ç,è,é,ê,ë,ì,í,î,ï,ð,ñ,ò,ó,ô,õ,ö,ø,ù,ú,û,ü,ý,þ,෾,ư,ǧ,ǵ,ṛ,ṭ,Ṉ,ạ,ả,ấ,ầ,ậ,ế,ễ,ệ,ỉ,ị,ọ,ố,ồ,ỗ,ộ,ờ,ở,ủ,ừ,ỹ,ʹ,ʻ,ˑ,β,Ӧ,ӧ,Š,Ž,š,œ,ž,Ÿ,Č,č,ě,Ğ,ğ,ī,ĭ,ĺ,ļ,Ō,ō,Ś,ś,Ŝ,Ş,ş,ū,ŭ,ů,ź,Ż,ż,Ž,ž,Ə,ț,Ⱦ,●,̌,К,М,Н,л,п,،,,‘,’,“,”,–,Ā,ā,ă,ą,ć,Đ,đ,ē,ĕ,ė,ę,Ġ,Ģ,ģ,İ,ı,Ķ,ķ,Ł,ł,ń,ņ,ň,Ő,ő,Œ,œ,Ř,ř,Š,š,ţ,ť,ũ,ű,ų,ŵ,Ź,​,‌,‍,‎,“,”,‪, ,ȇ,Ș,ș,ȧ,ə,̀,́,̂,̃,̇,̈,̧,̱,Α,Ι,А,Б,В,Е,И,С,Т,а,г,е,у, ,‐,‑,–,—,‘,’,•,⁄,⁠,№,Ⅱ,∂,−,　,〒,ﬁ,﻿,，,－,．,Ｋ,ｍ,＆,（,１,２,&,>"
special_chars2 = ",©,ª,«,,®,°,²,³,´,µ,·,¸,º,»,½,¾,A,A,A,A,A,A,Æ,C,E,E,E,E,I,I,D,N,O,O,O,O,U,U,U,Y,ß,a,a,a,a,a,a,æ,c,e,e,e,e,i,i,i,i,ð,n,o,n,n,n,n,o,u,u,u,u,y,þ,,u,g,g,r,t,N,a,a,a,a,a,e,e,e,i,i,o,o,o,o,o,o,o,u,u,y,ʹ,ʻ,ˑ,β,O,o,S,Z,s,œ,z,Y,C,c,e,G,g,i,i,l,l,O,o,S,s,S,S,s,u,u,u,z,Z,z,Z,z,Ə,t,Ⱦ,●,̌,K,M,H,л,п,،,,',',",",-,A,a,a,a,c,D,d,e,e,e,e,G,G,g,I,ı,K,k,L,l,n,n,n,O,o,Œ,œ,R,r,S,s,t,t,u,u,u,W,Z,,,,,",",,,e,S,s,a,ə,̀,́,̂,̃,̇,̈,̧,̱,Α,I,A,B,B,E,N,C,T,a,г,e,y,,‐,‑,–,—,‘,’,•,⁄,,N,Ⅱ,∂,−,,〒,ﬁ,,，,－,．,K,m,&,(,1,2,&,>"

old_chars_master = "&#x00a0;&#x00a9;&#x00aa;&#x00ab;&#x00ad;&#x00ae;&#x00b0;&#x00b2;&#x00b3;&#x00b4;&#x00b5;&#x00b7;&#x00b8;&#x00ba;&#x00bb;&#x00bd;&#x00be;&#x00c0;&#x00c1;&#x00c2;&#x00c3;&#x00c4;&#x00c5;&#x00c6;&#x00c7;&#x00c8;&#x00c9;&#x00ca;&#x00cb;&#x00cd;&#x00ce;&#x00d0;&#x00d1;&#x00d3;&#x00d4;&#x00d6;&#x00d8;&#x00d9;&#x00da;&#x00dc;&#x00dd;&#x00df;&#x00e0;&#x00e1;&#x00e2;&#x00e3;&#x00e4;&#x00e5;&#x00e6;&#x00e7;&#x00e8;&#x00e9;&#x00ea;&#x00eb;&#x00ec;&#x00ed;&#x00ee;&#x00ef;&#x00f0;&#x00f1;&#x00f2;&#x00f3;&#x00f4;&#x00f5;&#x00f6;&#x00f8;&#x00f9;&#x00fa;&#x00fb;&#x00fc;&#x00fd;&#x00fe;&#x0dfe;&#x01b0;&#x01e7;&#x01f5;&#x1e5b;&#x1e6d;&#x1e48;&#x1ea1;&#x1ea3;&#x1ea5;&#x1ea7;&#x1ead;&#x1ebf;&#x1ec5;&#x1ec7;&#x1ec9;&#x1ecb;&#x1ecd;&#x1ed1;&#x1ed3;&#x1ed7;&#x1ed9;&#x1edd;&#x1edf;&#x1ee7;&#x1eeb;&#x1ef9;&#x02b9;&#x02bb;&#x02d1;&#x03b2;&#x04e6;&#x04e7;&#x008a;&#x008e;&#x009a;&#x009c;&#x009e;&#x009f;&#x010c;&#x010d;&#x011b;&#x011e;&#x011f;&#x012b;&#x012d;&#x013a;&#x013c;&#x014c;&#x014d;&#x015a;&#x015b;&#x015c;&#x015e;&#x015f;&#x016b;&#x016d;&#x016f;&#x017a;&#x017b;&#x017c;&#x017d;&#x017e;&#x018f;&#x021b;&#x023e;&#x25cf;&#x030c;&#x041a;&#x041c;&#x041d;&#x043b;&#x043f;&#x060c;&#x0081;&#x0091;&#x0092;&#x0093;&#x0094;&#x0096;&#x0100;&#x0101;&#x0103;&#x0105;&#x0107;&#x0110;&#x0111;&#x0113;&#x0115;&#x0117;&#x0119;&#x0120;&#x0122;&#x0123;&#x0130;&#x0131;&#x0136;&#x0137;&#x0141;&#x0142;&#x0144;&#x0146;&#x0148;&#x0150;&#x0151;&#x0152;&#x0153;&#x0158;&#x0159;&#x0160;&#x0161;&#x0163;&#x0165;&#x0169;&#x0171;&#x0173;&#x0175;&#x0179;&#x200b;&#x200c;&#x200d;&#x200e;&#x201c;&#x201d;&#x202a;&#x202f;&#x0207;&#x0218;&#x0219;&#x0227;&#x0259;&#x0300;&#x0301;&#x0302;&#x0303;&#x0307;&#x0308;&#x0327;&#x0331;&#x0391;&#x0399;&#x0410;&#x0411;&#x0412;&#x0415;&#x0418;&#x0421;&#x0422;&#x0430;&#x0433;&#x0435;&#x0443;&#x2002;&#x2010;&#x2011;&#x2013;&#x2014;&#x2018;&#x2019;&#x2022;&#x2044;&#x2060;&#x2116;&#x2161;&#x2202;&#x2212;&#x3000;&#x3012;&#xfb01;&#xfeff;&#xff0c;&#xff0d;&#xff0e;&#xff2b;&#xff4d;&#xff06;&#xff08;&#xff11;&#xff12;&amp;&gt;"
new_chars_master = " ©ª«­®°²³´µ·¸º»½¾ÀÁÂÃÄÅÆÇÈÉÊËÍÎÐÑÓÔÖØÙÚÜÝßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ෾ưǧǵṛṭṈạảấầậếễệỉịọốồỗộờởủừỹʹʻˑβӦӧŠŽšœžŸČčěĞğīĭĺļŌōŚśŜŞşūŭůźŻżŽžƏțȾ●̌КМНлп،‘’“”–ĀāăąćĐđēĕėęĠĢģİıĶķŁłńņňŐőŒœŘřŠšţťũűųŵŹ​‌‍‎“”‪ ȇȘșȧə̧̱̀́̂̃̇̈ΑΙАБВЕИСТагеу ‐‑–—‘’•⁄⁠№Ⅱ∂−　〒ﬁ﻿，－．Ｋｍ＆（１２&>"


############################################################################################# Logo our team #############################################################################################
print("   ,     #_")
print("   ~\_  ####_")
print("  ~~  \_#####\'")
print("  ~~     \###|")
print("  ~~       \#/ ___")
print("   ~~       V~' '->")
print("    ~~~         /")
print("      ~~._.   _/")
print("         _/ _/")
print("       _/m/'")
print("tf_141")