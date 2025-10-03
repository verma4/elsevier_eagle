# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Task:
# MAGIC   1. Precision agent (fully integrated by Jan 2026)
# MAGIC   2. Metadata agent  (not fully integrated)
# MAGIC   3. Recall agent  (fully integrated)
# MAGIC   4. Hierarchy agent  (not fully integrated)
# MAGIC   5. Curation as a Service (fully integrated)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.utils import *
from pyspark.sql.catalog import *
from pyspark.sql.streaming import *
from openai import AzureOpenAI

import time
import pysolr
import socket
import uuid
import html
import io
import os
import re
import json
import requests
import pytz
import numpy as np
import pandas as pd
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET

# Create a timezone object for IST
ist = pytz.timezone('Asia/Kolkata')

# Azure OpenAI, Initialize the Azure OpenAI client
client = AzureOpenAI(api_key=dbutils.secrets.get("OrganizationDomain", "openai_api_key"), api_version="2024-08-01-preview", azure_endpoint=dbutils.secrets.get("OrganizationDomain", "openai_api_url"))

# COMMAND ----------

# DBTITLE 1,Fun1: Utilities
################## Define UDF that returns a UUID string ##################
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


################## Functions to decode HTML entities ##################
def html_unescape(s):
    if s is None:
        return None
    return html.unescape(s)
html_unescape_udf = udf(html_unescape, StringType())


################## Returns the current timestamp in IST ##################
def get_current_ist_timestamp():
    from datetime import datetime
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    date_yyyymmdd = now.strftime("%Y%m%d")
    date_yyyy_mm_dd = now.strftime("%Y-%m-%d")
    unix_timestamp = int(now.timestamp())
    normal_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    results = {
        "date_yyyymmdd": date_yyyymmdd,
        "date_yyyy_mm_dd": date_yyyy_mm_dd,
        "unix_timestamp": unix_timestamp,
        "normal_timestamp": normal_timestamp
    }
    return results


# COMMAND ----------

# DBTITLE 1,Fun2: Prompts Dictionary
def prompts_dictionary(key=None):
    #---- Created Prompt Dictionary ----#
    results = {
        "prompt1": """You are an Organization Matching Validator specialized in preventing wrong matches between affiliations and organization profiles. ## Input: - OrgInfo: Contains canonical organization name and location. It can also include its former_name, departments or sub organizations - AFT_JSON: Machine-tagged affiliation - Vendor_JSON: Human-tagged affiliation ## Core Matching Rules: - If the raw affiliation (assume AFT JSON's full raw string as raw) unambiguously represents the same organization in **OrgInfo**, set "Raw": true - If the JSON tagged by **AFT** can be disambiguated to match the organization and location in **OrgInfo**, set "Aft": true - If the JSON tagged by **Vendor** can be disambiguated to match the organization and location in **OrgInfo**, set "Vendor": true ## Matching guidelines to follow: - Only use the provided information while matching. DO NOT assume other locations and/or relationships ever - However, use the provided formername info and consider it a match if formername is mentioned or truly belongs to the main organization in **OrgInfo** - However, use the provided departmental info and consider it a match if department is mentioned or truly belongs to the main organization in **OrgInfo** - Parse and use location information for disambiguation even when incorrectly tagged (e.g., "orgs":[{"text":University A, City B}] should be treated same as "orgs":[{"text":University A}],cities:["City B"]") - Ignore academic titles/positions tagged as <orgs> (e.g., "Professor") while still considering the actual institution name for matching ## Common Error Cases to Check: - Substring matches that could belong to multiple institutions - Similar named institutions in different locations - Basing matches solely on based of location - Generic institution names without distinguishing elements - Often in chinese medical affiliations, **hospital** affiliated to **university**, the match is wrongly determined university. But it should be **hospital** as that is where the author's did research and not in the affiliated university. These kind of hospitals are first class citizens in our products. Thus, in such cases, treat affiliated hospitals as independent entities from their parent universities - Not getting matched due to minor typos - Getting confused by multiple institutes. We only care about whether the JSON matches the provided OrgInfo without ambiguity. ## Output Format: { "Raw": <true|false>, "Aft": <true|false>, "Vendor": <true|false>, "Reasoning": "<Clear explanation of match decision>" } Do not include any other text, analysis, or markdown formatting in your response. Return only the JSON object.""",
        "prompt2": """Good Morning""",
        "prompt3": """Amar Nath Verma""",
    }
    if key:
        return key, results.get(key)
    return results

# COMMAND ----------

# DBTITLE 1,Fun3: Masterlist Update from RMS
def masterlist_updated_from_rms():
    #---- Existinglist of orgs from MAIN ----#
    existinglist_of_orgs_main_df = spark.read.table("evaluations.precision_agent_list_of_orgs")
    existinglist_of_orgs_main_orgdb_IDs = [row["Orgid"] for row in existinglist_of_orgs_main_df.select("Orgid").collect()]
    # print(f"Orgs in Main Existing List: {existinglist_of_orgs_main_orgdb_IDs}")
    print(f"Count Orgs in Main Existing List: {len(existinglist_of_orgs_main_orgdb_IDs)}")

    #---- Existing list of orgs from RMS ----#
    existinglist_of_orgs_rms_df = (
        spark.read
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "jdbc:mysql://10.133.240.50:3306/projects")
        .option("dbtable", "tbl_dep_former")
        .option("user", "view")
        .option("password", "view")
        .load()
    )
    existinglist_of_orgs_rms_df = (existinglist_of_orgs_rms_df
                        .withColumn("Datetime_Updated_in_RMS", date_format(col("date_updated"), "yyyy-MM-dd HH:mm:ss"))
                        .withColumn("Orgid", col("org_id").cast("integer"))
                        .withColumn("FormerName", from_json(col("former_name"), ArrayType(StringType())))
                        .withColumn("FormerName", when(col("FormerName").isNull(), array(lit(""))).otherwise(col("FormerName")))
                        .withColumn("DepartmentName", from_json(col("department_name"), ArrayType(StringType())))
                        .withColumn("DepartmentName", when(col("DepartmentName").isNull(), array(lit(""))).otherwise(col("DepartmentName")))
                        )
    existinglist_of_orgs_rms_df = existinglist_of_orgs_rms_df.drop("id", "date_added", "org_id", "former_name", "department_name", "date_updated")
    
    #---- Read Orgdb Data ----#
    readdata_from_orgdb_df = spark.read.table("orgdb_support.list_orgdb")
    readdata_from_orgdb_df = (readdata_from_orgdb_df
                            .select(
                                col("org_id").alias("Orgid"),
                                col("orgvisibility").alias("Orgvisibility"),
                                col("orgname").alias("Orgname"),
                                col("street").alias("Street"),
                                col("city").alias("City"),
                                col("state").alias("State"),
                                col("postalcode").alias("Postalcode"),
                                col("country").alias("Country"))
                            ).na.fill('[]', subset=["Street", "City", "State", "Postalcode", "Country"])
    
    #---- Join Dataframes ----#
    existinglist_of_orgs_rms_df.cache()
    readdata_from_orgdb_df.cache()
    existinglist_of_orgs_rms_orgdb_df = existinglist_of_orgs_rms_df.join(readdata_from_orgdb_df, on="Orgid", how="left")
    existinglist_of_orgs_rms_orgdb_df = (existinglist_of_orgs_rms_orgdb_df
                                        .withColumn("OrgInfo", to_json(
                                            struct(
                                                col("Orgid").alias("org_id"),
                                                col("Orgname").alias("org_name"),
                                                col("FormerName").alias("former_name"),
                                                col("DepartmentName").alias("departments"),
                                                col("Street").alias("street"),
                                                col("City").alias("city"),
                                                col("State").alias("state"),
                                                col("Postalcode").alias("postal_code"),
                                                col("Country").alias("country"),
                                                )))
                                        )
    existinglist_of_orgs_rms_orgdb_df = (existinglist_of_orgs_rms_orgdb_df
                                        .withColumn("UID", uuid_udf())
                                        .withColumn("Date_Added", current_date().cast(DateType()))
                                        .withColumn("Locked_by_TMT", lit(None).cast(StringType()))
                                        .withColumn("Locked_Workspace", lit(None).cast(StringType()))
                                        .withColumn("Picked_Datetime", lit(None).cast(TimestampType()))
                                        .withColumn("List_of_ChildOrgs", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("No_of_ChildOrgs", lit(None).cast(IntegerType()))
                                        .withColumn("DCOP_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("DCOP_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("DCCO_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("DCCO_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("SOLR_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("SOLR_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("EFPMFC_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("EFPMFC_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("FINAL_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("FINAL_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("GROUP_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("GROUP_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("SAMPLE_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("SAMPLE_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("AI_Prompt", lit(None).cast(StringType()))
                                        .withColumn("Json_Filename", lit(None).cast(StringType()))
                                        .withColumn("Input_Batch", lit(None).cast(StringType()))
                                        .withColumn("Output_Batch", lit(None).cast(StringType()))
                                        .withColumn("Save_Datetime", lit(None).cast(TimestampType()))
                                        .withColumn("Status", lit(None).cast(StringType()))
                                        .withColumn("RY_No_of_Xml", lit(None).cast(IntegerType()))
                                        .withColumn("RY_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("RY_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("RY_CollectionName", lit(None).cast(StringType()))
                                        .withColumn("RN_No_of_Xml", lit(None).cast(IntegerType()))
                                        .withColumn("RN_No_of_AffIds", lit(None).cast(IntegerType()))
                                        .withColumn("RN_List_of_AffIds", lit(None).cast(ArrayType(StringType())))
                                        .withColumn("RN_CollectionName", lit(None).cast(StringType()))
                                        )
    existinglist_of_orgs_rms_orgdb_df = existinglist_of_orgs_rms_orgdb_df.select('UID', 'Date_Added', 'Datetime_Updated_in_RMS', 'Orgid', 'Orgvisibility', 'Locked_by_TMT', 'Locked_Workspace', 'Orgname', 'Street', 'City', 'State', 'Postalcode', 'Country', 'FormerName', 'DepartmentName', 'OrgInfo', 'Picked_Datetime', 'List_of_ChildOrgs', 'No_of_ChildOrgs', 'DCOP_List_of_AffIds', 'DCOP_No_of_AffIds', 'DCCO_List_of_AffIds', 'DCCO_No_of_AffIds', 'SOLR_List_of_AffIds', 'SOLR_No_of_AffIds', 'EFPMFC_List_of_AffIds', 'EFPMFC_No_of_AffIds', 'FINAL_List_of_AffIds', 'FINAL_No_of_AffIds', 'GROUP_List_of_AffIds', 'GROUP_No_of_AffIds', 'SAMPLE_List_of_AffIds', 'SAMPLE_No_of_AffIds', 'AI_Prompt', 'Json_Filename', 'Input_Batch', 'Output_Batch', 'Save_Datetime', 'Status', 'RY_No_of_Xml', 'RY_No_of_AffIds', 'RY_List_of_AffIds', 'RY_CollectionName', 'RN_No_of_Xml', 'RN_No_of_AffIds', 'RN_List_of_AffIds', 'RN_CollectionName')
    existinglist_of_orgs_rms_orgdb_IDs = [row["Orgid"] for row in existinglist_of_orgs_rms_orgdb_df.select("Orgid").collect()]
    # print(f"Orgs in RMS Existing List: {existinglist_of_orgs_rms_orgdb_IDs}")
    print(f"Count Orgs in RMS Existing List: {len(existinglist_of_orgs_rms_orgdb_IDs)}")

    #----  Added Newly orgs in MAIN from RMS (Match value: Orgid, Datetime_Updated_in_RMS) ----#
    newlylist_of_orgs_rms_orgdb_df = existinglist_of_orgs_rms_orgdb_df.join(existinglist_of_orgs_main_df, ((existinglist_of_orgs_rms_orgdb_df.Orgid == existinglist_of_orgs_main_df.Orgid) & (existinglist_of_orgs_rms_orgdb_df.Datetime_Updated_in_RMS == existinglist_of_orgs_main_df.Datetime_Updated_in_RMS)), "leftanti")
    newlylist_of_orgs_rms_orgdb_IDs = [row["Orgid"] for row in newlylist_of_orgs_rms_orgdb_df.select("Orgid").collect()]
    # print(f"Newly Orgs in RMS: {newlylist_of_orgs_rms_orgdb_IDs}")
    print(f"Count Newly Orgs in RMS: {len(newlylist_of_orgs_rms_orgdb_IDs)}")
    if newlylist_of_orgs_rms_orgdb_df.count() > 0:
        newlylist_of_orgs_rms_orgdb_df.write.mode("append").saveAsTable("evaluations.precision_agent_list_of_orgs")
        print(f"Updated organizations from RMS.")
    else:
        print(f"Don't Updated organizations from RMS.")
    print(f"---------- Finished (masterlist_updated_from_rms) ----------")

# COMMAND ----------

# DBTITLE 1,Fun4: Locked Organization
def locked_orginfo():
    #---- Locked Orgs ----#
    lockedorgs_df = spark.read.table("evaluations.orgs_daysLocked")
    latest_date = lockedorgs_df.agg(max("ReportDateTime").alias("max_date")).collect()[0]["max_date"]
    lockedorgs_df = (lockedorgs_df
                    .filter(col("Squad") == "Garuda")
                    .filter(col("ReportDateTime") == latest_date)
                    .withColumn("locked_orgs_Locked_by_TMT", lit("yes").cast(StringType()))
                    .select(col("OrgId").alias("Orgid"), "locked_orgs_Locked_by_TMT", col("workspace").alias("locked_orgs_Locked_Workspace"))
                    )
    #---- Existinglist of Main Orgs ----#
    existinglist_of_orgs_main_df = spark.read.table("evaluations.precision_agent_list_of_orgs")
    existinglist_of_orgs_main_lockedorgs_update_df = (existinglist_of_orgs_main_df.alias("exist_orgs")
                                                    .join(lockedorgs_df.alias("locked_orgs"), col("exist_orgs.Orgid") == col("locked_orgs.Orgid"), "left")
                                                    .withColumn("Locked_by_TMT", col("locked_orgs_Locked_by_TMT"))
                                                    .withColumn("Locked_Workspace", col("locked_orgs_Locked_Workspace"))
                                                    .drop(lockedorgs_df.Orgid, "locked_orgs_Locked_by_TMT", "locked_orgs_Locked_Workspace")
                                                    )
    existinglist_of_orgs_main_lockedorgs_update_IDs = [row["Orgid"] for row in existinglist_of_orgs_main_lockedorgs_update_df.select("Orgid").collect()]
    # print(f"Orgs in Main Existing List: {existinglist_of_orgs_main_lockedorgs_update_IDs}")
    print(f"Count Orgs in Main Existing List: {len(existinglist_of_orgs_main_lockedorgs_update_IDs)}")
    existinglist_of_orgs_main_lockedorgs_update_df.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.precision_agent_list_of_orgs")
    print(f"---------- Finished (locked_orginfo) ----------")


# COMMAND ----------

# DBTITLE 1,Fun5: Affiliation IDs from DC
def list_of_affids_from_dc(Orgid):
    #---- Read hiearchy table ----#
    list_of_childorgs = spark.read.table("orgdb_support.list_hierarchy")
    list_of_childorgs = (list_of_childorgs
                        .filter(list_of_childorgs["toplevel_orgid"] == Orgid)
                        .filter(list_of_childorgs["reltype"].isNotNull())
                        .filter(list_of_childorgs["final_attribution"]=="include")
                        )
    list_of_childorgs_IDs = [row["org_id"] for row in list_of_childorgs.select("org_id").distinct().collect()]
    # print(f"Child Orgs: {list_of_childorgs_IDs}")
    print(f"Count Child Orgs: {len(list_of_childorgs_IDs)}")
    
    #---- Read DC Primary ANI Output ----#
    dc_output = spark.read.table("orgdb_support.primary_ani_dcoutput")
    dc_output = (dc_output
                 .withColumn("Has_PrimaryOrgid", when(size(array_intersect(col("BestIDs"), array(lit(str(Orgid))))) > 0, True).otherwise(False))
                 .withColumn("Has_ChildOrgs", when(size(array_intersect(col("BestIDs"), lit(list_of_childorgs_IDs).cast(ArrayType(StringType())))) > 0, True).otherwise(False))
                 )
    list_of_affiliation_onlyprimary_IDs = [
        row["AffID"]
        for row in (
            dc_output
            .filter(col("Has_PrimaryOrgid") == True)
            .filter(col("Has_ChildOrgs") == False)
            .select(col("AffID").cast(LongType()))
            .distinct()
            .collect()
            )
        ]
    print(f"Count affids only for primary org: {len(list_of_affiliation_onlyprimary_IDs)}")
    list_of_affiliation_childorgs_IDs = [
        row["AffID"]
        for row in (
            dc_output
            .filter(col("Has_ChildOrgs") == True)
            .select(col("AffID").cast(LongType()))
            .distinct()
            .collect()
            )
        ]
    print(f"Count affids for child orgs: {len(list_of_affiliation_childorgs_IDs)}")
    return list_of_childorgs_IDs, list_of_affiliation_onlyprimary_IDs, list_of_affiliation_childorgs_IDs


# COMMAND ----------

df = spark.read.table("evaluations.precision_agent_list_of_orgs")
display(df)

# COMMAND ----------

# DBTITLE 1,Fun6: Solr Data of Affiliation
def solr_data_of_affiliation(UID, Orgid, Orgvisibility, OrgInfo, Prompt_Key, Prompt_Value, Normal_Timestamp, Unix_Timestamp, list_of_childorgs_IDs, list_of_affiliation_onlyprimary_IDs, list_of_affiliation_childorgs_IDs):
    def empty_affiliation_df():
        schema = StructType([
            StructField("affiliationid", LongType(), True),
            StructField("nearestmain", LongType(), True),
            StructField("opm_orgprofileid", LongType(), True),
            StructField("_childDocuments_", ArrayType(MapType(StringType(), StringType(), True), True), True),
            StructField("affil", StringType(), True),
            StructField("afttaggedjson", StringType(), True),
            StructField("vendortaggedjson", StringType(), True),
            StructField("year", StringType(), True),
            ])
        return spark.createDataFrame([], schema)

    #---- Solr Query ----#
    solr_url = pysolr.Solr('http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation', timeout=60)
    filterlist = "*,[parentFilter='contentType:affiliation' child childFilter='parsedopm_list.orgprofileid: [* TO *]' limit=2147483647], [parentFilter='contentType:affiliation' child childFilter='incompatibleopmm_list.orgprofileid: [* TO *]' limit=2147483647], [parentFilter='contentType:affiliation' child childFilter='parsedopmm_list.orgprofileid: [* TO *]' limit=2147483647]"
    data = []
    try:
        if Orgvisibility == "true":
            #---- Query for orgvisible true ----#
            query = (
                f"((((nearestmain:{Orgid}) OR (opm_orgprofileid:{Orgid}) "
                f"OR {{!parent which='contentType:affiliation' v=parsedopm_list.orgprofileid:{Orgid}}}) "
                f"AND NOT {{!parent which='contentType:affiliation' v=incompatibleopmm_list.orgprofileid:{Orgid}}}) "
                f"OR {{!parent which='contentType:affiliation' v=parsedopmm_list.orgprofileid:{Orgid}}}) "
                f"AND isprimary:1 AND NOT correspondence:true"
                )
            params = {'q': query, 'fl': filterlist, 'wt': 'json'}
            response = solr_url.search(**params)
            params['rows'] = response.hits
            response = solr_url.search(**params)
            data.extend([doc for doc in response])
        else:
            #---- Query for orgvisible false ---#
            if list_of_affiliation_onlyprimary_IDs:
                chunks = [list_of_affiliation_onlyprimary_IDs[i:i + 100] for i in range(0, len(list_of_affiliation_onlyprimary_IDs), 100)]
                for chunk in chunks:
                    query = (
                        f"(({') OR ('.join([f'affiliationid:{i}' for i in chunk])})) "
                        f"AND isprimary:1 AND NOT correspondence:true"
                    )
                    params = {'q': query, 'fl': filterlist, 'rows': 100, 'wt': 'json'}
                    response = solr_url.search(**params)
                    data.extend([doc for doc in response])
            else:
                print("No affids only for primary org.")
    except Exception as e:
        print(f"Solr query failed: {e}")

    #---- Build DataFrame Safely ----#
    if data and isinstance(data, list) and len(data) > 0:
        solr_data_df = spark.createDataFrame(data)
    else:
        solr_data_df = empty_affiliation_df()
    solr_data_df = solr_data_df.select("affiliationid", "nearestmain", "opm_orgprofileid", "_childDocuments_", col("affil").alias("XML"), col("afttaggedjson").alias("AFT_Tagged_Json"), col("vendortaggedjson").alias("Vendor_Tagged_Json"), col("year").alias("Year"))
    solr_data_affiliation_IDs = [row["affiliationid"] for row in solr_data_df.select("affiliationid").distinct().collect()]
    print(f"SOLR_No_of_AffIds: {len(solr_data_affiliation_IDs)}")

    #---- Extract value for child documents and finalmapped ids ----#
    df_childdocument_finalmapped = (solr_data_df
                                    .withColumn("parsedopm", expr("""filter(transform(_childDocuments_, x -> x['parsedopm_list.orgprofileid']), y -> y is not null)""").cast(ArrayType(LongType())))
                                    .withColumn("parsedopmm", expr("""filter(transform(_childDocuments_, x -> x['parsedopmm_list.orgprofileid']), y -> y is not null)""").cast(ArrayType(LongType())))
                                    .withColumn("incompatibleopmm", expr("""filter(transform(_childDocuments_, x -> x['incompatibleopmm_list.orgprofileid']), y -> y is not null)""").cast(ArrayType(LongType())))
                                    .withColumn("finalmapped", array_union(array(col("nearestmain")), array(col("opm_orgprofileid"))))
                                    .withColumn("finalmapped", array_union(col("finalmapped"), col("parsedopm")))
                                    .withColumn("finalmapped", array_except(col("finalmapped"), col("incompatibleopmm")))
                                    .withColumn("finalmapped", array_union(col("finalmapped"), col("parsedopmm")))
                                    .withColumn("has_fl_primaryorg", when(size(array_intersect(col("parsedopmm"), array(lit(Orgid)))) > 0, True).otherwise(False))
                                    .withColumn("has_map_fl_childorgs", when(size(array_intersect(col("finalmapped"), lit(list_of_childorgs_IDs).cast(ArrayType(LongType())))) > 0, True).otherwise(False))
                                    )
    #---- Exclude forcelinking from PrimaryOrg and Mapped with forcelinking from Childorgs ----#
    df_exclude_fl_primaryorg_and_map_fl_childorgs = (df_childdocument_finalmapped
                                                    .filter(col("has_fl_primaryorg") == False)
                                                    .filter(col("has_map_fl_childorgs") == False)
                                                    )
    efpmfc_list_of_affIds = [row["affiliationid"] for row in df_exclude_fl_primaryorg_and_map_fl_childorgs.select("affiliationid").distinct().collect()]
    print(f"EFPMFC_No_of_AffIds: {len(efpmfc_list_of_affIds)}")

    #---- Remove DC affiliation ids if mapping in childorgs with orgvisibility false ----#
    df_remove_dc_affids_mapping_with_childorgs = df_exclude_fl_primaryorg_and_map_fl_childorgs.select(col("affiliationid").cast(LongType()).alias("AffID"), "XML", "AFT_Tagged_Json", "Vendor_Tagged_Json", "Year")
    df_remove_dc_affids_mapping_with_childorgs = df_remove_dc_affids_mapping_with_childorgs.filter(~(col("AffID").isin(list_of_affiliation_childorgs_IDs)))
    final_list_of_affiliation_IDs = [row["AffID"] for row in df_remove_dc_affids_mapping_with_childorgs.select("AffID").distinct().collect()]
    print(f"Final_No_of_AffIds: {len(final_list_of_affiliation_IDs)}")

    #---- Group final data based on XML, AFT_Tagged_Json, and Vendor_Tagged_Json ----#
    df_finalgroup = df_remove_dc_affids_mapping_with_childorgs.groupBy("XML", "AFT_Tagged_Json", "Vendor_Tagged_Json").agg(collect_set("AffID").alias("AffID"))
    df_finalgroup = df_finalgroup.select(concat_ws("|", "AffID").alias("AffID"), "XML", "AFT_Tagged_Json", "Vendor_Tagged_Json")
    df_finalgroup = df_finalgroup.na.fill("")
    finalgroup_list_of_affiliation_IDs = [row["AffID"] for row in df_finalgroup.select("AffID").distinct().collect()]
    print(f"GROUP_No_of_AffIds: {len(finalgroup_list_of_affiliation_IDs)}")

    #---- Sample data random 3500 ----#
    df_finalgroup_sample = df_finalgroup.orderBy(rand()).limit(3500)
    df_finalgroup_sample = df_finalgroup_sample.withColumn("ID", monotonically_increasing_id()+1)
    df_finalgroup_sample = df_finalgroup_sample.withColumn("UID", lit(UID))
    df_finalgroup_sample = df_finalgroup_sample.withColumn("Orgid", lit(Orgid))
    df_finalgroup_sample = df_finalgroup_sample.withColumn("OrgInfo", lit(OrgInfo))
    df_finalgroup_sample = df_finalgroup_sample.withColumn("Picked_Datetime", lit(Normal_Timestamp))
    df_finalgroup_sample = df_finalgroup_sample.withColumn("Save_Datetime", lit(Unix_Timestamp))
    #---- list of affids after grouping split and convert into array ----#
    list_of_affiliation_sample_IDs = (df_finalgroup_sample
                                      .withColumn("AffID", explode(split(col("AffID"), "\|")))
                                      .withColumn("AffID", col("AffID").cast("bigint"))
                                      )
    list_of_affiliation_sample_IDs = [row["AffID"] for row in list_of_affiliation_sample_IDs.select("AffID").distinct().collect()]
    print(f"SAMPLE_No_of_AffIds: {len(list_of_affiliation_sample_IDs)}")

    #---- Make AI Input ----#
    df_finalgroup_sample_with_ai_input = df_finalgroup_sample.withColumn("AI_Input", to_json(
        struct(
            concat("UID", lit("_"), "Orgid", lit("_"), "ID").alias("custom_id"),
            lit("POST").alias("method"),
            lit("/chat/completions").alias("url"),
            struct(
                lit("gpt-4o-globalBatch-OrgDBExperiments").alias("model"),
                array(
                    struct(
                        lit("system").alias("role"),
                        lit(Prompt_Value).alias("content")
                    ),
                    struct(
                        lit("user").alias("role"),
                        concat(
                            lit("OrgDBInfo: "), col("OrgInfo"),
                            lit("; Raw: {\"affiliations\":\""), col("XML"), lit("\"}"),
                            lit("; AFT: "), col("AFT_Tagged_Json"),
                            lit("; Vendor: "), col("Vendor_Tagged_Json")
                        ).alias("content")
                    )
                ).alias("messages"),
                lit(0.0).cast("float").alias("temperature"),
                lit(0.95).cast("float").alias("top_p"),
                struct(
                    lit("json_object").alias("type")
                    ).alias("response_format")
                ).alias("body")
            )
        )
    )
    json_filename = None
    inputbatch_file = None
    outputbatch_file = None
    if df_finalgroup_sample_with_ai_input.count() > 0:
        df_finalgroup_sample_with_ai_input = df_finalgroup_sample_with_ai_input.select("UID", "ID", "Orgid", "OrgInfo", "AffID", "XML", "AFT_Tagged_Json", "Vendor_Tagged_Json", "AI_Input", "Picked_Datetime", "Save_Datetime")
    
        # ---------------- Write files ---------------- #
        json_filename = f"{UID}_{Orgid}.jsonl"
        with open(json_filename, "w") as f:
            for row in df_finalgroup_sample_with_ai_input.select("AI_Input").collect():
                f.write(f"{row.AI_Input}\n")

        # ---------------- Upload files ---------------- #
        try:
            with open(json_filename, "rb") as f:
                uploaded = client.files.create(
                    file=f,
                    purpose="batch",
                    extra_body={"expires_after": {"seconds": 1209600, "anchor": "created_at"}}
                )
            inputbatch_file = uploaded.id
            print(f"Uploaded file ID: {inputbatch_file}")
        except Exception as e:
            print(f"Error uploading {json_filename}: {e}")

        # ---------------- Submit batch Jobs ---------------- #
        if inputbatch_file:
            try:
                batch_response = client.batches.create(
                    input_file_id=inputbatch_file,
                    endpoint="/chat/completions",
                    completion_window="24h",
                    extra_body={"output_expires_after": {"seconds": 1209600, "anchor": "created_at"}}
                )
                outputbatch_file = batch_response.id
                print(f"Batch job submitted: {batch_response.id}")
            except Exception as e:
                print(f"Error submitting batch job for File ID {inputbatch_file}: {e}")
    else:
        print(f"{UID}_{Orgid}: Data not available on Solr")
    
    # ---------------- Prepare results ---------------- #
    results = {
        "uid": UID,
        "orgid": Orgid,
        "prompt_key": Prompt_Key,
        "picked_datetime": Normal_Timestamp,
        "list_of_childorgs": list_of_childorgs_IDs,
        "no_of_childorgs": len(list_of_childorgs_IDs),
        "dcop_list_of_affIds": list_of_affiliation_onlyprimary_IDs,
        "dcop_no_of_affIds": len(list_of_affiliation_onlyprimary_IDs),
        "dcco_list_of_affIds": list_of_affiliation_childorgs_IDs,
        "dcco_no_of_affIds": len(list_of_affiliation_childorgs_IDs),
        "solr_list_of_affIds": solr_data_affiliation_IDs,
        "solr_no_of_affIds": len(solr_data_affiliation_IDs),
        "efpmfc_list_of_affIds": efpmfc_list_of_affIds,
        "efpmfc_no_of_affIds": len(efpmfc_list_of_affIds),
        "final_list_of_affIds": final_list_of_affiliation_IDs,
        "final_no_of_affIds": len(final_list_of_affiliation_IDs),
        "group_list_of_affIds": finalgroup_list_of_affiliation_IDs,
        "group_no_of_affIds": len(finalgroup_list_of_affiliation_IDs),
        "sample_list_of_affIds": list_of_affiliation_sample_IDs,
        "sample_no_of_affIds": len(list_of_affiliation_sample_IDs),
        "json_filename": json_filename,
        "inputbatch_file": inputbatch_file,
        "outputbatch_file": outputbatch_file,
        "save_datetime": Unix_Timestamp,
        "df_finalgroup_sample_with_ai_input": df_finalgroup_sample_with_ai_input
	}

    # ---------------- Load fulltable for update entry ----------------#
    existing_org_df = spark.read.table("evaluations.precision_agent_list_of_orgs")
    existing_solr_df = spark.read.table("evaluations.precision_agent_solrdata")
    #---- Apply update only the matched rows columns ----#
    check_uid = results['uid']
    check_orgid = results['orgid']
    check_org_request_df = existing_org_df.filter(((col("UID") == check_uid) & (col("Orgid") == check_orgid)))
    if check_org_request_df.count() > 0:
        update_org_request_df = (existing_org_df
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['list_of_childorgs'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['no_of_childorgs'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['solr_list_of_affIds'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['solr_no_of_affIds'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['dcop_list_of_affIds'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 .withColumn("Picked_Datetime", when((col("UID") == check_uid) & (col("Orgid") == check_orgid), lit(results['picked_datetime'])).otherwise(col("Picked_Datetime")))
                                 )
    return results


# COMMAND ----------

# DBTITLE 1,Complete Workflow
# Step-1: Update the master list with data from RMS
masterlist_updated_from_rms()

# Step-2: Update locked org info for the master list entries
locked_orginfo()

# Step-3: 




# COMMAND ----------

# DBTITLE 1,Step-2: Pick Orgs for AI Processing
# Data: List_of_Orgs for Solr Data
get_orgs_for_ai_tasks = spark.read.table("evaluations.precision_agent_list_of_orgs")
get_orgs_for_ai_tasks = (get_orgs_for_ai_tasks
                        .filter(get_orgs_for_ai_tasks["Locked_Date"].isNull())
                        .filter(get_orgs_for_ai_tasks["Picked_Datetime"].isNull())
                        .sort("UID", "Date_Added", "Datetime_Updated_in_RMS", "Orgid")
                        ).filter(col("Orgid")=="60025371")
list_get_orgs_for_ai_tasks = [(row["UID"], row["Orgid"], row["Orgvisibility"], row["OrgInfo"]) for row in get_orgs_for_ai_tasks.collect()]
print(f"Total orgs count for AI tasks: {len(list_get_orgs_for_ai_tasks)}")
print(f"Unique orgs list: {list_get_orgs_for_ai_tasks}")

for UID, Orgid, Orgvisibility, OrgInfo in list_get_orgs_for_ai_tasks:

    #Call function for AI prompt
    Prompt_Key, Prompt_Value = system_prompts_dictionary('prompt1')

    #Call function for get timestamp
    Normal_Timestamp = get_current_ist_timestamp()['normal_timestamp']
    Unix_Timestamp = get_current_ist_timestamp()['unix_timestamp']
    
    # Solr Data:
    final_result = solr_childorgs_dcaffids_aitask(UID, Orgid, Orgvisibility, OrgInfo, Prompt_Key, Prompt_Value, Normal_Timestamp, Unix_Timestamp)
    
    print("------------------")


# COMMAND ----------

# df = spark.read.table("evaluations.ai_list_orgs_with_allorgs_batch")
# print(df.count())
# df = df.filter(df["org_id"].isin('60030674','60029116','60100018','60009914','60015300','60014304','60012370','60069716','60104339','60103681','60110852','60117280','60069895','60087346','60087352','60068746','60068758','60068756'))
# display(df)