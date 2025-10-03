# Databricks notebook source
# MAGIC %md
# MAGIC Input:
# MAGIC  - evaluations.ai_list_orgs_with_results_async_mode_2
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library
import elsevier_eagle
from elsevier_eagle import *

# COMMAND ----------

# DBTITLE 1,OrgIDs
org_id = '60014217'

# COMMAND ----------

# DBTITLE 1,Read Data
df_result_async_collection = spark.read.table("evaluations.ai_list_orgs_with_results_async_mode_2")
df_result_async_collection = df_result_async_collection.filter(df_result_async_collection["PID"].isin(org_id))
df_result_async_collection = df_result_async_collection.withColumn("Result1", when(((col("Aft")=="true") & (col("Vendor")=="true")), "RN").otherwise("RY"))
df_result_async_collection = df_result_async_collection.withColumn("AffID", explode(split("AffID", "\|")))
df_result_async_collection = df_result_async_collection.filter(df_result_async_collection["Result1"]=="RY")
print(f"total affiliations for review : {df_result_async_collection.select('AffID').distinct().count()}")

df_result_async_collection = df_result_async_collection.withColumn(
    "Result2",
    when(
        (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "true"),
        "RN - NoProblem"
    ).when(
        (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "true"),
        "RN - NoProblem"
    ).when(
        (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "true"),
        "RY - Problem"
    ).when(
        (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "false"),
        "RY - Problem"
    ).when(
        (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "false"),
        "RY - Problem"
    ).when(
        (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "false"),
        "RY - Problem"
    ).when(
        (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "false"),
        "2outof3"
    ).when(
        (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "true"),
        "2outof3"
    )
)

display(df_result_async_collection)


# COMMAND ----------

df_collection = spark.read.table("evaluations.AiPA_collectionwise_data")
df_collection_hasPrecisionIssue = df_collection.select("collection", lit("HasPrecisionIssue").alias("Issue"), col("HasPrecisionIssue_affiliationid").alias("affiliationid"))
df_collection_noPrecisionIssue = df_collection.select("collection", lit("NoPrecisionIssue").alias("Issue"), col("NoPrecisionIssue_affiliationid").alias("affiliationid"))
df_collection_master = df_collection_hasPrecisionIssue.union(df_collection_noPrecisionIssue)
df_collection_master = df_collection_master.withColumn("affiliationid", explode(col("affiliationid"))).distinct()
df_collection_master = df_collection_master.withColumn("orgid", regexp_extract("collection", r"\b(\d{8})\b", 1))
display(df_collection_master)


# COMMAND ----------

# DBTITLE 1,functions
#----------------------------------------------- OrgTool Collection Creation2 -----------------------------------------
def collection_creation_request(affiliationid_list, part_of_data):
    spark = SparkSession.getActiveSession()
    auth_url = "http://orgtool.mhub.elsevier.com:3030/authentication"
    scope_name = 'OrganizationDomain'
    username = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_username")
    password = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_password")

    utc_now = datetime.utcnow()
    india_tz = pytz.timezone('Asia/Kolkata')
    ist_now = utc_now.replace(tzinfo=pytz.utc).astimezone(india_tz).strftime("%Y%m%d_%H%M%S")

    # --- Build Payload and Headers ---
    payload = { "username": username, "password": password }
    headers = { 'Content-Type': 'application/json', 'Accept': 'application/json' }
    
    access_token = None
    try:
        response = requests.post(auth_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        print(f"SUCCESS: Authentication (Status: {response.status_code})")
        response_data = response.json()
        access_token = response_data.get("accessToken")
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

    # --- Prepare affiliation IDs from DataFrame ---
    id_column_name = "affiliationid"
    try:
        collected_rows = affiliationid_list.select(id_column_name).distinct().collect()
        affiliation_ids_from_df = [str(row[id_column_name]) for row in collected_rows if row[id_column_name] is not None]
        print(f"Collected {len(affiliation_ids_from_df)} affiliation IDs.")
    except Exception as e:
        print(f"ERROR collecting IDs: {e}")
        return None

    # --- Build collection payload ---
    collection_name = f"{ist_now}_{part_of_data}_fromDataBricks"   ################## Name of Collections ##################
    url = "http://orgtool.mhub.elsevier.com:3030/collections/affiliations"

    payload = {
        "collectionName": collection_name,
        "user": "AiPA",
        "role": "USER",
        "source": "Elsevier",
        "selectedSearchCore": "ANI",
        "affiliations": {
            "grouped": [],
            "affiliationIds": affiliation_ids_from_df
        },
        "d2mType": "",
        "isDebug": False,
        "project": "GoldProfileMaintenance",
        "curationType": ["Precision"],
    }

    headers['Authorization'] = f"Bearer {access_token}"

    # --- Call collection creation API ---
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        print(f"Collection created successfully: {collection_name}")
        return collection_name
    except Exception as e:
        print(f"Collection creation failed: {e}")
        return None


# COMMAND ----------

# DBTITLE 1,Data for Collection Creation ~ test2outof3
test2outof3 = df_result_async_collection.filter(df_result_async_collection["Result2"]=="test2outof3")
print(f"test2outof3: {test2outof3.select('AffID').distinct().count()}")
test2outof3_afid = test2outof3.select(col("AffID").alias("affiliationid"))
test2outof3_collection = org_id+'_test2outof3'
collection_creation_request(test2outof3_afid, test2outof3_collection)


# COMMAND ----------

# DBTITLE 1,Data for Collection Creation ~ test1outof3
test1outof3 = df_result_async_collection.filter(df_result_async_collection["Result2"]=="test1outof3")
print(f"test1outof3: {test1outof3.select('AffID').distinct().count()}")
test1outof3_afid = test1outof3.select(col("AffID").alias("affiliationid"))
test1outof3_collection = org_id+'_test1outof3'
collection_creation_request(test1outof3_afid, test1outof3_collection)