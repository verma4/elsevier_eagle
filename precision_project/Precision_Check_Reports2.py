# Databricks notebook source
# MAGIC %md
# MAGIC Input:
# MAGIC  - evaluations.report_collectionmetrics
# MAGIC  - evaluations.ai_list_orgs_with_results_async_mode_2
# MAGIC  - evaluations.ai_list_orgs_with_allorgs_batch
# MAGIC
# MAGIC Output:
# MAGIC   - evaluations.AiPA_RNRY_Numbers
# MAGIC   - evaluations.AiPA_RN_Jsonl_Track
# MAGIC   - evaluations.AiPA_RN_output_datewise
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library
import elsevier_eagle
from elsevier_eagle import *

# COMMAND ----------

# DBTITLE 1,Find OrgID based on end_time & Input OrgID for Particular Date Complete
current_ist_datetime = datetime.now(ist)
current_ist_datetime_min1day = current_ist_datetime.date() - timedelta(days=1)

FILEDATE = current_ist_datetime_min1day
FILEDATE = str(FILEDATE).replace('-', '')
print(f"date for orgid : {FILEDATE}")

df_collections_metrics = spark.read.table("evaluations.report_collectionmetrics")
df_collections_metrics = df_collections_metrics.filter(df_collections_metrics["duration_minutes_total"] > 0)
df_collections_metrics = df_collections_metrics.filter(df_collections_metrics["collection_name"].like('%_fromDataBricks%'))
df_collections_metrics = df_collections_metrics.filter((df_collections_metrics["PID"].isNotNull()) & (df_collections_metrics["PID"] != ""))
df_collections_metrics = df_collections_metrics.select("PID", "Date", "collection_name", "start_time", "end_time")
df_collections_metrics = df_collections_metrics.groupby("PID", "Date", "collection_name").agg(min("start_time").alias("start_time"), max("end_time").alias("end_time"))
df_collections_metrics = df_collections_metrics.withColumn("start_time", array_min("start_time"))
df_collections_metrics = df_collections_metrics.withColumn("end_time", array_max("end_time"))
df_collections_metrics = df_collections_metrics.sort(desc("end_time"))
df_collections_metrics = df_collections_metrics.withColumn("pointer", col("end_time").substr(1, 10))
df_collections_metrics = df_collections_metrics.filter(df_collections_metrics["pointer"].isin(current_ist_datetime_min1day))
df_collections_metrics_list = df_collections_metrics.select("PID", "Date")
list_of_ids = [row["PID"] for row in df_collections_metrics_list.select("PID").distinct().collect()]
print(list_of_ids)
print(f"Total orgs: {len(list_of_ids)}")

df_aioutput_data = spark.read.table("evaluations.ai_list_orgs_with_results_async_mode_2")
df_aioutput_data = df_aioutput_data.select("PID", "AffID", "Raw", "Aft", "Vendor", "Reasoning", col("processed_datetime").alias("Date"))
df_aioutput_data = df_aioutput_data.join(df_collections_metrics_list, on=["PID", "Date"], how="leftsemi")
print(f"Total rows: {df_aioutput_data.count()}")

review_no = df_aioutput_data.filter(((df_aioutput_data["Aft"]=="true") & (df_aioutput_data["Vendor"]=="true")))
review_no_afid = review_no.withColumn("AffID", explode(split("AffID", "\|")))

#select random 3500 affiliaitons.
review_no_afid_sample = review_no_afid.orderBy(rand()).limit(3500)
review_no_afid_sample_PID = [row["PID"] for row in review_no_afid_sample.select("PID").distinct().collect()]
print(f"Selected in random orgs: {len(review_no_afid_sample_PID)}")



# COMMAND ----------

def get_access_token():
    spark = SparkSession.builder.appName("AuthRequestToDF").getOrCreate()
    auth_url = "http://orgtool.mhub.elsevier.com:3030/authentication"

    scope_name = 'OrganizationDomain'
    username = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_username")
    password = dbutils.secrets.get(scope=scope_name, key="collection_api_ai_password")

    payload = {
        "username": username,
        "password": password
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response_data = None
    try:
        response = requests.post(auth_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        print(f"SUCCESS: Authentication request successful (Status: {response.status_code})")
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            print("FAILED: Response received, but it was not valid JSON.")
            print("Response Text:", response.text)
            response_data = None

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
        print(e.response.text)
    except requests.exceptions.RequestException as e:
        print(f"FAILED: An unexpected error occurred during the request.")
        print(f"Error details: {e}")

    auth_df = None
    access_token = None
    if response_data is not None:
        try:
            if isinstance(response_data, dict):
                data_for_df = [response_data]
                auth_df = spark.createDataFrame(data_for_df)

            elif isinstance(response_data, list) and all(isinstance(item, dict) for item in response_data):
                auth_df = spark.createDataFrame(response_data)

            else:
                print(f"Response data type ({type(response_data)}) cannot be directly converted to DataFrame.")

        except Exception as e:
            print(f"Error creating DataFrame from response data: {e}")
            print("Response data that caused error:", response_data)
    else:
        print("\nNo valid JSON response data received, DataFrame not created.")

    if auth_df is not None and 'accessToken' in auth_df.columns:
        try:
            access_token = auth_df.select("accessToken").first()[0]
        except Exception as e:
            print(f"Could not extract access_token from DataFrame: {e}")
    return access_token

def Collection_creation_request(access_token, affiliationid_list, ORGID, TRIGGERDATE):
    url = "http://orgtool.mhub.elsevier.com:3030/collections/affiliations"

    trigger_datetime = datetime.strptime(TRIGGERDATE, "%Y-%m-%d %H:%M:%S")
    trigger_timestamp = int(trigger_datetime.timestamp())

    collection_name = f"{trigger_timestamp}_{ORGID}_RN_fromDataBricks"
    user_name = "AiPA"
    role_name = "USER"

    ids_df = affiliationid_list
    id_column_name = "affiliationid"

    affiliation_ids_from_df = []

    try:
        collected_rows = ids_df.select(id_column_name).distinct().collect()
        affiliation_ids_from_df = [str(row[id_column_name]) for row in collected_rows if row[id_column_name] is not None]
        print(f"Successfully collected {len(affiliation_ids_from_df)}.")

    except Exception as e:
        print(f"ERROR: Failed to collect affiliation IDs from DataFrame. Details: {e}")

    if not affiliation_ids_from_df:
        print("Warning: The list of affiliation IDs collected from the dataframe is empty, collection will not be created.")
        query_string = "() AND isprimary:1 AND NOT correspondence:true"
    else:
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
            "orgid": ORGID
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
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


# COMMAND ----------

for ORGID in review_no_afid_sample_PID:
    access_token = get_access_token()
    review_no_filter = review_no_afid_sample.filter(col("PID") == ORGID)
    affiliationid_list = review_no_filter.select(col("AffID").alias("affiliationid")).distinct()
    TRIGGERDATE = review_no_filter.select("Date").distinct().collect()[0][0]
    collection_name = Collection_creation_request(access_token, affiliationid_list, ORGID, TRIGGERDATE)