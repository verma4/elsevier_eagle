# Databricks notebook source
pip install cassandra-driver

# COMMAND ----------

!pip install openai --upgrade
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library
# MAGIC %run "OrganizationDomain/analytics-eagle/Amar_Nath_Verma/Initialize"

# COMMAND ----------

# DBTITLE 1,Function Solr Data Extraction
# Function to query Solr for Particular OrgID
def solr_data_for_id(orgid, org_metadata):
    schema = StructType([
        StructField("affiliationid", StringType(), True),
        StructField("affil", StringType(), True),
        StructField("afttaggedjson", StringType(), True),
        StructField("vendortaggedjson", StringType(), True),
        StructField("year", StringType(), True)
    ])
    
    base_url = "http://orgtool-solr-prod.mhub.elsevier.com:9001/solr/elsevier.affiliation/query"
    results = []

    query = (
        f"((((nearestmain:{orgid}) OR (opm_orgprofileid:{orgid}) "
        f"OR {{!parent which='contentType:affiliation' v=parsedopm_list.orgprofileid:{orgid}}}) "
        f"AND NOT {{!parent which='contentType:affiliation' v=incompatibleopmm_list.orgprofileid:{orgid}}}) "
        f"OR {{!parent which='contentType:affiliation' v=parsedopmm_list.orgprofileid:{orgid}}}) "
        f"AND isprimary:1 AND NOT correspondence:true"
    )
    params = {'q': query, 'wt': 'json'}
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        num_found = data.get('response', {}).get('numFound', 0)
        if num_found > 0:
            params['rows'] = num_found
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            docs = data.get('response', {}).get('docs', [])
            results.extend(docs)
            df = spark.createDataFrame(results, schema=schema)
            print(f"Number of affiliations found for {orgid}: {df.count()}")
            df = df.select(col("affiliationid").alias("AffID"), col("affil").alias("XML"), col("afttaggedjson").alias("AFT_Tagged_Json"), col("vendortaggedjson").alias("Vendor_Tagged_Json"), col("year").alias("year"))
            df = df.withColumn(
                "OrgInfo",
                to_json(
                    struct(
                        lit(org_metadata["org_id"]).alias("org_id"),
                        lit(org_metadata["org_name"]).alias("org_name"),
                        lit(org_metadata["former_names_array"]).alias("former_name"),
                        lit(org_metadata["departments_array"]).alias("departments"),
                        lit(org_metadata["street"]).alias("street"),
                        lit(org_metadata["city"]).alias("city"),
                        lit(org_metadata["state"]).alias("state"),
                        lit(org_metadata["postal_code"]).alias("postal_code"),
                        lit(org_metadata["country"]).alias("country")
                        )
                    )
                )
            return df
        else:
            print(f"No results found for ID {orgid}.")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for ID {orgid}: {e}")
    except KeyError as e:
        print(f"Unexpected response structure for ID {orgid}: {e}")

# COMMAND ----------

def group_solr_data(df_result):
    df_results_group = df_result.groupBy("XML", "AFT_Tagged_Json", "Vendor_Tagged_Json", "OrgInfo").agg(collect_set("AffID").alias("AffID"))
    df_results_group = df_results_group.select(concat_ws("|", "AffID").alias("AffID"), "XML", "AFT_Tagged_Json", "Vendor_Tagged_Json", "OrgInfo")
    df_results_group = df_results_group.na.fill("")
    df_results_group = df_results_group.withColumn("XML", regexp_replace(col("XML"), '"', '\\\\"'))
    df_results_group = df_results_group.withColumn("AFT_Tagged_Json", regexp_replace(col("AFT_Tagged_Json"), '"', '\\\\"'))
    df_results_group = df_results_group.withColumn("AFT_Tagged_Json", regexp_replace(col("AFT_Tagged_Json"), '\\\\\\\\"', '\\\\"'))
    df_results_group = df_results_group.withColumn("Vendor_Tagged_Json", regexp_replace(col("Vendor_Tagged_Json"), '"', '\\\\"'))
    df_results_group = df_results_group.withColumn("Vendor_Tagged_Json", regexp_replace(col("Vendor_Tagged_Json"), '\\\\\\\\"', '\\\\"'))
    df_results_group = df_results_group.withColumn("OrgInfo", regexp_replace(col("OrgInfo"), '"', '\\\\"'))
    print(f"Number after grouping same xml: {(df_results_group).count()}")
    df_results_group_sample = df_results_group.orderBy(rand()).limit(3500)
    print(f"Number after random select: {(df_results_group_sample).count()}")
    return df_results_group_sample

    


# COMMAND ----------

def get_json_data(df_results_group_sample_3500, ORGID):
    schema_json = StructType([
        StructField("custom_id", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("body", StructType([
            StructField("model", StringType(), True),
            StructField("messages", ArrayType(StructType([
                StructField("role", StringType(), True),
                StructField("content", StringType(), True)
                ])), True),
            StructField("temperature", FloatType(), True),  # Use FloatType for temperature
            StructField("top_p", FloatType(), True),        # Use FloatType for top_p
            StructField("response_format", StructType([      # response_format as a struct
                StructField("type", StringType(), True)      # 'type' is a string
            ]), True)
        ]), True)
    ])

    # Assuming df_results_group is your initial DataFrame
    df_results_group_jsondata = df_results_group_sample_3500.withColumn(
        "jsonData",
        concat(
            lit('{"custom_id": "'), concat(lit(ORGID), lit("_"), col("AffID")), lit('", '),
            lit('"method": "POST", "url": "/chat/completions", "body": {'),
            lit('"model": "gpt-4o-globalBatch-OrgDBExperiments", '),
            lit('"messages": [{"role": "system", "content": '),
            lit('"You are an Organization Matching Validator specialized in preventing wrong matches between affiliations and organization profiles. ## Input: - OrgInfo: Contains canonical organization name and location. It can also include its former_name, departments or sub organizations - AFT_JSON: Machine-tagged affiliation - Vendor_JSON: Human-tagged affiliation ## Core Matching Rules: - If the raw affiliation (assume AFT JSON\'s full raw string as raw) unambiguously represents the same organization in **OrgInfo**, set \\"Raw\\": true - If the JSON tagged by **AFT** can be disambiguated to match the organization and location in **OrgInfo**, set \\"Aft\\": true - If the JSON tagged by **Vendor** can be disambiguated to match the organization and location in **OrgInfo**, set \\"Vendor\\": true ## Matching guidelines to follow: - Only use the provided information while matching. DO NOT assume other locations and/or relationships ever - However, use the provided formername info and consider it a match if formername is mentioned or truly belongs to the main organization in **OrgInfo** - However, use the provided departmental info and consider it a match if department is mentioned or truly belongs to the main organization in **OrgInfo** - Parse and use location information for disambiguation even when incorrectly tagged (e.g., \\"orgs\\":[{\\"text\\":University A, City B}] should be treated same as \\"orgs\\":[{\\"text\\":University A}],cities:[\\"City B\\"]\\") - Ignore academic titles/positions tagged as <orgs> (e.g., \\"Professor\\") while still considering the actual institution name for matching ## Common Error Cases to Check: - Substring matches that could belong to multiple institutions - Similar named institutions in different locations - Basing matches solely on based of location - Generic institution names without distinguishing elements - Often in chinese medical affiliations, **hospital** affiliated to **university**, the match is wrongly determined university. But it should be **hospital** as that is where the author\'s did research and not in the affiliated university. These kind of hospitals are first class citizens in our products. Thus, in such cases, treat affiliated hospitals as independent entities from their parent universities - Not getting matched due to minor typos - Getting confused by multiple institutes. We only care about whether the JSON matches the provided OrgInfo without ambiguity. ## Output Format: { \\"Raw\\": <true|false>, \\"Aft\\": <true|false>, \\"Vendor\\": <true|false>, \\"Reasoning\\": \\"<Clear explanation of match decision>\\" } Do not include any other text, analysis, or markdown formatting in your response. Return only the JSON object."'),
            lit('}, {"role": "user", "content": "OrgDBInfo: '),
            col("OrgInfo"),
            lit('; Raw: {\\"affiliations\\":\\"'),
            col("XML"),
            lit('\\"}; AFT: '),
            col("AFT_Tagged_Json"),
            lit('; Vendor: '),
            col("Vendor_Tagged_Json"),
            lit('"}],"temperature":0,"top_p":0.95,"response_format":{"type":"json_object"}}}')
        ))
    # Parse the constructed JSON string to validate against the schema
    json_df = df_results_group_jsondata.withColumn("parsed_json", from_json(col("jsonData"), schema_json))
    json_df = json_df.select("parsed_json")
    json_df = json_df.select(to_json(col('parsed_json')).alias('json_string'))
    return json_df

# COMMAND ----------

import os
from openai import AzureOpenAI

def upload_files_to_openai(json_df, client, org_id_pair):

    # Step 1: Write to files
    batch_size = 7000
    filename_all = []

    for i in range(0, json_df.count(), batch_size):
        file_name = f"test_{org_id_pair}.jsonl"
        with open(file_name, "w") as f:
            batch_rows = json_df.select("json_string").collect()[i:i + batch_size]
            for item in batch_rows:
                if item.json_string.strip() != "{}":
                    f.write(f"{item.json_string}\n")
        filename_all.append(file_name)

    # Print all written files
    for filename_line in filename_all:
        print(filename_line)

    # Step 2: Upload files with a purpose of "batch"
    file_ids = []  # List to store the IDs of uploaded files

    for file_name in filename_all:
        try:
            with open(file_name, "rb") as f:
                file = client.files.create(
                    file=f,
                    purpose="batch"
                )
            print(f"Uploaded: {file_name} | File ID: {file.id}")
            file_ids.append(file.id)

        except Exception as e:
            print(f"An error occurred while uploading {file_name}: {e}")

    # Print all uploaded file IDs
    for fileid_line in file_ids:
        print(fileid_line)

    return file_ids

# COMMAND ----------

def submit_batch_jobs(file_ids, client):
    batch_ids = []  # List to store responses for each batch job

    for file_id in file_ids:
        try:
            # Submit a batch job with the file
            batch_response = client.batches.create(
                input_file_id=file_id,
                endpoint="/chat/completions",
                completion_window="24h",
            )
            #print(batch_response.model_dump_json(indent=2))
            print(f"input_file_id: {batch_response.input_file_id} | id: {batch_response.id}")
            batch_ids.append(batch_response.id)

        except Exception as e:
            print(f"An error occurred while submitting batch job for File ID {file_id}: {e}")

    # Optionally, print all batch responses
    for batchid_line in batch_ids:
        print(batchid_line)

    return batch_ids[0]


# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType # May be needed if schema inference fails

def get_access_token():
    # Ensure SparkSession is available (usually 'spark' in Databricks notebooks)
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

    # Recommended way using Databricks Secrets:
    # try:
    #     username = dbutils.secrets.get(scope="your_credential_scope", key="dp_orgtool_username")
    #     password = dbutils.secrets.get(scope="your_credential_scope", key="dp_orgtool_password")
    # except Exception as e:
    #     print(f"Error retrieving secrets: {e}. Please ensure scope and keys are correct.")
    #     # Handle error appropriately - perhaps raise it to stop execution
    #     raise e # Stop if secrets can't be loaded

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

    return access_token

# COMMAND ----------

# import requests
# import json
# from pyspark.sql.functions import col # Make sure col is imported if needed elsewhere

# def Collection_creation_request(access_token, affiliationid_list, ORGID, TRIGGERDATE):
#     # --- Configuration ---
#     # The target URL
#     url = "http://orgtool.mhub.elsevier.com:3030/affiliations/collection"

#     #convert TRIGGEREDATE TO UNIX timestamp format
#     trigger_datetime = datetime.datetime.strptime(TRIGGERDATE, "%Y-%m-%d %H:%M:%S")
#     trigger_timestamp = int(trigger_datetime.timestamp())

#     # Other details for the payload (adjust if necessary)
#     collection_name = f"{trigger_timestamp}_{ORGID}_DF_fromDataBricks" # Example name, adjust as needed
#     user_name = "AiPA" # Make dynamic or get from context if needed
#     role_name = "USER"  # Ensure this is appropriate

#     # --- Get Affiliation IDs from DataFrame ---

#     # <<<<<<<<<<<< MODIFY THIS SECTION >>>>>>>>>>>>>>>>
#     # Replace 'your_dataframe' with the actual name of your PySpark DataFrame
#     # Replace 'your_id_column' with the actual name of the column containing affiliation IDs
#     ids_df = affiliationid_list
#     id_column_name = "affiliationid"
#     # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

#     affiliation_ids_from_df = [] # Initialize empty list

#     #print(f"Attempting to collect distinct IDs from DataFrame column '{id_column_name}'...")
#     try:
#         # 1. Select the specific column
#         # 2. Get distinct values
#         # 3. Collect the results to the Driver node as a list of Row objects
#         # WARNING: .collect() brings all distinct IDs to the driver's memory.
#         # This can cause OutOfMemoryErrors if the number of unique IDs is extremely large (e.g., millions).
#         collected_rows = ids_df.select(id_column_name).distinct().collect()

#         # 4. Extract the ID value from each Row and convert to string
#         affiliation_ids_from_df = [str(row[id_column_name]) for row in collected_rows if row[id_column_name] is not None]

#         print(f"Successfully collected {len(affiliation_ids_from_df)} unique IDs for {ORGID}.")
#         # print(f"Sample IDs: {affiliation_ids_from_df[:10]}") # Optional: print first 10 IDs

#     except Exception as e:
#         print(f"ERROR: Failed to collect affiliation IDs from DataFrame. Details: {e}")
#         # Depending on your requirements, you might want to stop execution here
#         # or proceed with an empty list (which the next block handles)

#     # --- Build the Dynamic Query String (using IDs from DataFrame) ---
#     if not affiliation_ids_from_df:
#         print("Warning: The list of affiliation IDs collected from the dataframe is empty, collection will not be created.")
#         # Handle empty list: Maybe skip API call or construct a query differently?
#         # For this example, we'll proceed which will likely result in query "() AND ..."
#         query_string = "() AND isprimary:1 AND NOT correspondence:true" # Or handle error upstream
#     else:
#         #print(f"collection_name: {collection_name}")
#         # Format each ID into '(affiliationid:ID)'
#         formatted_ids = [f"(affiliationid:{id_val})" for id_val in affiliation_ids_from_df]

#         # Join them with ' OR '
#         or_conditions = " OR ".join(formatted_ids)

#         # Construct the full query string
#         query_string = f"(({or_conditions})) AND isprimary:1 AND NOT correspondence:true"

#         # --- Build the JSON Payload ---
#         payload = {
#             "query": query_string,
#             "collection": collection_name,
#             "frozen": False,
#             "selectedSearchCore": "ANI",
#             "d2mType": "",
#             "project": "GoldProfileMaintenance",
#             "curationType": ["Testing"],
#             "user": user_name,
#             "role": role_name
#         }

#         # --- Define Headers ---
#         headers = {
#             'Content-Type': 'application/json',
#             'Accept': 'application/json',
#             'Authorization': f'Bearer {access_token}'
#         }

#         # --- Make the POST Request ---
#         #print(f"\nMaking POST request to: {url}")
#         # print("Payload being sent:") # Optional: Print payload if needed for debugging
#         # print(json.dumps(payload, indent=2))

#         try:
#             response = requests.post(url, headers=headers, json=payload, timeout=60) # Increased timeout slightly

#             response.raise_for_status()

#             #print(f"\nSUCCESS: Request successful!")
#             print(f"Status Code: {response.status_code}")
#             return collection_name
#             try:
#                 response_json = response.json()
#                 # print("Response Body (JSON):") # Optional: Print response if needed
#                 # print(json.dumps(response_json, indent=2))
#             except json.JSONDecodeError:
#                 print("Response Body (Non-JSON):")
#                 print(response.text)

#         # --- Error Handling ---
#         except requests.exceptions.Timeout:
#             print(f"\nFAILED: Request timed out after 60 seconds.")
#         except requests.exceptions.ConnectionError as e:
#             print(f"\nFAILED: Could not connect to the server. Check URL and network connectivity.")
#             print(f"Error details: {e}")
#         except requests.exceptions.HTTPError as e:
#             print(f"\nFAILED: HTTP Error occurred.")
#             print(f"Status Code: {e.response.status_code}")
#             print("Response Body:")
#             print(e.response.text)
#         except requests.exceptions.RequestException as e:
#             print(f"\nFAILED: An unexpected error occurred during the request.")
#             print(f"Error details: {e}")

#     # End of block that runs only if IDs were collected
#     # Add any further processing based on the API response here

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import col # Make sure col is imported if needed elsewhere

def Collection_creation_request(access_token, affiliationid_list, ORGID, TRIGGERDATE):
    # --- Configuration ---
    # The target URL
    url = "http://orgtool.mhub.elsevier.com:3030/collections/affiliations"

    #convert TRIGGEREDATE TO UNIX timestamp format
    trigger_datetime = datetime.datetime.strptime(TRIGGERDATE, "%Y-%m-%d %H:%M:%S")
    trigger_timestamp = int(trigger_datetime.timestamp())

    # Other details for the payload (adjust if necessary)
    collection_name = f"{trigger_timestamp}_{ORGID}_RY_fromDataBricks" # Example name, adjust as needed
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

    #print(f"Attempting to collect distinct IDs from DataFrame column '{id_column_name}'...")
    try:
        # 1. Select the specific column
        # 2. Get distinct values
        # 3. Collect the results to the Driver node as a list of Row objects
        # WARNING: .collect() brings all distinct IDs to the driver's memory.
        # This can cause OutOfMemoryErrors if the number of unique IDs is extremely large (e.g., millions).
        collected_rows = ids_df.select(id_column_name).distinct().collect()

        # 4. Extract the ID value from each Row and convert to string
        affiliation_ids_from_df = [str(row[id_column_name]) for row in collected_rows if row[id_column_name] is not None]

        print(f"Successfully collected {len(affiliation_ids_from_df)} unique IDs for {ORGID}.")
        print(f"Sample IDs: {affiliation_ids_from_df}") # Optional: print first 10 IDs

    except Exception as e:
        print(f"ERROR: Failed to collect affiliation IDs from DataFrame. Details: {e}")
        # Depending on your requirements, you might want to stop execution here
        # or proceed with an empty list (which the next block handles)

    # --- Build the Dynamic Query String (using IDs from DataFrame) ---
    if not affiliation_ids_from_df:
        print("Warning: The list of affiliation IDs collected from the dataframe is empty, collection will not be created.")
        # Handle empty list: Maybe skip API call or construct a query differently?
        # For this example, we'll proceed which will likely result in query "() AND ..."
        query_string = "() AND isprimary:1 AND NOT correspondence:true" # Or handle error upstream
    else:
        #print(f"collection_name: {collection_name}")
        # Format each ID into '(affiliationid:ID)'
        # formatted_ids = [f"(affiliationid:{id_val})" for id_val in affiliation_ids_from_df]
        # # Join them with ' OR '
        # or_conditions = " OR ".join(formatted_ids)

        # # Construct the full query string
        # query_string = f"(({or_conditions})) AND isprimary:1 AND NOT correspondence:true"

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
            "orgid": ORGID
        }
        
        # --- Define Headers ---
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # --- Make the POST Request ---
        #print(f"\nMaking POST request to: {url}")
        # print("Payload being sent:") # Optional: Print payload if needed for debugging
        # print(json.dumps(payload, indent=2))

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60) # Increased timeout slightly

            response.raise_for_status()

            #print(f"\nSUCCESS: Request successful!")
            print(f"Status Code: {response.status_code}")
            return collection_name
            try:
                response_json = response.json()
                # print("Response Body (JSON):") # Optional: Print response if needed
                # print(json.dumps(response_json, indent=2))
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

    # End of block that runs only if IDs were collected
    # Add any further processing based on the API response here

# COMMAND ----------

import time

def check_batch_status(batch_id, client):
    try:
        status_response = client.batches.retrieve(batch_id)
        return status_response.status
    except Exception as e:
        print(f"An error occurred while checking status for Batch ID {batch_id}: {e}")
        return None

def get_output_file_id(batch_id, client):
    try:
        batch_response = client.batches.retrieve(batch_id)
        output_file_id = batch_response.output_file_id
        print(output_file_id)
        if output_file_id:
            return output_file_id
        else:
            return batch_response.error_file_id
        
    except Exception as e:
        print(f"An error occurred while retrieving output file ID for Batch ID {batch_id}: {e}")
        return None

def batch_monitor_and_process_responses(batch_ids, client):
    # Monitor and process responses for each batch job
    for batch_id in batch_ids:
        if batch_id:
            while True:
                status = check_batch_status(batch_id, client)
                if status == "completed":
                    output_file_id = get_output_file_id(batch_id, client)
                    if output_file_id:
                        process_response(output_file_id, batch_id)
                    break
                elif status == "failed":
                    print(f"Batch job {batch_id} failed.")
                    break
                else:
                    print(f"Batch job {batch_id} is still processing. Checking again in 60 seconds.")
                    time.sleep(60)

# COMMAND ----------

from typing import List, Dict, Any
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, to_json, struct, lit
import requests
import json
import time
import datetime

def process_response(output_file_id, batch_id):
    output_schema = StructType([
        StructField("PID", StringType(), True),
        StructField("AffID", StringType(), True),
        StructField("Raw", StringType(), True),
        StructField("Aft", StringType(), True),
        StructField("Vendor", StringType(), True),
        StructField("Reasoning", StringType(), True),
        StructField("Batch_Id", StringType(), True),
        StructField("processed_datetime", StringType(), True)
    ])

    processed_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processed_records = []
    ORGIDS = set()
    if output_file_id:
        file_response = client.files.content(output_file_id)
        raw_responses = file_response.text.strip().split('\n')

        response_count = len(raw_responses)
        print(f"Total responses in output file: {response_count}")
        for raw_response in raw_responses:
            json_response = json.loads(raw_response)
            custom_id = json_response["custom_id"]
            ORGID, Affiliaion_id = custom_id.split('_', 1)
            ORGIDS.add(ORGID)
            choices = json_response["response"]["body"]["choices"]
            if choices:
                content_str = choices[0]["message"]["content"]
                content_json = None
                try:
                    content_json = json.loads(content_str)
                except json.JSONDecodeError as e:
                    print(f"An error occurred while decoding JSON for response: {e}")
                    continue
                raw_value = content_json.get("Raw")
                aft_value = content_json.get("Aft")
                vendor_value = content_json.get("Vendor")
                Reasoning_value = content_json.get("Reasoning")
                processed_records.append({
                    "PID": ORGID,
                    "AffID": Affiliaion_id,
                    "Raw": raw_value,
                    "Aft": aft_value,
                    "Vendor": vendor_value,
                    "Reasoning": Reasoning_value,
                    "Batch_Id": batch_id,
                    "processed_datetime": processed_datetime
                })

    df_batch = spark.createDataFrame(processed_records, schema=output_schema)
    print(f"count: {df_batch.count()} ORGIDS : {ORGIDS} BatchID : {batch_id}")
    df_batch.write.mode("append").saveAsTable("evaluations.ai_list_orgs_with_results_async_mode_2")
    access_token = get_access_token()
    for orgid in ORGIDS:
        Precision_check(orgid, df_batch, processed_datetime, access_token)

# COMMAND ----------

def Precision_check(ORGID, df_batch, processed_datetime, access_token):
    df_batch = df_batch.select("PID", "AffID", "Raw", "Aft", "Vendor", "Reasoning")
    df_batch = df_batch.filter(df_batch["PID"].isin(ORGID))
    print(f"ORGID: {ORGID}")
    print(f"df_batch.count(): {df_batch.count()}")
    review_yes = df_batch.filter(~((df_batch["Aft"]=="true") & (df_batch["Vendor"]=="true")))
    review_no = df_batch.filter(((df_batch["Aft"]=="true") & (df_batch["Vendor"]=="true")))

    # Numbers
    OA = df_batch.count()
    RY = review_yes.count()
    RN = review_no.count()
    MP = (RN / OA) * 100

    # Collect Affiliations
    affiliationid_list = review_yes.select(col("AffID").alias("affiliationid"))
    affiliationid_list = affiliationid_list.withColumn("affiliationid", explode_outer(split("affiliationid", "\|"))).distinct()
    collection_name = Collection_creation_request(access_token, affiliationid_list, ORGID, processed_datetime)
    affiliationid_list = affiliationid_list.withColumn("ORGID", lit(ORGID))
    affiliationid_list = affiliationid_list.withColumn("processed_datetime", lit(processed_datetime))
    affiliationid_list.write.mode("append").saveAsTable("evaluations.affiliation_list_async_mode_2")
   
    # Dataframe for orgid information
    df_information = spark.createDataFrame([{
        "PID": ORGID,
        "OA": OA,
        "RY": RY,
        "RN": RN,
        "MP": MP,
        "processed_datetime": processed_datetime
    }], schema="PID STRING, OA INT, RY INT, RN INT, MP DOUBLE, processed_datetime STRING")
    df_information.write.mode("append").saveAsTable("evaluations.ai_list_orgs_with_result_async_mode_final_2")
    
    update_all_orgs_table(ORGID, df_information, collection_name)

# COMMAND ----------

def org_data_for_id(orgId):
    org_data = spark.sql(f"SELECT * FROM evaluations.ai_list_orgs_with_allorgs_batch WHERE org_id = '{orgId}'").collect()[0]

    current_time = datetime.datetime.now()
    trigger_date = current_time.strftime("%Y%m%d_%H%M%S")
    org_id = org_data["org_id"]
    org_name = org_data["org_name"]
    street = org_data["street"]
    city = org_data["city"]
    state = org_data["state"]
    postal_code = org_data["postal_code"]
    country = org_data["country"]
    former_names = org_data["former_name"]
    former_names_array = array([lit(name) for name in former_names])
    departments = org_data["departments"]
    departments_array = array([lit(name) for name in departments])

    return {
        "current_time": current_time,
        "trigger_date": trigger_date,
        "org_id": org_id,
        "org_name": org_name,
        "street": street,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "country": country,
        "former_names_array": former_names_array,
        "departments_array": departments_array
    }

# COMMAND ----------

def update_all_orgs_table(ORGID, df_final_result, Collection_Name):
    if df_final_result.count() > 0 and df_final_result.collect()[0]["MP"] >= 99:
        spark.sql(f"""
            UPDATE evaluations.ai_list_orgs_with_allorgs_batch
            SET ai_OK = 'true', collection_review_pending = 'true', collection_name = '{Collection_Name}',
                count = COALESCE(count, 0) + 1
            WHERE org_id = '{ORGID}'
        """)
    elif df_final_result.count() > 0 and df_final_result.collect()[0]["MP"] < 99:
        spark.sql(f"""
            UPDATE evaluations.ai_list_orgs_with_allorgs_batch
            SET ai_OK = 'false', collection_review_pending = 'true', collection_name = '{Collection_Name}',
                count = COALESCE(count, 0) + 1
            WHERE org_id = '{ORGID}'
        """)

# COMMAND ----------

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

def collection_table_cassandra():
    cluster = Cluster(['orgtool-prod-cassandra-2.mhub.elsevier.com'], port=9042, execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())}, protocol_version=4)
    session = cluster.connect()

    try:
        session.set_keyspace('orgdb')
        query = "SELECT name,approved,approvedstatus,approvedtime FROM collections ALLOW FILTERING"
        rows = session.execute(query)
    #    data = [row._asdict() for row in rows]   #use this if you want to access the column names in sorted order but note we need to change variable value in below line

        if rows:
            df = spark.createDataFrame(rows)
        else:
            print("No data returned from the query.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cluster.shutdown()


    df.createOrReplaceTempView("collections")


def is_count_more_than_100():

    spark.sql("""
        create or replace temporary view QA_approved_collections_filtered as
        select * from collections
        where approved='true'
    """)
    
    result = spark.sql("""
        select count(*) as cnt from evaluations.ai_list_orgs_with_allorgs_batch
        where collection_name in (select name from QA_approved_collections_filtered)
    """).collect()[0]['cnt']
    
    return result > 100

def update_collection_review_pending():
    spark.sql("""
        create or replace temporary view fully_approved_collections_filtered as
        select * from
        (
        select
          *,
          ((date_diff(day,(to_timestamp(approvedtime)),now()))) as diff_in_days
        from collections
        where approvedstatus=2
        )
        where diff_in_days>=12
        order by diff_in_days
    """)

    spark.sql("""
        update evaluations.ai_list_orgs_with_allorgs_batch
        set collection_review_pending = "false"
        where collection_name in (select name from fully_approved_collections_filtered);

    """)

def update_locked_by_tmt():
    max_report_date = spark.sql("""
        SELECT max(ReportDateTime) as max_date
        FROM evaluations.orgs_dayslocked
    """).collect()[0]["max_date"]

    org_ids = spark.sql(f"""
        SELECT OrgId
        FROM evaluations.orgs_dayslocked
        WHERE ReportDateTime = '{max_report_date}' AND squad = 'Garuda'
    """)

    org_ids.createOrReplaceTempView("org_ids_view")

    spark.sql("""
        MERGE INTO evaluations.ai_list_orgs_with_allorgs_batch AS target
        USING org_ids_view AS source
        ON target.org_id = source.OrgId
        WHEN MATCHED THEN
        UPDATE SET target.locked_by_tmt = 'true'
    """)


# COMMAND ----------

# DBTITLE 1,Extract Data through Function and group same details
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from pyspark.sql.functions import col, when
import random

scope_name = 'OrganizationDomain'
openai_api_key = dbutils.secrets.get(scope = scope_name, key = "openai_api_key")
openai_api_url = dbutils.secrets.get(scope = scope_name, key = "openai_api_url")
# Initialize the Azure OpenAI client
client = AzureOpenAI(
    api_key=openai_api_key,
    api_version="2024-08-01-preview",
    azure_endpoint=openai_api_url
)

write_lock = Lock()

def process_org_id_pair(org_id_pair):
    json_dfs = []
    for ORGID in org_id_pair:
        org_metadata = org_data_for_id(ORGID)
        df_result = solr_data_for_id(ORGID, org_metadata)
        if df_result is not None:
            df_result_group = group_solr_data(df_result)
            json_df = get_json_data(df_result_group, ORGID)
            json_dfs.append(json_df)
        # Update ai_list_orgs_with_allorgs
        with write_lock:
            IDS_ALLLIST = spark.read.table("evaluations.ai_list_orgs_with_allorgs_batch")
            IDS_ALLLIST = IDS_ALLLIST.withColumn("ai_picked", when(col("org_id") == ORGID, "yes").otherwise(col("ai_picked")))
            IDS_ALLLIST.write.mode("overwrite").saveAsTable("evaluations.ai_list_orgs_with_allorgs_batch")
    if json_dfs:
        json_df_combined = json_dfs[0] if len(json_dfs) == 1 else json_dfs[0].union(json_dfs[1])
        print(f"json_df_combined.count(): {json_df_combined.count()} for org_id_pair: {org_id_pair}")
        file_ids = upload_files_to_openai(json_df_combined, client, org_id_pair)
        batch_id = submit_batch_jobs(file_ids, client)
        return batch_id
    else:
        return None

    
while True:
    collection_table_cassandra()
    if is_count_more_than_100():
        print("count more than 100")
        break
    update_collection_review_pending()
    update_locked_by_tmt()

    Master_IDS = spark.read.table("evaluations.ai_list_orgs_with_allorgs_batch").filter("locked_by_tmt != 'true'")
    print(f"Master_IDS count: {Master_IDS.count()}")
    Picked_IDS = Master_IDS
    if Picked_IDS.filter(col("ai_picked") == '').count() == 0:
        break
    IDS = Master_IDS.filter(((Master_IDS["ai_OK"]=="false") & (Master_IDS["collection_review_pending"]=="false")))
    print(f"primary_IDS count: {IDS.count()}")

    remaining_IDS = Master_IDS.subtract(IDS)
    remaining_count = 30 - IDS.count()
    random_remaining_IDS = remaining_IDS.filter(col("ai_picked") == '').orderBy(rand()).limit(remaining_count)
    IDS = IDS.union(random_remaining_IDS)

    if IDS.count() == 0:
        break

    org_ids = [row["org_id"] for row in IDS.collect()]
    print(f"ORGIDS count: {len(org_ids)}")

    batch_ids = []

    # max_workers and org_ids size acts a regulator
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for i in range(0, len(org_ids), 2):
            org_id_pair = org_ids[i:i+2]
            print(f"org_id_pair started processing: {org_id_pair}")
            futures.append(executor.submit(process_org_id_pair, org_id_pair))

        for future in futures:
            batch_ids.append(future.result())

    print(f"Batch IDs Count: {len(batch_ids)}")
    print(f"Batch IDs: {batch_ids}")
    batch_monitor_and_process_responses(batch_ids, client)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM evaluations.ai_list_orgs_with_allorgs_batch 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM evaluations.ai_list_orgs_with_result_async_mode_final_2 ORDER BY PID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM evaluations.ai_list_orgs_with_results_async_mode_2
# MAGIC --where pid=60280464

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM evaluations.affiliation_list_async_mode_2 

# COMMAND ----------

# DBTITLE 1,Install PySolr and PySpark Libraries
!pip install pysolr
!pip install pyspark
!pip install cassandra-driver
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,reset script
# %sql
# UPDATE evaluations.ai_list_orgs_with_allorgs_batch SET ai_picked='', locked_by_tmt = '', collection_review_pending = NULL, collection_name = NULL, ai_OK = NULL, count = NULL ;

# TRUNCATE TABLE evaluations.ai_list_orgs_with_result_async_mode_final_2;

# TRUNCATE TABLE evaluations.ai_list_orgs_with_results_async_mode_2;

# TRUNCATE TABLE evaluations.affiliation_list_async_mode_2;

# COMMAND ----------

# DBTITLE 1,Reset for specific orgids
# %sql
# UPDATE evaluations.ai_list_orgs_with_allorgs_batch SET ai_picked='', locked_by_tmt = '', collection_review_pending = NULL, collection_name = NULL, ai_OK = NULL, count = NULL WHERE org_id = ;

# DELETE FROM evaluations.ai_list_orgs_with_result_async_mode_final_2 where PID ='60000590' , 'orgid2', 'orgid3' ;

# DELETE FROM evaluations.ai_list_orgs_with_results_async_mode_2 WHERE PID ='60000590' , 'orgid2', 'orgid3' ;

# DELETE FROM evaluations.affiliation_list_async_mode_2 WHERE ORGID ='60000590' , 'orgid2', 'orgid3' ;