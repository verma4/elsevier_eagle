import os
import json
import requests

api_url = os.getenv("SCOPUS_ENDPOINT")
api_key = os.getenv("SCOPUS_KEY")

# Scopus Search endpoint
api_url = "https://api.elsevier.com/content/search/scopus"

# Headers with your API key
headers = {
    "Accept": "application/json",
    "X-ELS-APIKey": api_key
}

print(api_key)

# # Params for search
# params = {
#     "query": "heart attack AND text(liver)",  # search query
#     "count": 5,  # how many results you want
#     "sort": "relevancy",  # optional sort
#     "httpAccept": "application/json"  # response format
# }

# # Make request
# response = requests.get(api_url, headers=headers, params=params)

# # Handle different response codes
# if response.status_code == 200:
#     data = response.json()
#     print("✅ Success!")
#     print("Total Results:", data["search-results"]["opensearch:totalResults"])
#     for item in data["search-results"]["entry"]:
#         print(item.get("dc:title"))

# elif response.status_code == 400:
#     print("❌ 400 Bad Request: Invalid query or params.")
#     print(response.text)

# elif response.status_code == 401:
#     print("❌ 401 Unauthorized: Missing/invalid API key or OAuth token.")
#     print(response.text)

# elif response.status_code == 403:
#     print("❌ 403 Forbidden: No entitlements for this resource (check institution or token).")
#     print(response.text)

# elif response.status_code == 405:
#     print("❌ 405 Method Not Allowed: Wrong HTTP method (only GET is supported here).")
#     print(response.text)

# elif response.status_code == 406:
#     print("❌ 406 Not Acceptable: Invalid response format requested.")
#     print(response.text)

# elif response.status_code == 429:
#     print("⚠️ 429 Too Many Requests: Quota limit exceeded. Try again later or request higher quota.")
#     print(response.text)

# elif response.status_code == 500:
#     print("💀 500 Internal Server Error: Something went wrong on Elsevier’s side.")
#     print(response.text)

# else:
#     print(f"🤔 Unexpected Error {response.status_code}:")
#     print(response.text)