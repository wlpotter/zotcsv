import json
import time
import requests
import sys, os

"""
General todos:
- implement date based filtering on what is saved (i.e., after modified dates)
- implement version based filtering (i.e., the ?since parameter, as an option
- maybe: implement cli options or more robust handling so that parameters and other things can be passed to the script
"""
    
"""
FUNCTIONS
"""

"""
This function 
response is the 
"""
def handle_error_retries(response, params, current_retry_attempt: int, max_retries: int):
    # whenever the max number of retries is exceeded, return error info and exit the script - the assumption is the server is down long term or a much longer delay is needed
    if(current_retry_attempt > max_retries):
        print("Max retries exceeded for the following request URL")
        print(response.url)
        print("Final HTTP Status Code and Response headers:")
        print(response.status_code)
        sys.exit(response.headers)
    # for a subset of returns, we have a time delay then retry the request
    if(response.status_code in [429, 500, 502, 503]):
        # set the time to wait before retrying the request, will scale harder based on the response code (429 gets most scaling) or presence of backoff or retry-after header parameters
        time_delay = calculate_time_delay(response.status_code, response.headers, current_retry_attempt)
        print(f"Received a {response.status_code} error, will retry request after {time_delay} second. This is attempt {current_retry_attempt} of {max_retries}")
        retry_response = retry_request(params, time_delay)
        if(retry_response.status_code == 200):
            return retry_response
        else:
            return handle_error_retries(retry_response, params, current_retry_attempt+1, max_retries)
    # for any other error, return some context and throw the code
    else:
        print("Unresolvable HTTP error on the following request URL:")
        print(response.url)
        print("Response Headers:")
        print(response.headers)
        response.raise_for_status()

def calculate_time_delay(status_code: int, headers, retry_attempt: int):
    # set the base delay to the default, but override if the response provided backoff or retry-after headers
    delay = DEFAULT_BACKOFF_DELAY
    if("backoff" in headers):
        delay = int(headers["backoff"])
    elif("retry-after" in headers):
        delay = int(headers["base_delay"])
    
    # scale the delay based on retry attempts, to be extra courteous
    delay *= retry_attempt

    # provide additional scaling for a 429 error
    if(status_code == 429):
        delay *= 2
    return delay

def retry_request(params, time_delay: int):
    time.sleep(time_delay)
    print("Retrying request")
    return session.get(ZOTERO_API_BASE, params=params)

"""
This function checks the item keys of a returned JSON array containing Zotero records
Against a list of expected , based on the start and end window of the API request
"""
def returned_keys_match_expected_sequence(returned_keys: list, all_keys: list, window_start: int, window_end: int):
    return returned_keys == all_keys[window_start:window_end]    

"""
CONSTANTS
"""
ZOTERO_API_BASE = "https://api.zotero.org/groups/4861694/items/top" # Note: using top should ignore notes
START_AT = 0
LIMIT = 10
MAX_LIMIT = 20 # set the max number of records to return, useful for testing purposes and for batching TODO: implement so that it is the length of the number of keys if set to None
SAVE_DIRECTORY = "/home/arren/Documents/GitHub/zotcsv/2025-09-26_dump/"
FILE_NAME_BASE = "zotero_dump2025-09-26_"

MAX_RETRIES = 3
DEFAULT_BACKOFF_DELAY = 15

"""
Configuration and Initialization
"""
# create the directory
os.makedirs(SAVE_DIRECTORY, exist_ok=True)

# This is the main session object which will make the API requests
session = requests.Session()

# set the overall Zotero API version for all requests using a Session header
session_header = {
    "Zotero-API-Version": '3'
}
session.headers.update(session_header)

# initialize the request parameters for the first API call -- start will be updated 
init_req_params = {
    "limit": LIMIT,
    "start": START_AT,
    "format": "json",
    "include": "bib,data,coins,citation",
    "style": "chicago-fullnote-bibliography"
}
# get the full list of keys
print("Getting a list of item keys from the Zotero library for comparison")
keys_response = session.get(ZOTERO_API_BASE, params={"format": "keys"})
all_keys = []
if(keys_response.status_code == 200):
    all_keys = keys_response.text.splitlines()
    print("Item keys successfully retrieved")

"""
Set up and Start Main Loop
"""
i = START_AT
req_params = init_req_params

while i < MAX_LIMIT:
    # try request
    try:
        response = session.get(ZOTERO_API_BASE, params=req_params)
        print(f"Making request for {response.url}")
        response.raise_for_status() # raise an error if not a 200 status code
    # handle http errors
    except requests.HTTPError:
        handle_error_retries(response, req_params, 1, MAX_RETRIES)
        print("Response Code: " + response.status_code)
        print(response.headers)
    else:
        data = response.json()
        returned_keys = [rec["key"] for rec in data] # create a list of item keys to compare
        window_start = req_params["start"]
        window_end = window_start + req_params["limit"]

        if(returned_keys_match_expected_sequence(returned_keys=returned_keys, all_keys=all_keys, window_start=window_start, window_end=window_end)):
            # save the data to a file, named based on the constants and the start/end window
            filepath = SAVE_DIRECTORY + FILE_NAME_BASE + str(window_start) + "-" + str(window_end) + ".json" # construct the filename for this file
            with open(filepath, "w+") as f:
                json.dump(data, f, indent=2)
            print("Data successfully saved to " + filepath)
        else:
            print("Returned sequence of keys doesn't match the expected sequence")
            print("returned keys:")
            print(chunk_keys)
            print("expected keys:")
            print(all_keys[window_start:window_end])

        i += req_params["limit"]
        req_params["start"] += req_params["limit"]
        time.sleep(3)