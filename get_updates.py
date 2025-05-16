import json
import time
import sys, os
from pyzotero import zotero

"""
This function takes a requesting function and attempts to retry it
The request function should be a pyzotero.zotero call
Used to handle retries when an HTTP exception was raised
"""
def handle_retries(request_parameters: dict, initial_response_headers, times_to_retry: int):
    response_headers = initial_response_headers # this will be overwritten as retries are made
    print(f"Request failed with the following parameters: {request_parameters}. Attempting to retry request")
    for i in range(1, times_to_retry+1):
        print(f"Retrying request, attempt {i}")
        response = retry_request(request_parameters, response_headers, i)
        # if the retry returned a list, it was successful; otherwise try again...
        if isinstance(response, list):
            return response
        else:
            response_headers = response
    # if after all of the retry attempts an error still occurred, exit
    print(f"Retry attempts exceeded. Logging request parameters: {request_parameters}.")
    print(f"Saving data to {SAVE_FILE}")
    with open(SAVE_FILE, "w+") as f:
        json.dump(updated_records, f, indent=2)
    print(zot_library.request)
    sys.exit(zot_library.request.headers)


"""
The actual logic of retrying the request, attempting to handle rate limiting
Returns a boolean
"""
def retry_request(request_parameters, response_headers, back_off_delay_scaler: int):
    # set the back off time delay, either following the HTTP response or using the default
    back_off = DEFAULT_BACKOFF_DELAY
    if "Backoff" in response_headers:
        back_off = int(response_headers["Backoff"])
    elif "Retry-After" in response_headers:
        back_off = int(response_headers["Retry-After"])
    back_off *= back_off_delay_scaler # scale that time delay to be extra courteous
    # wait the requested or default back off time before retrying
    print(f"Waiting {back_off} seconds to retry request")
    time.sleep(back_off)

    print("Retrying request")
    try:
        return zot_library.items(**request_parameters) #if successful, return the output of the function
    except zotero.ze.HTTPError:
        return zot_library.request.headers # return the response headers, which will be used in the next retry



DATE_OF_LAST_DUMP = "2024-01-19"
SAVE_FILE = "dump.json" # Path to the file where this data will be saved
OVERWRITE_SAVE = False # if true, deletes the contents of the save file; otherwise, appends returned data
LIMIT = 50 # change this limit
START_AT = 0 # use if you need to start at a later point, useful if a failure requires a retry
DEFAULT_SLEEP_TIME = 2 # set the number of seconds to wait between API requests
DEFAULT_BACKOFF_DELAY = 15 #set the number of seconds to wait if an HTTP error occurs that does not include a back off or retry after amount
config_file = open("config.json")
config_info = json.load(config_file)

# set Zotero library parameters from config file
zotero_library_type = config_info['zotero']['library_type']
zotero_library_id = config_info['zotero']['library_id']
zotero_api_key = config_info['zotero']['api_key']

zot_library = zotero.Zotero(zotero_library_id, zotero_library_type, zotero_api_key)

updated_records = []
# if we are not overwriting the save, and the file exists, prepare updated records with its contents
if not(OVERWRITE_SAVE) and os.path.exists(SAVE_FILE):
    with open(SAVE_FILE, "r") as f:
        updated_records = json.load(f)

# declare initial request parameters
request_parameters = {
    "start": START_AT,
    "limit": LIMIT,
    "format": "json",
    "include": "bib,data,coins,citation",
    "style": "chicago-fullnote-bibliography"
}
# note: pyzotero defaults limit to 100 under the hood
try:
    items_chunk = zot_library.items(**request_parameters)
except zotero.ze.HTTPError:
    handle_retries(request_parameters, zot_library.request.headers, 3)
else:    
    while True:
        # since items are sorted on API return, if last item in the chunk is later than specified terminus, add all to the array and get next slice
        if items_chunk[LIMIT-1]["data"]["dateModified"] > DATE_OF_LAST_DUMP:
            updated_records = updated_records + items_chunk
            print(f"appended full chunk. Start index = {request_parameters['start']}; limit = {request_parameters['limit']}")
            time.sleep(2)
            request_parameters["start"] += request_parameters["limit"] # set start to get next slice
            try:
                 #get the next slice of the data from the API
                items_chunk = zot_library.items(**request_parameters)
            except zotero.ze.HTTPError:
                handle_retries(request_parameters, zot_library.request.headers, 3)
        # if only a subset of items in the chunk are later than specified terminus, add that subset and break
        elif items_chunk[0]["data"]["dateModified"] > DATE_OF_LAST_DUMP:
            print(f"partial matches. Start index = {request_parameters['start']}; limit = {request_parameters['limit']}. Appending single records")
            for item in items_chunk:
                if item["data"]["dateModified"] > DATE_OF_LAST_DUMP:
                    updated_records.append(item)
                    print("appended single record")
                else: # items sorted by date; break once you reach the date of last dump to avoid unnecessary loops
                    break
            break
        else: # if no part of the slice is more recent than specified terminus, break from the loop
            print("no matches")
            break

    with open(SAVE_FILE, "w+") as f:
        json.dump(updated_records, f, indent=2)
    # print(json.dumps(item, indent=2))