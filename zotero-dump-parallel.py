import asyncio
import argparse
import aiohttp
import json, yaml
import os
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

"""
TODO:
- handling modified after filtering
- add max retries and default backoff to config, and how to call from the retry functions
- add checking for 429 and response delay header to tenacity retry
- reorganize (and maybe split this whole thing out...into its own Python module/project)
"""

# This function controls the number of concurrent calls by bounding the main
# async coro within a Semaphore, limiting max concurrent operations
async def bounded_get_zotero_data(sem, call_context, session, sleep_time=1):
    async with sem:
        return await get_zotero_data(call_context, session, sleep_time)

# This function handles the main logic of getting and processing Zotero data
# from the API, checking the key sequence, and saving to file
async def get_zotero_data(call_context, session, sleep_time=1):
    # Catches and returns exceptions to the log file, specifically processing HTTP and Value errors for key mismatch
    try:
        # Attempt to download data from Zotero
        url, result = await make_api_call(call_context, session)
        print(f'Successfully retrieved data for {url}')

        returned_keys = [rec["key"] for rec in result]
        if(returned_key_sequence_matches_expected(returned_keys, call_context["expected_keys"])):
            print(f"Records returned for {url} matches expected key sequence; saving data to disk")
            file_path = await save_json_data(result, call_context)
            return log_success(file_path=file_path, call_context=call_context, url=url)
        else:
            raise ValueError("Returned item keys did not match the expected sequence")
    except aiohttp.ClientResponseError as re:
        print(f"HTTP {re.status} error fetching data from {re.request_info.url}. Retry attempts exceeded, logging final response headers and message.")
        logged_error = {
            "result": "Error",
            "context": call_context,
            "url": str(re.request_info.url),
            "status_code": re.status,
            "response_headers": re.headers,
            "error": str(re)
        }
        return logged_error
    except ValueError as ve:
        print(f"Keys returned by {url} did not match the expected sequence, see log for details")
        logged_error = {
            "result": "Error",
            "context": call_context,
            "url": url,
            "returned_keys": returned_keys,
            "error": str(ve)
        }
        return logged_error
    # Handle any other exception by returning it to the log
    except Exception as e:
        print(f"An unspecified error occurred processing an API call, see log for details")
        print(e)
        logged_error = {
            "result": "Error",
            "context": call_context,
            "error": str(e)
        }
        return logged_error
    finally:
        # give a bit of breathing room
        await asyncio.sleep(sleep_time)

# This function is used by the retry logic to determine whether to retry the request
# Retries if a response exception was raised, and the status code is in the set of server errors, or the 429 Too Many Requests error
# This will allow other errors, such as 404 or 403, to pass immediately back to the caller
def is_retriable_error(exception):
    return isinstance(exception, aiohttp.ClientResponseError) and exception.status in {429, 500, 502, 503, 504}

# The logic for making the API call itself, which is retriable using tenacity
# Will be retried if a response error occurs and the status code is 429 or a server error
# Tries at least 3 times at a randomly expanding window
# If attempts exceeded, will re-raise the error to be handled by the caller coro
@retry(retry=retry_if_exception(is_retriable_error),
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1, max=60),
    reraise=True)
async def make_api_call(call_context, session):
    async with session.get(call_context["url_base"], params=call_context["request_params"]) as response:
        print(f"Making API call to {response.url}")
        # raise any exceptions for unsuccessful connects, which triggers tenacity to retry
        response.raise_for_status()
        # if no error occured, return the URL and the JSON data
        data = await response.json()
        return str(response.url), data

def returned_key_sequence_matches_expected(returned, expected):
    return returned == expected

# Saves the JSON data to disk, returning the filepath of the JSON file for reference
async def save_json_data(result, call_context):
    filepath = call_context["save_directory"] + call_context["file_name"]
    with open(filepath, "w+") as f:
        json.dump(result, f, indent=2)
    return filepath


def log_success(file_path, call_context, url):
    logged_success = {
                "result": "Success",
                "context": call_context,
                "url": url,
                "message": f"Data saved successfully to {file_path}"
            }
    return logged_success

def create_api_calls_list(config, max_limit: int, init_req_params, expected_keys):
    if(config.get("errors_only") and config["errors_only"]):
        return get_api_calls_list_from_error_log(config)
    else:
        return construct_api_calls_list(config=config, max_limit=max_limit, expected_keys=expected_keys, init_req_params=init_req_params)

def get_api_calls_list_from_error_log(config):
    filepath = config["log_path"] + config["log_file_name"]
    print(f"Preparing queue of needed API calls from previously logged errors found in {filepath}")
    # open and parse the JSON log file
    with open(filepath, 'r') as f:
        log = json.load(f)
        # Get the context, i.e. the API call context, for all "Error" records in the log
        errors = [rec["context"] for rec in log if rec["result"] == "Error"]
        return errors

def construct_api_calls_list(config, max_limit: int, init_req_params, expected_keys):
    print(f"Preparing queue of needed API calls (Items {config['start_at']} to {max_limit} at an interval of {config['limit_interval']})")
    api_calls = []
    for i in range(config["start_at"], max_limit, config["limit_interval"]):
        api_call = {}

        # set the start and end window
        window_start = i
        window_end = window_start + config["limit_interval"]

        # get a copy of the request parameters and set the "start" parameter based on the loop location
        req_params = init_req_params.copy()
        req_params["start"] = window_start

        # get a subset of the returned item keys to validate this API chunk against
        windowed_keys = expected_keys[window_start:window_end]
        
        # add these to a dictionary, which is pushed the work queue for later processing by the async workers
        api_call["url_base"] = config["zotero_api_base"]
        api_call["request_params"] = req_params
        api_call["expected_keys"] = windowed_keys
        api_call["save_directory"] = config["save_directory"]
        api_call["file_name"] = f'{config["file_name_base"]}{window_start}-{window_end - 1}.json'
        api_calls.append(api_call)
    return(api_calls)

async def main(config):
    # initialize the request parameters for the first API call -- start will be updated 
    init_req_params = {
        "limit": config['limit_interval'],
        "start": config['start_at'],
        "since": config.get("since", 0),
        "format": "json",
        "include": "bib,data,coins,citation",
        "style": "chicago-fullnote-bibliography"
    }

    async with aiohttp.ClientSession(headers=config['session_headers']) as session:
        print("Getting list of Item Keys from Zotero API")
        expected_keys = []
        async with session.get(config['zotero_api_base'], params={"format": "keys"}) as response:
            print(f"URL: {response.url}. Status Code: {response.status}")
            keys = await response.text()
            expected_keys = keys.splitlines()
    
        print(f"Got {len(expected_keys)} keys")

        # set the max limit to the lesser of the number of returned item keys or the constant, if set
        max_limit = len(expected_keys)
        if(config.get("max_records")):
            max_limit = min(max_limit, config["max_records"])

        # Create an async work queue for the API calls based on the start, max limit, and interval parameters
        api_calls = create_api_calls_list(config=config, max_limit=max_limit, expected_keys=expected_keys, init_req_params=init_req_params)

        # save the initial number of calls
        total_api_calls = len(api_calls)
        print(f"A total of {total_api_calls} API calls will be made.")


        # Create a Semaphore to control number of simultaneous callers allowed
        sem = asyncio.Semaphore(config["total_callers"])

        print("Beginning API calls")
        callers = [
            bounded_get_zotero_data(sem, call, session)
            for call in api_calls
        ]
        
        results = await asyncio.gather(*callers)

        print("Gathering and saving results log to file")
        log_file_path = config["log_path"] + config["log_file_name"]
        with open(log_file_path, 'w+') as log:
            json.dump(results, log, indent=2)
        print(f"Log file saved to {log_file_path}")

        num_success = len([r for r in results if r["result"] == "Success"])
        num_error = len([r for r in results if r["result"] == "Error"])
        print('=========')
        print(f'All {total_api_calls} API Calls have been made, with {num_success} successful; {num_error} error(s). See the log file for full results')

"""
START
"""
# Get the command line variables via argparse
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Path to the config JSON (or YAML) file")
parser.add_argument("-e", "--errors", action="store_true", help="Whether to re-run just on the errors found in the log file")
args = parser.parse_args()

# Read in and set up the config file based on terminal arguments
config = {}
with open(args.config, 'r') as f:
    if(args.config.endswith('.yml') or args.config.endswith('.yaml')):
        config = yaml.safe_load(f)
    elif(args.config.endswith('.json')):
        config = json.load(f)
    else:
        raise("Config file type not recognized, please use either a JSON (.json) or YAML (.yml or .yaml) file.")

# Set whether errors 
if(args.errors):
    config["errors_only"] = True

# Make sure the directories are created for saving files to them
os.makedirs(config["save_directory"], exist_ok=True)
os.makedirs(config["log_path"], exist_ok=True)

# run the main function, passing in the config dictionary
asyncio.run(main(config))