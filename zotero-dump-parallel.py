import asyncio
import argparse
import aiohttp
import json

# pass this to the 
config = {
    "zotero_api_base": "https://api.zotero.org/groups/4861694/items/top",
    "session_headers": {"Zotero-API-Version": '3'},
    "limit_interval": 100,
    "start_at": 0,
    "max_records": 500,
    "total_callers": 4,
}

# SAVE_DIRECTORY = "/home/arren/Documents/GitHub/zotcsv/2025-10-02_dump/"
# FILE_NAME_BASE = "zotero_dump2025-10-02_"

# MAX_RETRIES = 3
# DEFAULT_BACKOFF_DELAY = 15

# This is the main worker/consumer coroutine that processes the API calls and calls chained
# async coroutines for handling the request, retries, validation, and saving of the JSON data
async def api_caller(name: str, api_calls, session, results_log):
    # will run until cancelled by the main function
    # ADD TRY/CATCH and use finally to mark the task as done
    while True:
        try:
            # get an API call context from the queue
            call_context = await api_calls.get()
            print(f"{name} retrieved an API call to make")

            # do something with it
            url, result = await get_zotero_data(call_context=call_context, session=session, caller_name=name)
            print(f"{name} successfully retrieved data for {url}")

            returned_keys = [rec["key"] for rec in result]
            if(returned_key_sequence_matches_expected(returned_keys, call_context["expected_keys"])):
                print(f"{name}'s data matches expected key sequence; saving data to disk")
                file_path = await save_json_data(result, call_context)
                await results_log.put(log_success(file_path, url, call_context, name))
            else:
                raise ValueError("Returned item keys did not match the expected sequence")
            """
            If the result is successful (i.e., a list of JSON docs), then validate the keys
                if valid, save them to a file
                otherwise raise an error
            else if the subroutine is unsuccessful (i.e., retries exceeded and got an error), log that error
            """
        # Explicit handling of ValueError exception -- raised by 
        except ValueError as ve:
            print(f"{name} encountered a key mismatch error")
            logged_error = {
                "result": "Error",
                "context": call_context,
                "url": url,
                "returned_keys": returned_keys,
                "caller": name,
                "error": str(ve)
            }
            await results_log.put(logged_error)
        except Exception as e:
            print("An error occurred for {name}")
            logged_error = {
                "result": "Error",
                "context": call_context,
                "caller": name,
                "error": str(e)
            }
            await results_log.put(logged_error)
        finally:
            # give a bit of sleep delay for breathing room
            await asyncio.sleep(1)
            # Mark this task done
            api_calls.task_done()

async def get_zotero_data(call_context, session, caller_name):
    async with session.get(call_context["url_base"], params=call_context["request_params"]) as response:
        print(f"{caller_name} making API call to {response.url}")
        response.raise_for_status() # raise any exceptions for unsuccessful connects
        # TODO: turn into try/catch block that handles retries of 500 erros
        data = await response.json()
        return (response.url, data)

def returned_key_sequence_matches_expected(returned, expected):
    return returned == expected

async def save_json_data(result, call_context):
    # TODO: implement this function to save based on data in the call context
    return "Saved successfully; TBD to actually do this"


def log_success(file_path, call_context, caller_name, url):
    logged_success = {
                "result": "Success",
                "context": call_context,
                "url": url,
                "caller": caller_name,
                "message": f"Data successfully to {file_path}"
            }
    return logged_success

async def main(config):
    # initialize the request parameters for the first API call -- start will be updated 
    init_req_params = {
        "limit": config['limit_interval'],
        "start": config['start_at'],
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
        print("Preparing queue of needed API calls")
        api_calls_queue = asyncio.Queue()
        for i in range(config["start_at"], max_limit, config["limit_interval"]):
            api_call = {}

            # get a copy of the request parameters and set the "start" parameter based on the loop location
            req_params = init_req_params.copy()
            req_params["start"] = i

            # get a subset of the returned item keys to validate this API chunk against
            window_start = i
            window_end = i + config["limit_interval"]
            windowed_keys = expected_keys[window_start:window_end]
            
            # add these to a dictionary, which is pushed the work queue for later processing by the async workers
            api_call["url_base"] = config["zotero_api_base"]
            api_call["request_params"] = req_params
            api_call["expected_keys"] = windowed_keys
            api_calls_queue.put_nowait(api_call)

        # save the initial number of calls
        total_api_calls = api_calls_queue.qsize()

        # initialize a results Queue for logging the result of each task
        results_log = asyncio.Queue()

        # Create the api caller 'workers' 
        print(f"Initializing the {config['total_callers']} concurrent API calling functions")
        api_callers = []
        for i in range(config["total_callers"]):
            caller = asyncio.create_task(api_caller(f'caller-{i+1}', api_calls_queue, session, results_log))
            api_callers.append(caller)
        
        print("Beginning to make and process API Calls")
        await api_calls_queue.join()

        # Cleanup by cancelling callers
        for caller in api_callers:
            caller.cancel()
        
        await asyncio.gather(*api_callers, return_exceptions=True)

        """
        TODO:
        - get the items from results_log queue and conver to a list to dump as a JSON file
        """

        print('=========')
        print(f'{len(api_callers)} API Callers performed all {total_api_calls} API Calls. See the logs for full results')
        """
        - define the async chain of work for:
            - handling retries
            - saving the file
        - add to this the functionalities for 'since' and last modified saving
        """
asyncio.run(main(config))