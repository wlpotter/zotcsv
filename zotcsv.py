from pyzotero import zotero
import pandas as pd
import csv
import json 

def append_data_in_extra(current_data, new_data):
    new_data = "\n".join(new_data)
    return "\n".join((current_data, new_data))

# open and parse JSON configuration file
# CHANGE: make this into a user input request to get the path to the config file
config_file = open("config.json")
config_info = json.load(config_file)

# set Zotero library parameters from config file
zotero_library_type = config_info['zotero']['library_type']
zotero_library_id = config_info['zotero']['library_id']
zotero_api_key = config_info['zotero']['api_key']

# set CSV I/O and Data Header parameters from config file
in_csv_path = config_info['csv']['input_path']
csv_delimiter = config_info['csv']['delimiter']
out_csv_path = config_info['csv']['output_path'] # CHANGE: by default, if empty this should just update the input csv
item_key_header = config_info['csv']['item_key_column_header']
data_headers = config_info['csv']['data_headers']

zot_library = zotero.Zotero(zotero_library_id, zotero_library_type, zotero_api_key)

"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~END CONFIG~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
csv_data = pd.read_csv(in_csv_path, delimiter=csv_delimiter)

for i, row in csv_data.iterrows():
    # put this into a try/catch block
    item = zot_library.item(row[item_key_header])
    # update any non-extra-field data
    # TBD

    # update extra field data
    extra_field_column_headers = []
    for header in data_headers:
        if(header['zotero_field'] == "extra"):
            extra_field_column_headers.append(header['column_header'])
    new_extra_field_data = []
    for field in extra_field_column_headers:
        if(not(pd.isna(row[field]))):
            new_extra_field_data.append(field+": "+row[field])
    extra = append_data_in_extra(item['data']['extra'], new_extra_field_data)
    print(extra)
    # Update Zotero item via write request
    # TBD
    """
    # Catch blocks:
    # UserNotAuthorized should not be caught so that the script stops and the error is logged to console
    catch InvalidItemFields as iif_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", iif_error)
        df.at[i, 'zotcsv_status'] = "InvalidItemFields"
    catch ResourceNotFound as rnf_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", rnf_error)
        df.at[i, 'zotcsv_status'] = "ResourceNotFound"
    catch HTTPError as http_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", http_error)
        df.at[i, 'zotcsv_status'] = "HTTPError"
    catch Conflict as c_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", c_error)
        df.at[i, 'zotcsv_status'] = "Conflict"
    catch PreConditionFailed as pcf_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", pcf_error)
        df.at[i, 'zotcsv_status'] = "PreConditionFailed"
    catch TooManyRequests as tcreq_error:
        # maybe try again??
    catch TooManyRetries as tcret_error:
        # want to do this for the remainder of the rows, right?
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", tcret_error)
        df.at[i, 'zotcsv_status'] = "TooManyRetries"
    else:
        print("SUCCESS. row: ", str(i+2), ". Item: ", row[item_key_header], ".")
        df.at[i, 'zotcsv_status'] = "Success"
    """
    # sleep(2000)
"""
# after the rows have been processed, write a csv
# csv_data.writecsv() to the output file path, using the same delimiter
"""