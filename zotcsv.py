from pyzotero import zotero
import pandas as pd
import csv
import json
import time

def append_data_in_extra(current_data, new_data):
    new_data = "\n".join(new_data)
    return "\n".join((current_data, new_data))

# open and parse JSON configuration file
# CHANGE: make this into a user input request to get the path to the config file
# CHANGE: move this to a config.py module that gets imported
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
    try:
        item = zot_library.item(row[item_key_header])
    except ResourceNotFound as rnf_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", rnf_error)
        csv_data.at[i, 'zotcsv_status'] = "ResourceNotFound"
    except HTTPError as http_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", http_error)
        csv_data.at[i, 'zotcsv_status'] = "HTTPError"
    except Conflict as c_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", c_error)
        csv_data.at[i, 'zotcsv_status'] = "Conflict" 
    else:
        """
        # update any non-extra-field data
        # TBD
        """
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

        item['data']['extra'] = extra.lstrip()
        # print(json.dumps(item, indent=4))
        # Update Zotero item via write request
        try:
            zot_library.update_item(item)
        #log failures to console and record them in CSV
        except InvalidItemFields as iif_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", iif_error)
            csv_data.at[i, 'zotcsv_status'] = "InvalidItemFields"
        except HTTPError as http_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", http_error)
            csv_data.at[i, 'zotcsv_status'] = "HTTPError"
        except Conflict as c_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", c_error)
            csv_data.at[i, 'zotcsv_status'] = "Conflict"
        except PreConditionFailed as pcf_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", pcf_error)
            csv_data.at[i, 'zotcsv_status'] = "PreConditionFailed"
        # except TooManyRequests as tmreq_error:
        #   TBD. Ideally would try again after like 10-30 seconds
        # except TooManyRetries as tmret_error:
        #   TBD. Ideally would log a message like 'too many retries starting at row x', print the error. Then would break out of the loop and record "TooManyRetries" for the remaining records in the CSV before writing the CSV
        else:
            print("SUCCESS. row: ", str(i+2), ". Item: ", row[item_key_header], ".")
            csv_data.at[i, 'zotcsv_status'] = "Success"
        time.sleep(3)
        # Update Zotero item via write request
        # TBD
"""
# after the rows have been processed, write a csv
# csv_data.writecsv() to the output file path, using the same delimiter
"""