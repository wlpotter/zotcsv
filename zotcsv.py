"""
This script updates a set of Zotero item records using a CSV data sheet.
It relies on the pyzotero library to interact with the Zotero Web API to read
and write Zotero items.
Data is updated from the CSV columns declared in a config.json file.
Currently only supports appending values to the "Extra" field, but more functionality may be
implemented down the line.

Initial use case is the migration of the Comprehensive Bibliography on Syriac Studies to a
Srophe Application with Zotero backend. This project required appending crosswalks between
the Syriaca.org Bibliography URIs and the new CBSS Bibliography URIs.
"""
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
if(config_info['csv']['output_path'] != ""):
    out_csv_path = config_info['csv']['output_path']
else:
    out_csv_path = in_csv_path
item_key_header = config_info['csv']['item_key_column_header']
data_headers = config_info['csv']['data_headers']

zot_library = zotero.Zotero(zotero_library_id, zotero_library_type, zotero_api_key)

"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~END CONFIG~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""
csv_data = pd.read_csv(in_csv_path, delimiter=csv_delimiter)

for i, row in csv_data.iterrows():
    # for each row, search for item and record any errors to console and in data frame
    try:
        item = zot_library.item(row[item_key_header])
    except zotero.ze.ResourceNotFound as rnf_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", rnf_error)
        csv_data.at[i, 'zotcsv_status'] = "ResourceNotFound"
        csv_data.at[i, 'zotcsv_errormsg'] = rnf_error
    except zotero.ze.HTTPError as http_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", http_error)
        csv_data.at[i, 'zotcsv_status'] = "HTTPError"
        csv_data.at[i, 'zotcsv_errormsg'] = http_error
    except zotero.ze.Conflict as c_error:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", c_error)
        csv_data.at[i, 'zotcsv_status'] = "Conflict" 
        csv_data.at[i, 'zotcsv_errormsg'] = c_error
    else:
        # if the item is successfully found, attempt to update the field(s) based on the csv data
        """
        # update any non-extra-field data
        # TBD
        """
        # update extra field data. For now, extra field data is appended rather than overwritten
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
        
        # Update Zotero item via write request, logging errors to console and CSV
        try:
            zot_library.update_item(item)
        #log failures to console and record them in CSV
        except zotero.ze.InvalidItemFields as iif_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", iif_error)
            csv_data.at[i, 'zotcsv_status'] = "InvalidItemFields"
            csv_data.at[i, 'zotcsv_errormsg'] = iif_error
        except zotero.ze.HTTPError as http_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", http_error)
            csv_data.at[i, 'zotcsv_status'] = "HTTPError"
            csv_data.at[i, 'zotcsv_errormsg'] = http_error
        except zotero.ze.Conflict as c_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", c_error)
            csv_data.at[i, 'zotcsv_status'] = "Conflict"
            csv_data.at[i, 'zotcsv_errormsg'] = c_error
        except zotero.ze.PreConditionFailed as pcf_error:
            print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", pcf_error)
            csv_data.at[i, 'zotcsv_status'] = "PreConditionFailed"
            csv_data.at[i, 'zotcsv_errormsg'] = pcf_error
        except zotero.ze.TooManyRequests as tmreq_error:
            try:
                time.sleep(30)
                zot_library.update_item(item)
            except zotero.ze.TooManyRequests as tmreq_error2:
                print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", tmreq_error2)
                csv_data.at[i, 'zotcsv_status'] = "TooManyRequests"
                csv_data.at[i, 'zotcsv_errormsg'] = tmreq_error2
            except zotero.ze.TooManyRetries as tmret_error2:
                print("FATAL ERROR. Any rows after row", str(i+1), "were not processed. Zotero API rate limit reached, or server was busy. Please wait a few minutes before retrying.")
                print("You can avoid this problem in the future by processing smaller batches of records.")
                print(tmret_error2)
                csv_data.at[i, 'zotcsv_status'] = "TooManyRetries"
                csv_data.at[i, 'zotcsv_errormsg'] = tmret_error2
                break
            else:
                print("SUCCESS. row: ", str(i+2), ". Item: ", row[item_key_header], ".")
                csv_data.at[i, 'zotcsv_status'] = "Success"
                csv_data.at[i, 'zotcsv_errormsg'] = ""
        except zotero.ze.TooManyRetries as tmret_error:
            print("FATAL ERROR. Any rows after row", str(i+1), "were not processed. Zotero API rate limit reached, or server was busy. Please wait a few minutes before retrying.")
            print("You can avoid this problem in the future by processing smaller batches of records.")
            print(tmret_error)
            csv_data.at[i, 'zotcsv_status'] = "TooManyRetries"
            csv_data.at[i, 'zotcsv_errormsg'] = tmret_error
            break
        else:
            print("SUCCESS. row: ", str(i+2), ". Item: ", row[item_key_header], ".")
            csv_data.at[i, 'zotcsv_status'] = "Success"
            csv_data.at[i, 'zotcsv_errormsg'] = ""
        # wait 3 seconds between each row to avoid overloading Web API
        time.sleep(3)

# Finally, output the updated CSV, including the "zotcsv_status" column
csv_data.to_csv(out_csv_path, sep=csv_delimiter, index=False)