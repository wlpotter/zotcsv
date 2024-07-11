import requests
import os
import pandas as pd
import csv
import json
import time

zotero_api_base = "https://api.zotero.org/groups/"
zotero_group_id = "4861694"
api_options = {
    "format": "format=json",
    "include": "include=bib,data,coins,citation",
    "style": "style=chicago-fullnote-bibliography"
  }

api_options_string = "?" + "&".join(api_options.values())

# set CSV I/O and Data Header parameters
in_csv_path = "/home/arren/Documents/Work_Syriaca/cbsc-bibls/download-json/2024-07-10_missing-json.csv"
csv_delimiter = "\t"

out_csv_path = in_csv_path # change to another path if desired
item_key_header = "itemKey"

csv_data = pd.read_csv(in_csv_path, delimiter=csv_delimiter)

json_output_path = "out/" + str(time.time()).replace(".", "_")
os.makedirs(json_output_path, exist_ok = True)
json_output_name_base = "cbss_"
all_json_items = []

for i, row in csv_data.iterrows():
    # skip rows marked as successful
    if(row['zotcsv_status'] == "Success"):
        continue
    key = row[item_key_header]
    url = zotero_api_base + zotero_group_id + "/items/" + key + api_options_string
    try:
        r = requests.get(url)
        r.raise_for_status()
        item = r.json()
    except requests.exceptions.HTTPError as errh:
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", errh)
        csv_data.at[i, 'zotcsv_status'] = "Error"
        csv_data.at[i, 'zotcsv_errormsg'] = errh.args[0]
        break
    except Exception as exc:
        # log any errors to the console, add a message to the csv
        print("ERROR. row: ", str(i+2), ". Item: ", row[item_key_header], ". ", exc)
        csv_data.at[i, 'zotcsv_status'] = "Error"
        csv_data.at[i, 'zotcsv_errormsg'] = exc
        # errors will currently force a fail state, exiting the script after saving progress
        break
    else:
        # add the json return to an array of jsons if there were no errors
        all_json_items.append(item)
        print("SUCCESS. row: ", str(i+2), ". Item: ", row[item_key_header], ".")
        csv_data.at[i, 'zotcsv_status'] = "Success"
        csv_data.at[i, 'zotcsv_errormsg'] = ""
        # log a success
    # delay by a second to try to not overload the server
    time.sleep(5)
'''
chunk the all_json_items into sets of 100 and save to a file
'''
for i in range(0, len(all_json_items), 100):
    # slice in chunks of 100, ending at the length of the array
    next_i = min(i+99, len(all_json_items))

    # take a slice of the json items and save to a file
    subset = all_json_items[i:next_i]
    with open(json_output_path+"/"+json_output_name_base+str(i)+".json", 'x', encoding='utf-8') as f:
        json.dump(subset, f, ensure_ascii=False, indent=4)


csv_data.to_csv(out_csv_path, sep=csv_delimiter, index=False)

