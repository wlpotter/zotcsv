'''
take the files in out/
read them in as json
pull all the items into a single list of dictionaries
chunk again by 100s
save to files starting with zotero_27800.json and going up by 100
'''
import json
import os
from pathlib import Path

in_folder = "out/"
out_folder = "data/"
os.makedirs(out_folder, exist_ok = True)
file_name_postfix_offset = 27800

zotero_all = []

for path in Path(in_folder).rglob("*.json"):
    with path.open() as f:
        contents = json.load(f)
        zotero_all.extend(contents)
# print(len(zotero_all))
for i in range(0, len(zotero_all), 100):
    
    # slice in chunks of 100, ending at the length of the array
    next_i = min(i+99, len(zotero_all))
    # take a slice of the json items and save to a file
    subset = zotero_all[i:next_i]
    with open(out_folder + "zotero_"+str(i+file_name_postfix_offset)+".json", 'w', encoding='utf-8') as f:
        json.dump(subset, f, ensure_ascii=False, indent=4)
