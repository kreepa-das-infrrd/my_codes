import json
import pathlib
from collections import Counter
from itertools import groupby

import pandas as pd

base_dir = 'report/'

p = pathlib.Path(base_dir).glob('report_*.json')

summary = []
columns = []
for i in p:
    try:
        with open(str(i), "r") as fp:
            doc_ids = json.load(fp)
            for doc, value in doc_ids["report_data"].items():
                for items in value:
                    summary.append([items["label"], items["train_text"], items["doc_type"]])

    except:
        pass
    
report_data = dict()
summary.sort(key=lambda x: x[0])
data = groupby(summary, key=lambda x: x[0])
json_data = []
for field_name, tup in data:
    field_data = list(tup)
    json_data.extend(field_data)

mod_data = [tuple(i) for i in json_data]
value = Counter(mod_data)

csv_data = []
for item, count in value.items():
    json_dict = {}
    json_dict[item[0]] = item[1]
    json_dict[f'{item[0]}_count'] = count
    json_dict[f'{item[0]}_vendor'] = item[2]
    csv_data.append(json_dict)

ent = list(csv_data[0].keys())[0]
temp_lists = []
temp_inner_list = []
for idx, data_dict in enumerate(csv_data):
    if ent in list(data_dict.keys()):
        temp_inner_list.append(data_dict)
        if idx == len(csv_data) - 1:
            temp_lists.append(temp_inner_list)
    else:
        temp_lists.append(temp_inner_list)
        ent = list(data_dict.keys())[0]
        temp_inner_list = []
        temp_inner_list.append(data_dict)

temp_lists.sort(key = len)

temp_lists = [sorted(data, key=lambda x: x.get(list(x.keys())[2])) for data in temp_lists]

df = pd.DataFrame(temp_lists[0])
for index, temp_list in enumerate(temp_lists):
    if index != 0:
        df1 = pd.DataFrame(temp_list)
        df = pd.concat([df, df1], axis=1)

df.to_csv('summary_field.csv', index=False)
