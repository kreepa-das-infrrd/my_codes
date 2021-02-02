import csv

import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

INDEX_NAME = 'dev-gimlet-filter-scanrequest*'
SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"
sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
base_url = 'http://54.184.160.83:31000/'
document_filename_url = base_url + 'dev-gimlet-filter-scanrequest*/_search?size=1000'
headers = {
    'Content-Type': 'application/json'
}

# conn = mysql.connector.connect(
#     host="54.184.160.83",
#     port="32222",
#     user="root",
#     password="dep9espasTlqablwrest",
#     database="kms_db"
# )


def get_all_annotation_document(userids):

    ES_QUERY = {
        "bool": {
            "must": [
                {"terms": {
                    "userId.keyword": userids
                }}
            ]
        }
    }

    s = Search(using=sn_es_client, index=INDEX_NAME, doc_type='docs')
    s = s.query(ES_QUERY).params(size=10000)
    response = s.execute().to_dict()
    documents = [dict({"id": doc["_id"]}, **doc['_source']) for doc in response['hits']['hits']]
    return documents


def get_field_value_data(doc, json_dict):
    value_list = doc['fields']['collaterals']['value']
    for idx, item in enumerate(value_list, 1):
        for key in item.keys():
            if not item[key]:
                json_dict[f'policy{idx}_{key}'] = ""
                continue
            if type(item.get(key).get('value')) == dict:
                value = item.get(key).get('value')
                for k in value.keys():
                    json_dict[f'policy{idx}_{key}_{k}'] = value.get(k).get('value')
                continue
            if type(item.get(key).get('value')) == list:
                value = item.get(key).get('value')
                for no, driver in enumerate(value, 1):
                    k = list(driver.keys())[0]
                    temp_keys = driver.get(k).get('value').keys()
                    for temp_k in temp_keys:
                        json_dict[f'policy{idx}_{key}_{no}_driver_{temp_k}'] = driver.get(k).get('value').get(
                            temp_k).get('value')
                continue
            json_dict[f'policy{idx}_{key}'] = item.get(key).get('value')


# cursor = conn.cursor()
# cursor.execute("SELECT * FROM api_instances")
#
# myresult = cursor.fetchall()
#
# api_keys = """2ac033ea-efbe-40ce-a59c-08707f531554"""
# api_keys = api_keys.split()
# userids = []

# for x in myresult:
#     if x[2] in api_keys:
#         userids.append(x[6])
#
# print(userids)
userids = ["InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:00d81a61-7dd4-4355-bfef-66f47734e433",
 "InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:66a18a75-7a8e-49fb-9c5e-3ecc3621e6f8"]

json_data = []
documents = get_all_annotation_document(userids)

for doc in documents:
    try:
        json_dict = {}
        json_dict['scanRequestId'] = doc['scanRequestId']
        json_dict['s3ImagePath'] = doc.get('s3ImagePath')
        json_dict['fileName'] = doc.get('fileName')
        get_field_value_data(doc, json_dict)
        json_data.append(json_dict)
    except:
        print(doc)

csv_columns = []
for data in json_data:
    for keys in list(data.keys()):
        if keys not in csv_columns:
            csv_columns.append(keys)

csv_columns = list(csv_columns)
print(len(csv_columns))

try:
    with open(f'release1-test-set.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in json_data:
            writer.writerow(data)

except IOError:
    print("I/O error")

# df_1 = pd.read_csv('release_2_annotated_data.csv')
# csv_columns = list(df_1.columns)
#
# df = pd.read_csv('collateral_test.csv')
#
# keep_cols = csv_columns
# new_f = df[keep_cols]
# new_f.to_csv("collateral_test1.csv", index=False)
#
# df_new = pd.read_csv('collateral_test1.csv')
# print(len(df_new.columns))
# import csv
#
# df_exclude = pd.read_csv('release2_test_data_set.csv')
# saved_column = list(df_exclude['scanRequestId'])
#
# with open('correct_release2_doc_type_mapping - non list scan ids.csv', 'r') as t2:
#     filetwo = csv.reader(t2)
#     fields = next(filetwo)
#     rows = []
#
#     for row in filetwo:
#         if row[0].strip() not in saved_column:
#             rows.append(row)
#
# with open('release2_train_data_set.csv', 'w') as csvfile:
#     # creating a csv writer object
#     csvwriter = csv.writer(csvfile)
#
#     # writing the fields
#     csvwriter.writerow(fields)
#
#     # writing the data rows
#     csvwriter.writerows(rows)

# df = pd.read_csv('release2_train_data_set.csv')
# doc_type = list(df['document_type'])
# doc_type_set = set(doc_type)
# count_dict = {doc_t:doc_type.count(doc_t) for doc_t in doc_type_set}
# print(count_dict)


# with open('correct_release2_doc_type_mapping.csv', 'r') as fp:
#     file = csv.reader(fp)
#     fields = next(file)
#
#     doc_type_dict = {}
#     for row in file:
#         value_set = set()
#         if row[0].strip() not in doc_type_dict:
#             value_set.add(row[2].strip())
#             doc_type_dict[row[0].strip()] = value_set
#         else:
#             doc_type_dict[row[0].strip()].add(row[2].strip())
#
# list_type = []
# non_list_type = []
# for scanid, doc_type in doc_type_dict.items():
#     if len(doc_type) > 1:
#         list_type.append(scanid)
#     elif len(doc_type) == 1:
#         non_list_type.append(scanid)
#
# with open('correct_release2_doc_type_mapping.csv', 'r') as t2:
#     filetwo = csv.reader(t2)
#     fields = next(filetwo)
#     rows_list = []
#     rows_non_list = []
#
#     for row in filetwo:
#         if row[0].strip() in list_type:
#             rows_list.append(row)
#         elif row[0].strip() in non_list_type:
#             rows_non_list.append(row)
#
# with open('correct_r2__doc_type_list.csv', 'w') as listfile, open('correct_r2_doc_type_non_list.csv', 'w') as non_list_file:
#     # creating a csv writer object
#     csvwriter_list = csv.writer(listfile)
#
#     # writing the fields
#     csvwriter_list.writerow(fields)
#
#     # writing the data rows
#     csvwriter_list.writerows(rows_list)
#
#     csvwriter_Non_list = csv.writer(non_list_file)
#
#     # writing the fields
#     csvwriter_Non_list.writerow(fields)
#
#     # writing the data rows
#     csvwriter_Non_list.writerows(rows_non_list)


# df_excluded = pd.read_csv('id-doc_type_mapping_all_data_set_release2.csv')
# scanids = list(df_excluded['scanRequestId'])
# scanids = set(scanids)
# print(len(scanids))
#
# df_r2 = pd.read_csv('id-doc_type_mapping_all_data_set_release2_reuploads.csv')
# r2_scanid = list(df_r2['scanRequestId'])
# r2_scanids = set(r2_scanid)
# print(len(r2_scanids))
#
# count = len([1 for id in scanids if id in r2_scanids])
# print(count)


# import random
#
# doc_type_dict = {}
# with open('correct_release2_doc_type_mapping - non list scan ids.csv', 'r') as fp:
#     csvreader = csv.reader(fp)
#     fields =next(csvreader)
#
#     for row in csvreader:
#         if row[2] not in doc_type_dict:
#             scan_id_list = []
#             scan_id_list.append(row[0])
#             doc_type_dict[row[2]] = scan_id_list
#         else:
#             doc_type_dict[row[2]].append(row[0])
#
# test_set = []
# for doc_type, scan_ids in doc_type_dict.items():
#     if len(scan_ids) > 200:
#         test_set_num = int(0.2 * len(scan_ids))
#         test_set.extend(random.sample(scan_ids, test_set_num))
#
# with open('correct_release2_doc_type_mapping - non list scan ids.csv', 'r') as t2:
#     filetwo = csv.reader(t2)
#     fields = next(filetwo)
#     rows = []
#
#     for row in filetwo:
#         if row[0].strip() in test_set:
#             rows.append(row)
#
# with open('release2_test_data_set.csv', 'w') as csvfile:
#     # creating a csv writer object
#     csvwriter = csv.writer(csvfile)
#
#     # writing the fields
#     csvwriter.writerow(fields)
#
#     # writing the data rows
#     csvwriter.writerows(rows)
