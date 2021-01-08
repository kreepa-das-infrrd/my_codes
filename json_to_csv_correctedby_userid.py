import csv
import pandas as pd

import mysql.connector
import requests
from elasticsearch import Elasticsearch

INDEX_NAME = 'dev-gimlet-filter-scanrequest*'
SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"
sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
base_url = 'http://54.184.160.83:31000/'
document_filename_url = base_url + 'dev-gimlet-filter-scanrequest*/_search?size=1000'
headers = {
    'Content-Type': 'application/json'
}

conn = mysql.connector.connect(
    host="54.184.160.83",
    port="32222",
    user="root",
    password="dep9espasTlqablwrest",
    database="kms_db"
)


def get_all_annotation_document(userid, correctedby):
    documents = None

    ES_QUERY = {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "userId.keyword": {
                            "value": userid
                        }
                    }},
                    {
                        "term": {
                            "correctedBy.keyword": {
                                "value": correctedby
                            }
                        }
                    }
                ]
            }
        }
    }

    if not documents:
        try:
            response = requests.get(url=document_filename_url, json=ES_QUERY, headers=headers).json()
            documents = [dict(**doc['_source']) for doc in response['hits']['hits']]
        except Exception as e:
            print(e)
    return documents


def get_field_value_data(doc, json_dict):
    field_dict = doc.get('fields')
    value_list = field_dict.get('collaterals').get('value')
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


cursor = conn.cursor()
cursor.execute("SELECT * FROM api_instances")

myresult = cursor.fetchall()

api_keys = ["572899fd-0824-4a41-928e-95c07ddab847",
"c6adc639-476a-4443-8e7c-f5bfa2578d7e",
"61d58d1b-e417-4cd4-b7e6-b93df64daa2f",
"502ee35d-181b-4056-9253-2f4509c9a701",
"edfd5f15-8a25-4f39-ae4c-c08b00a0f030",
"a77cc6cf-cec8-4494-b23e-63994b16a9e5",
"c20059ce-7cb7-423b-af49-2f3f47c7bafa",
"a77cc6cf-cec8-4494-b23e-63994b16a9e5",
"20155037-cef9-45b8-8a45-76846ea07ba2",
"56d99a8f-075b-4aa9-85c1-157feee68d2d",
"9c5c36ba-c469-4085-b5d3-96d30263b7f0",
"ca2eed31-9078-4544-b8d3-ec7003eb1b62",
"5fa3b668-e6d0-46dc-89ed-f4f60fbfcf68",
"936d3311-2f57-468f-ba2c-640f6d2886d3"]
userids = []
correctedby_ids = ["e917397d-8285-4d2c-9d1f-3928f187896a", "278f5fe3-66a8-40a9-92fd-c143c2a550c5", "7464b341-f425-4f1c-937e-a3751af68143",
                   "389bd169-0115-49da-8cfe-c43b69eebc63"]

for x in myresult:
    if x[2] in api_keys:
        userids.append(x[6])

print(userids)

json_data = []
for correctedby in correctedby_ids:
    for userid in userids:
        documents = get_all_annotation_document(userid, correctedby)

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
        with open(f'{correctedby}_release_2_annotated_data.csv', 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in json_data:
                writer.writerow(data)

    except IOError:
        print("I/O error")


