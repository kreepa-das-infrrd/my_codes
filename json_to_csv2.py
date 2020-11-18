import csv

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


def get_all_annotation_document(userid):
    documents = None

    ES_QUERY = {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "userId.keyword": {
                            "value": userid
                        }
                    }}
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


conn = mysql.connector.connect(
    host="54.184.160.83",
    port="32222",
    user="root",
    password="dep9espasTlqablwrest",
    database="kms_db"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM api_instances")

myresult = cursor.fetchall()

api_keys = ['ade801ef-e090-4217-8b63-159cefb9cb36', 'd7f197ca-7bfc-4cba-a4aa-2556c3e22950']
userids = []

for x in myresult:
    if x[2] in api_keys:
        userids.append(x[6])

json_data = []
for userid in userids:
    documents = get_all_annotation_document(userid)

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

try:
    with open('new_annotated_data.csv', 'a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in json_data:
            writer.writerow(data)

except IOError:
    print("I/O error")
