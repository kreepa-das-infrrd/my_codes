import csv
import pandas as pd
import tqdm

import mysql.connector
import requests
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

conn = mysql.connector.connect(
    host="54.184.160.83",
    port="32222",
    user="root",
    password="dep9espasTlqablwrest",
    database="kms_db"
)

VENDOR_TYPES_LIST = ["CANCELLATION", "INTENT TO CANCEL", "ACORD FORM", "BINDER", "DECLARATION", "ID CARD",
                     "RENEWAL", "CHANGE REQUEST", "AMENDMENT", "OTHER", "REINSTATEMENT", "APPLICATION"]


def get_all_annotation_document(userids):
    ES_QUERY = {
            "bool": {
                "must": [
                    {"terms": {
                        "userId.keyword": userids
                    }},
                    {
                        "term": {
                            "isCorrected": {
                                "value": True
                            }
                        }
                    }
                ]
            }
        }



    # response = requests.get(url=document_filename_url, json=ES_QUERY, headers=headers).json()
    # documents = [dict(**doc['_source']) for doc in response['hits']['hits']]
    s = Search(using=sn_es_client, index=INDEX_NAME, doc_type='docs')
    s = s.source(includes=["scanRequestId", "fileName", "fields.collaterals.value.document_type.value"])
    s = s.query(ES_QUERY).params(size=10000)
    documents = []
    for hit in s.scan():
        documents.append(hit)
    return documents


def get_vendor_type(vendor_name):
    vendor_type_mappings = {
        "CANCELLATION": ["Personal Auto Policy Cancellation Notice", "CANCELLATION CUSTOMER REQUEST"],
        "INTENT TO CANCEL": ["NOTICE OF CANCELLATION OR REFUSAL TO RENEW", "NOTICE OF CANCELLATION"],
        "ACORD FORM": ["EVIDENCE OF PROPERTY INSURANCE", "VEHICLE OR EQUIPMENT CERTIFICATE OF INSURANCE"],
        "BINDER": ["APPLICATION BINDER"]
    }

    for vendor_type, synon_list in vendor_type_mappings.items():
        if any([v_type for v_type in synon_list if vendor_name.lower() in v_type.lower()]):
            return vendor_type

    return vendor_name


def get_doc_types(doc, json_data):
    doc_types_list = doc.fields.collaterals.value
    for doc_type in doc_types_list:
        json_dict = {}
        json_dict['scanRequestId'] = doc['scanRequestId']
        json_dict['fileName'] = doc['fileName']
        vendor_type = doc_type.document_type.value
        if vendor_type and vendor_type not in VENDOR_TYPES_LIST:
            vendor_type = get_vendor_type(vendor_type)
        elif not vendor_type:
            vendor_type = 'OTHER'
        json_dict['document_type'] = vendor_type
        json_data.append(json_dict)


# cursor = conn.cursor()
# cursor.execute("SELECT * FROM api_instances")
#
# myresult = cursor.fetchall()
#
# api_keys = """ade801ef-e090-4217-8b63-159cefb9cb36
# d7f197ca-7bfc-4cba-a4aa-2556c3e22950"""
# api_keys = api_keys.split()
# userids = []
#
# for x in myresult:
#     if x[2] in api_keys:
#         userids.append(x[6])

# print(userids)

userids = """
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:0933d8d0-9b28-4b90-bbce-1fe181a86f18
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:0c86a897-d8c2-4959-8e21-375668e8b625
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:12a486bb-186a-42bf-902c-e2ac0667523d
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:1398a47c-0aec-4a27-afb4-2bcd8246b64d
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:17eece7e-d43b-4f87-89a2-4063be53cefe
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:4968c932-fffe-4df0-ae53-58b9e6a55787
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:62a024bf-9905-408a-ac4e-215a02b1ca26
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:6b3f47dd-ba06-4bd3-9c79-454bf572fa87
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:6b9d5b11-8ec9-423a-b8a4-940d1cdfce5d
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:7f2f5e33-1bc4-4857-81d3-ee700e1d41a9
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:888940f9-d598-47d9-9abc-2986b364fdb7
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:a7c54208-07eb-45e3-aeb1-a11c6d749c0d
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:c21e1eac-1918-4fd3-943f-3ecbc0c5479b
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:c33733d0-7604-4495-bab0-7ec1dddded85
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:c8ea8c69-fba9-432c-adb7-3c7e2a12623d
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:cc05c342-ea31-4b14-9bf8-f90e3b93a767
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:d8b3dd3d-e652-45d7-a452-d6048bd5d4a2
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:da251579-33a2-49ab-b5f1-1241f799bdcb
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:dbdf1647-78d4-4aaf-9b5a-2323ab49cdef
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:e48dc1c1-4a0e-454b-ad40-be17f5a7bd60
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:e50d23c0-5651-46da-951c-d5e5c32d169e
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:e71758ac-e783-49c0-9f00-d722ce39f49e
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:f8c24eef-6580-44f5-a9aa-cd72a05a1626
InfrrdUser:1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9,KongUsername:ff42d77a-8a5f-48b1-a13e-fee9d03bc58c
"""

userids = userids.split('\n')

json_data = []
documents = get_all_annotation_document(userids)

for doc in tqdm.tqdm(documents):
    try:
        get_doc_types(doc, json_data)
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
    with open(f'id-doc_type_mapping_all_data_set_release2_reuploads.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in json_data:
            writer.writerow(data)

except IOError:
    print("I/O error")