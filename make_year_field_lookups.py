import requests
import csv
import pandas as pd
import re
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"

INDEX_NAME = 'dev-gimlet-filter-scanrequest*'
user_model_id = '895823ba-9fd3-4fbb-ae81-06a53b32a916'  # SN-PoC user model id
customer_id = '6cc6764f-ae80-40d1-87ec-4f7737057f2c'  # SN- accountID
time_format = "%Y-%m-%d %H:%M:%S:%f"
sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
base_url = 'http://54.184.160.83:31000/'
document_filename_url = base_url + 'dev-gimlet-filter-scanrequest*/_search'
headers = {
    'Content-Type': 'application/json'
}




def get_all_annotation_document():
    documents = None

    ES_QUERY = {
        "size": 10000,
  "_source": {
    "includes": [
    "fields.collaterals.value.make_year_model"]
    },
  "query": {
    "bool": {
        "must": [
            {
                "term": {
                    "isCorrected": {
                        "value": True
                    }
                }
            },
            {
                "range": {
                    "uploadDateTime": {
                        "gte": 1602590413000
                    }
                }
            },
            {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "userId": "1d220e85-b6fb-4fbb-bf5f-d918fa44bbb9"
                            }
                        }
                    ]
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


documents = get_all_annotation_document()
vehicle_make_set = set()
vehicle_model_set = set()
vehicle_year_set = set()
c = 0
for doc in documents:
    vehicle_make_list = doc.get('fields').get('collaterals').get('value')
    try:
        for item in vehicle_make_list:
            vehicle_make_set.add(item['make_year_model']['value'])
    except:
        c += 1

l = [(x,) for x in vehicle_make_set if x is not None]
print(l)


with open('make_year_model.csv', 'w') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['make_year_model'])
    for row in l:
        csv_out.writerow(row)


# df = pd.read_csv('vehicle_year.csv')
# make = df['Year']
# veh_make = list(filter(lambda x: x =='TRY', make))
# print(veh_make)

