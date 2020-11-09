import pandas as pd
import requests

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


INDEX_NAME = 'dev-gimlet-filter-scanrequest*'
SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"
sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
base_url = 'http://54.184.160.83:31000/'
document_filename_url = base_url + 'dev-gimlet-filter-scanrequest*/_search?size=500'
headers = {
    'Content-Type': 'application/json'
}

def get_file_names(req_id_list):
    query = {
        "_source": ["fileName"],
        "query": {
            "terms": {
                "scanRequestId.keyword": [req_id_list]
                }
            }
        }
    documents = None

    if not documents:
        try:
            response = requests.get(url=document_filename_url, json=query, headers=headers).json()
            documents = [dict(**doc['_source']) for doc in response['hits']['hits']]
        except Exception as e:
            print(e)
    return documents


df = pd.read_csv('/home/users001/Downloads/List of ScanIds - Rajneesh.csv')
saved_column = list(df['scanRequestId'])
print(saved_column)
filenames = []
for req_id in saved_column:
    filename = get_file_names(req_id)
    filenames.append(filename)

for filename in filenames:
    print(filename[0]['fileName'])