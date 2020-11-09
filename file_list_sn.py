import requests

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

ES_QUERY = {
  "_source": ["s3ImagePath"],
  "query": {
    "terms": {
      "scanRequestId.keyword": [
        "3a98653d-6999-11ea-9497-d52667486c1d",
        "2f36ba57-6999-11ea-9aec-299865bc9b0d",
        "c7777960-6999-11ea-9aec-f9c8b89aafa0",
        "3a9864ca-6999-11ea-9aec-d711d8adcf6a",
        "2ecb9ce9-6999-11ea-9497-d98768a83aad",
        "65645b65-6999-11ea-9aec-0330a35e2db6",
        "67b09614-6999-11ea-bf75-476007074a5a",
        "6778203d-6999-11ea-9aec-0d1d95dd2139",
        "2ef396d6-6999-11ea-9aec-b747f4e5a839",
        "c8557d53-6999-11ea-9aec-85f3da04cb80",
        "c7777a10-6999-11ea-bf75-13ae65c23200",
        "3b46f50a-6999-11ea-bf75-17f12e271460",
        "2e140d06-6999-11ea-bf75-4fd205888eae",
        "3a98b3a9-6999-11ea-bf75-83e5b0a4a600",
        "c8070f88-6999-11ea-9497-5b2a0a380ea2",
        "66b397b9-6999-11ea-9aec-59e8c45d57e5",
        "815d9cbc-6999-11ea-9aec-39ddb15128d1",
        "3b0ad5ab-6999-11ea-9aec-f70c6e4579be",
        "665b6651-6999-11ea-bf75-6f0a84b94c71",
        "813db8ab-6999-11ea-9aec-f32a20edb201",
        "6619f080-6999-11ea-bf75-c3f89916f22d",
        "2f49315b-6999-11ea-9497-4b2c1bce0d82",
        "66ec820b-6999-11ea-9aec-237c14389962",
        "686f0361-6999-11ea-9aec-731f63beb319",
        "0b4dc4a5-6999-11ea-bf75-037eda191ee3",
        "3a9864c9-6999-11ea-9aec-cd58683e83cc",
        "66b5ba9a-6999-11ea-9aec-f5bd5d785973"
      ]
    }
  }
}

INDEX_NAME = 'dev-gimlet-filter-scanrequest*'
SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"
sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
base_url = 'http://54.184.160.83:31000/'
document_filename_url = base_url + 'dev-gimlet-filter-scanrequest*/_search?size=50'
headers = {
    'Content-Type': 'application/json'
}


def get_all_annotation_document():
    documents = None

    if not documents:
        try:
            response = requests.get(url=document_filename_url, json=ES_QUERY, headers=headers).json()
            documents = [dict(**doc['_source']) for doc in response['hits']['hits']]
        except Exception as e:
            print(e)
    return documents


all_annotation_document = get_all_annotation_document()
for dict in all_annotation_document:
  print(dict['s3ImagePath'].split('/')[-1])
