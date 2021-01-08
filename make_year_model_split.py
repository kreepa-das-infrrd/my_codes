import requests
import csv


base_url = 'http://localhost:9200'

ES_QUERY_COUNT = {
  "query": {
    "bool": {
      "must": [
        {"terms": {
          "fieldName.keyword": [
              "MAKE_YEAR_MODEL"
            ]
        }},
      ]
    }
  }
}

ES_QUERY = {
    "size":10000,
  "query": {
    "bool": {
      "must": [
        {"terms": {
          "fieldName.keyword": [
              "MAKE_YEAR_MODEL"
            ]
        }},
      ]
    }
  }
}

headers = {
    'Content-Type': 'application/json',
}


def get_make_year_model():
    field_count = base_url + '/customer_field/_count'
    field_url = base_url + '/customer_field/_search?scroll=10m'
    field_scroll = base_url + '/_search'
    total_docs = requests.get(url=field_count, json=ES_QUERY_COUNT, headers=headers).json()
    doc_count = total_docs["count"]
    resp = requests.get(url=field_scroll, json=ES_QUERY, headers=headers).json()
    reponse = resp['hits']['hits']
    return_resp = [doc["_source"] for doc in reponse]
    return return_resp


def get_page_url(id):
    QUERY = {
  "query": {
    "bool": {
      "must": [
        {"terms": {
          "id.keyword": [
            id
          ]
        }}
      ]
    }
  }
}

    doc_url = base_url + '/document_index/_search'
    resp = requests.get(url=doc_url, json=QUERY, headers=headers).json()
    all_docs = resp["hits"]["hits"]
    return_resp = [doc["_source"] for doc in all_docs]
    return return_resp



field_response = get_make_year_model()
make_year_model = []
doc_id = []
page_url = []

for doc in field_response:
    make_year_model.append(doc["correctedValue"])
    doc_id.append(doc["documentId"])

for id in doc_id:
    page_url_resp = get_page_url(id)
    page_url.append(page_url_resp[0]['pageUrl'])


zipped = list(zip(make_year_model, doc_id, page_url))


with open('make_year_model.csv', 'w') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['make_year_model', 'doc_id', 'pageUrl'])
    for row in zipped:
        csv_out.writerow(row)
# print(make_year_model)
# print(len(make_year_model))
# print(doc_id)
# print(len(doc_id))
# print(page_url)
# print(len(page_url))


