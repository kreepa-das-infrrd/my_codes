"""
changing format of customer field index data

"""

import os
import re
import uuid
import json
import tqdm
import time
from copy import deepcopy
from itertools import groupby
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

ES_RESPONSE_SIZE = 10
VALID_TOKEN_PERCENTAGE = 80

SN_ES_IP = "54.184.160.83"
SN_ES_PORT = "31000"

# TITAN_PUBLIC_ES_IP = "44.229.105.51"
# TITAN_PRIVATE_ES_IP = "10.2.0.14"
# TITAN_ES_PORT = "32036"
# TITANS_USERNAME = 'krishnavatar'
# TITAN_PASSWORD = "Gtsevisdv G56VGers"

TITAN_PRIVATE_ES_IP = "localhost"
TITAN_ES_PORT = "9200"

INDEX_NAME = 'dev-gimlet-filter-scanrequest*'

HEADERS = {
    'Content-Type': 'application/json'
}

user_model_id = '895823ba-9fd3-4fbb-ae81-06a53b32a916'  # SN-PoC user model id
customer_id = '6cc6764f-ae80-40d1-87ec-4f7737057f2c'  # SN- accountID
time_format = "%Y-%m-%d %H:%M:%S"

VENDOR_TYPES_LIST = ["CANCELLATION", "INTENT TO CANCEL", "ACORD FORM", "BINDER", "DECLARATION", "ID CARD",
                     "RENEWAL", "CHANGE REQUEST", "AMENDMENT", "OTHER", "REINSTATEMENT", "APPLICATION"]

vendor_type = 'OTHER'

with open("data.json") as fp:
    SN_DATA = json.loads(fp.read())

with open("vehicle_model_values_v2.json") as fp:
    MAKE_YEAR_MODEL_DATA = json.loads(fp.read())

sn_es_client = Elasticsearch(hosts=SN_ES_IP, port=SN_ES_PORT)
# print(sn_es_client.info())


# if you want to index into titan comment local ES client else comment titan ES client
# To index into titan ES
# titan_es_client = Elasticsearch(hosts=TITAN_PRIVATE_ES_IP, port=TITAN_ES_PORT,
#                                 http_auth=(TITANS_USERNAME, TITAN_PASSWORD))

titan_es_client = Elasticsearch(hosts=TITAN_PRIVATE_ES_IP, port=TITAN_ES_PORT)


# to index in Local ES
# titan_es_client = Elasticsearch()


# script methods

def get_time(epoch_time):
    micro_sec = str(int(epoch_time) % 1000).zfill(3)
    return time.strftime(f"{time_format}:{micro_sec}", time.gmtime(int(epoch_time) / 1000.))


def create_document_data(document):
    document_id = f"{document.get('scanRequestId')}_document-1_split-1"
    file_ext = os.path.splitext(os.path.basename(document.get('s3ImagePath')))[-1]
    data = dict(userModelId=user_model_id, customerId=customer_id,
                requestId=document.get('scanRequestId'), id=document_id, status='TAGGED',
                fileName=os.path.basename(document.get('s3ImagePath')))

    data['createdDate'] = get_time(document.get('uploadDateTime'))
    data['lastModifiedDate'] = get_time(document.get('lastModifyDateTime'))
    data['correctedOn'] = get_time(document.get('lastModifyDateTime'))

    data['fileType'] = "pdf" if 'pdf' in file_ext.lower() else ""
    data['totalPages'] = len(document.get("images", []))
    data['docType'] = document.get("fields", {}).get('scenario', {}).get("value", 'NON-LIST')
    data['endPage'] = len(document.get("images", []))
    data['pageUrl'] = document.get('s3ImagePath')

    return data


def create_page_data(document, page_no, doc_data):
    page_id = f"{doc_data.get('id')}_page-{page_no}"
    page_data = dict(id=page_id, requestId=doc_data.get('requestId'), customerId=customer_id,
                     userModelId=user_model_id, parentDocumentId=doc_data.get("id"),
                     documentId=doc_data.get("id"), status='OCR_COMPLETED', pageNumber=page_no,
                     imageUrl=get_image_url(page_no, document.get('images', [])))

    page_data['createdDate'] = get_time(document.get('uploadDateTime'))
    page_data['lastModifiedDate'] = get_time(document.get('lastModifyDateTime'))

    return page_data


def create_ocr_data(ocr_tokens, page_text):
    ocr_data = {"pages": [], "rawText": page_text}
    ocr_tokens_data = [generate_token_data(token) for token in ocr_tokens]
    if ocr_tokens_data:
        max_x, max_y = max(ocr_tokens, key=lambda x: x['endX']), max(ocr_tokens, key=lambda x: x['endY'])
    else:
        max_x, max_y = {'endX': 2550}, {'endY': 3300}
    page_ocr_data = dict(startX=0, startY=0, tokens=ocr_tokens_data, text=page_text,
                         endX=2550 if max_x.get('endX') <= 2550 else int(max_x.get('endX')),
                         endY=3300 if max_y.get('endY') <= 3300 else int(max_y.get('endY')),
                         width=2550 if max_x.get('endX') <= 2550 else int(max_x.get('endX')),
                         height=3300 if max_y.get('endY') <= 3300 else int(max_y.get('endY'))
                         )
    ocr_data['pages'].append(page_ocr_data)
    return ocr_data


def generate_token_data(token):
    token_id = f"{int(token.get('pageNo'))}_" \
               f"{int(token.get('startX'))}_" \
               f"{int(token.get('startY'))}_" \
               f"{int(token.get('endX'))}_" \
               f"{int(token.get('endY'))}"
    return {
        "type": "token",
        "endY": int(token.get('endY')),
        "endX": int(token.get('endX')),
        "startIndex": int(token.get('startIndex')),
        "endIndex": int(token.get('endIndex')),
        "startY": int(token.get('startY')),
        "startX": int(token.get('startX')),
        "id": token_id,
        "text": token.get('word')
    }


def segregate_text(raw_texts):
    return [text.replace('\\n', '') for text in raw_texts.split('\n\f') if len(text) > 2]


def get_image_url(page_no, images):
    match = list(filter(lambda x: re.search(r'page-(\d)+', x).groups()[0] == str(page_no), images))
    if match:
        return match[0]


def segregate_ocr_tokens(ocr_tokens):
    return [(page_no, list(tokens)) for page_no, tokens in groupby(ocr_tokens, key=lambda x: x['pageNo'])]


def create_word_info(word_data):
    word_id = f"{int(word_data.get('pageNo'))}_" \
              f"{int(word_data.get('startX'))}_" \
              f"{int(word_data.get('startY'))}_" \
              f"{int(word_data.get('endX'))}" \
              f"_{int(word_data.get('endY'))}"
    return {
        "endY": int(word_data.get('endY')),
        "endX": int(word_data.get('endX')),
        "startY": int(word_data.get('startY')),
        "startX": int(word_data.get('startX')),
        "id": word_id,
        "pageNo": int(word_data.get('pageNo'))
    }


def generate_customer_field_data(key, data, idx):
    field_data, startIndex, endIndex, page_no = {}, None, None, None
    word_list = sorted(data['coordinates'] if data['coordinates'] else [], key=lambda x: x.get('startIndex'))
    if word_list:
        startIndex = int(word_list[0]['startIndex'])
        endIndex = int(word_list[-1]['endIndex'])
        page_no = int(word_list[0]['pageNo'])

    confidence = data['confidence']
    value = data['value']
    corrected_word_info = [create_word_info(word) for word in deepcopy(word_list)]

    multi_values = []
    collateral_dict = dict(startIndex=startIndex, endIndex=endIndex, confidence=confidence, pageNumber=page_no,
                           wordCoordinates=corrected_word_info, value=value, order=idx, id="")
    multi_values.append(collateral_dict)

    return dict(fieldType='Multi Value',
                values=multi_values,
                fieldName=key.upper()
                )


def add_additional_customer_field_data(annotation_data, doc_es_data, document_data):
    annotation_data['fieldId'] = str(uuid.uuid4())
    annotation_data['customerId'] = customer_id
    annotation_data['requestId'] = doc_es_data.get('requestId')
    annotation_data['userModelId'] = user_model_id
    annotation_data['documentId'] = doc_es_data.get('id')
    annotation_data['consumerId'] = document_data.get('userId')
    annotation_data['correctedBy'] = document_data.get('correctedBy')
    annotation_data['fileName'] = document_data.get('fileName')

    annotation_data['createdDate'] = get_time(document_data.get('uploadDateTime'))
    annotation_data['lastModifiedDate'] = get_time(document_data.get('lastModifyDateTime'))

    return annotation_data


def is_token_match(token, match_word):
    if token.lower() in match_word.lower() or match_word.lower() in token.lower():
        return True
    return False


def generate_and_split_make_year_model(data):
    new_keys = ['vehicle_year', 'vehicle_make', 'vehicle_model']
    word_list = deepcopy(sorted(data['coordinates'] if data['coordinates'] else [], key=lambda x: x.get('startIndex')))
    split_data = []
    value = data.get('value')
    if value:
        value = value.lower().strip()

    validate_data = MAKE_YEAR_MODEL_DATA.get(value)
    if validate_data:
        for key in new_keys:
            tokens = validate_data.get(key.replace("vehicle_", ''))
            if not tokens:
                continue

            new_word_info_list = []
            for token in tokens:
                match_tokens = sorted([word for word in word_list if is_token_match(word.get('word'), token)],
                                      key=lambda x: x.get('startIndex'))
                if match_tokens:
                    match_tokens.sort(key=lambda x: x.get('startIndex'))
                    new_word_info_list.append(match_tokens[0])
                    word_list.remove(match_tokens[0])

            new_word_info_list.sort(key=lambda x: x.get('startIndex'))
            startIndex = int(new_word_info_list[0]['startIndex']) if new_word_info_list else 0
            endIndex = int(new_word_info_list[-1]['endIndex']) if new_word_info_list else 0
            page_no = int(new_word_info_list[0]['pageNo']) if new_word_info_list else 1

            field_data = dict(correctedWordInfoList=new_word_info_list, fieldType='Single Value',
                              confidence=1.0, correctedValue=" ".join(tokens),
                              correctedStartIndex=startIndex, correctedEndIndex=endIndex,
                              pageNumber=page_no, fieldName=key.upper(), vendorType=vendor_type)
            split_data.append(field_data)

    return split_data


def create_annotation_data(annotations, annotation_values, idx=0, key_prefix=''):
    make_year_model_found = False
    vehicle_list = ['VEHICLE_YEAR', 'VEHICLE_MAKE', 'VEHICLE_MODEL']
    found_list = []
    for key, fields_data in annotation_values.items():
        if make_year_model_found and key.upper() in vehicle_list and key.upper() not in found_list:
            continue

        if key == 'make_year_model' and fields_data.get('value'):
            split_data = generate_and_split_make_year_model(fields_data)
            if split_data:
                for found_field in [f.get('fieldName') for f in split_data]:
                    found_list.append(found_field)
                make_year_model_found = True
                create_multi_value_annotation_data(annotations, split_data, idx)
        elif fields_data and isinstance(fields_data['value'], dict):
            create_annotation_data(annotations, fields_data['value'], key_prefix=key)
        elif fields_data and isinstance(fields_data['value'], list):
            for annotation_data in fields_data['value']:
                create_annotation_data(annotations, annotation_data)
        elif fields_data and fields_data['value']:
            new_key = key if key_prefix == '' else f'{key_prefix}_{key}'
            field_dict = generate_customer_field_data(new_key, fields_data, idx)
            create_multi_value_annotation_data(annotations, field_dict, idx, key=new_key)


def create_multi_value_annotation_data(annotations, data_dict, idx, key=None):
    if idx == 0:
        if isinstance(data_dict, list):
            annotations.extend(data_dict)
        else:
            annotations.append(data_dict)
    else:
        required_dict = next(item for item in annotations if item["fieldName"] == key.upper())
        required_dict["values"].extend(data_dict["values"])


def get_all_annotation_document():
    documents = None

    # read from JSON file
    if os.path.exists("SN_data.json"):
        with open("SN_data.json") as fp:
            documents = json.loads(fp.read())

    if not documents:
        # all_user_ids = get_user_ids()
        # all_scan_ids = SN_DATA.get("RELEASE_2_SCAN_IDS") + SN_DATA.get("RELEASE_1_SCAN_IDS") + SN_DATA.get("RELEASE_3_SCAN_IDS")
        # all_scan_ids = SN_DATA.get("TEST_SCAN_IDS")
        all_scan_ids = ["f7f71a50-0c9c-11eb-b3c4-1b00f897afec"]

        query = Q('bool', must=[Q('term', isCorrected=True),
                                Q('term', **{"fields.scenario.value.keyword": "NON-LIST"}),
                                Q('terms', **{"scanRequestId.keyword": all_scan_ids})])

        s = Search(using=sn_es_client, index=INDEX_NAME, doc_type='docs')
        s = s.query(query).params(size=ES_RESPONSE_SIZE)
        # response = s.execute().to_dict()
        # documents = [dict({"id": doc["_id"]}, **doc['_source']) for doc in response['hits']['hits']]

        response = [(hit.meta.id, hit.to_dict()) for hit in s.scan()]
        documents = [dict({"id": _id}, **doc) for _id, doc in response]

    # with open("SN_data.json", "w") as fp:
    #     fp.write(json.dumps(documents))
    print(f"Total {len(documents)} document found in SN-ES DB")
    documents.sort(key=lambda x: x['uploadDateTime'])
    return documents


def index_data(index_name, body, doc_id):
    response = titan_es_client.index(index=index_name, doc_type='_doc',
                                     body=body, id=doc_id)
    print(f"document is created with id {response['_id']}")


def check_if_doc_exist(index_name, doc_id):
    return titan_es_client.exists(index=index_name, doc_type="_doc", id=doc_id)


def get_vendor_type_by_id(scan_id):
    return {
        "e5d19735-29cb-11eb-a46b-f9ae32144f3e": "REINSTATEMENT",
        "e25c5ad0-3593-11eb-b3c4-3b09b59d9193": "DECLARATION",
        "02ff0642-34d8-11eb-b3c4-1141615a741f": "DECLARATION",
        "a3fc2de4-69c8-11ea-9497-c1aaebe89a9b": "CANCELLATION",
        "3e3e139a-6f4a-11ea-bf75-bd22650821f1": "CANCELLATION",
        "1b51cc64-6f4a-11ea-bf75-117ac52fd1ee": "INTENT TO CANCEL",
        "7d70ae1d-2575-11eb-9847-1166d3896afd": "CANCELLATION",
        "c265df48-f8b0-11ea-8478-97b2432b7553": "INTENT TO CANCEL",
        "a3bdb4ac-14a5-11eb-b3c4-ef40c05da954": "CANCELLATION",
        "576b550b-14ff-11eb-b3c4-19fb84b287f6": "INTENT TO CANCEL",
        "ff070c58-14ff-11eb-844d-831205aef82f": "INTENT TO CANCEL"
    }.get(scan_id)


def get_vendor_type(vendor_name):
    vendor_type_mappings = {
        "CANCELLATION": ["Personal Auto Policy Cancellation Notice", "CANCELLATION CUSTOMER REQUEST", "CELLATION",
                         "NOTICE OF CANCELLATION OR REFUSAL TO RENEW"],
        "BINDER": ["APPLICATION BINDER"],
        "CHANGE REQUEST": ["POLICY CHANGE", "POLICY CHANGE DECLARATIONS"],
        "RENEWAL": ["RENEWAL DECLARATIONS", "RENEWAL DECLARATIONS (CONTINUED)", "RENEWAL NOTICE"],
        "DECLARATION": ["DECLARATIONS", "INSURANCE COVERAGE NOTIFICATION(S)", "CERTIFICATE OF INSURANCE",
                        "COVERAGE SUMMARY", "EVIDENCE OF INSURANCE FOR MORTGAGE COMPANIES",
                        "NOTICE OF INSURANCE COVERAGE", "EVIDENCE OF PROPERTY INSURANCE",
                        "VEHICLE OR EQUIPMENT CERTIFICATE OF INSURANCE"],
        "AMENDMENT": ["ENDORSEMENT"],
        "REINSTATEMENT": ["NOTICE OF REINSTATEMENT"]
    }

    for vendor_type, synon_list in vendor_type_mappings.items():
        if any([v_type for v_type in synon_list if vendor_name.lower() in v_type.lower()]):
            return vendor_type

    return vendor_name


def generate_titan_annotation_data_for_document(document_json):
    global vendor_type

    doc_data = create_document_data(document_json)
    all_ocr_tokens = segregate_ocr_tokens(document_json.get('ocrCoordinates'))
    all_texts = segregate_text(document_json.get("rawText"))
    all_found_page_nos = [page_no for page_no, _ in all_ocr_tokens]
    page_data_generator_obj = page_data_generator(all_ocr_tokens, all_texts)
    # all_ocr_tokens = [(ocr_tokens[0], ocr_tokens[1], text) for ocr_tokens, text in zip(all_ocr_tokens, all_texts)]
    all_ocr_tokens = [next(page_data_generator_obj) if page_inx in all_found_page_nos else (page_inx, [], '')
                      for page_inx in range(1, max(all_found_page_nos) + 1)]
    document_pages_data = []
    for page_no, ocr_tokens, text in all_ocr_tokens:
        page_data = create_page_data(document_json, page_no, doc_data)
        page_data['ocrData'] = create_ocr_data(ocr_tokens, text)
        document_pages_data.append(page_data)

    annotation_values = document_json['fields']['collaterals']['value']
    annotations = []
    for idx, values in enumerate(annotation_values):
        vendor_type = get_vendor_type_by_id(document_json["scanRequestId"])
        if not vendor_type:
            vendor_type = values.get('document_type', {}).get('value')
        if vendor_type and vendor_type not in VENDOR_TYPES_LIST:
            vendor_type = get_vendor_type(vendor_type)
        elif not vendor_type:
            vendor_type = 'OTHER'

        create_annotation_data(annotations, values, idx=idx)

    annotations = [add_additional_customer_field_data(ann_data, doc_data, document_json)
                   for ann_data in annotations]

    doc_data['taggedFields'] = list(set([ann_data.get('fieldName') for ann_data in annotations]))
    return doc_data, document_pages_data, annotations


def check_all_word_have_indexes(ocr_tokens):
    total_tokens = len(ocr_tokens)
    if total_tokens < 1:
        return False

    valid_tokens = [token for token in ocr_tokens if token.get('startIndex') and token.get('endIndex')]
    return (len(valid_tokens) / total_tokens) * 100 > VALID_TOKEN_PERCENTAGE


def bulk_indexing(bulk_requests, index):
    response = helpers.bulk(titan_es_client, bulk_requests, stats_only=True)
    if response[1] > 0:
        query = Q("term", **{"_id": [data.get("_id") for data in bulk_requests]})
        s = Search(using=titan_es_client, index=index, doc_type="_doc")
        s = s.query(query).params(size=10000).source("_id")
        res = s.execute().to_dict()
        indexed_ids = [doc.get("_id") for doc in res['hits']['hits']]
        failed_documents = [req for req in bulk_requests if req.get("_id") not in indexed_ids]
        bulk_indexing(failed_documents, index)

    else:
        return response


def index_bulk_data(index_name, bulk_data, id_key='id'):
    bulk_index_data = [dict(_op_type="update", _index=index_name, _id=data[id_key],
                            _type="_doc", doc_as_upsert=True, doc=data)
                       for data in bulk_data]
    for chunk_data in get_data_buffer(bulk_index_data):
        bulk_indexing(chunk_data, index_name)


def get_data_buffer(bulk_data, buffer_size=1000):
    for start in range(0, len(bulk_data), buffer_size):
        yield bulk_data[start: start + buffer_size]


def page_data_generator(all_ocr_tokens, raw_texts):
    for ocr_tokens, raw_text in zip(all_ocr_tokens, raw_texts):
        yield ocr_tokens[0], ocr_tokens[1], raw_text


def main():
    global vendor_type

    all_annotation_document = get_all_annotation_document()
    titan_document_index_data = []
    titan_page_index_data = []
    titan_customer_field_index_data = []
    fail_doc = []
    for es_document_json in tqdm.tqdm(all_annotation_document):
        vendor_type = 'OTHER'

        doc_id = f"{es_document_json.get('scanRequestId')}_document-1"
        # if doc_id != '88b07a61-34d4-11eb-a46b-9936b1af3d45_document-1':
        #     continue

        doc_indexed_flag = check_if_doc_exist('document_index', doc_id)
        # doc_indexed_flag = False
        word_index_flag = check_all_word_have_indexes(es_document_json.get('ocrCoordinates'))

        if word_index_flag:
            if not doc_indexed_flag:
                document_json, pages_data, annotations_data = generate_titan_annotation_data_for_document(
                    es_document_json)

                titan_document_index_data.append(document_json)
                titan_page_index_data.extend(pages_data)
                titan_customer_field_index_data.extend(annotations_data)

        else:
            fail_doc.append(es_document_json.get('scanRequestId'))

    print(f"Indexing start for document_index ........ Total document -> {len(titan_document_index_data)}")
    index_bulk_data('document_index', titan_document_index_data)
    print("indexing done")

    print(f"Indexing start for page_index ........ Total document -> {len(titan_page_index_data)}")
    index_bulk_data('page_index', titan_page_index_data)
    print("indexing done")

    print(
        f"Indexing start for customer field ......Total document -> {len(titan_customer_field_index_data)}")
    index_bulk_data('customer_field', titan_customer_field_index_data, id_key='fieldId')
    print("indexing done")

    print(f"Total indexing failed doc count {len(fail_doc)}")


if __name__ == '__main__':
    main()

