"""
modified the report part add the dir constants
"""

import base64
import json
import os
import pathlib
from copy import deepcopy
from itertools import combinations, groupby
import pickle

import requests
from natsort import natsorted
from pyjarowinkler import distance

GENERATE_REPORT = True
INCLUDE_INVALID_ENTS = False

MATCH_THRESHOLD = 0.8
CHAR_DIFF_BUFFER = 5

TITAN_PUBLIC_ES_IP = "44.229.105.51"
TITAN_PRIVATE_ES_IP = "10.2.0.14"
TITAN_ES_PORT = "32036"
TITANS_USERNAME = 'krishnavatar'
TITAN_PASSWORD = "Gtsevisdv G56VGers"

BIN_DIR = './bin_data'
DATA_DIR = './data'
REPORT_DIR = './report'
pathlib.Path(BIN_DIR).mkdir(parents=True, exist_ok=True)
pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
pathlib.Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

# UAT
base_url = 'http://localhost:9200'

# base_url = f'http://{TITAN_PUBLIC_ES_IP}:{TITAN_ES_PORT}'
auth = TITANS_USERNAME + ':' + TITAN_PASSWORD

auth = base64.b64encode(auth.encode()).decode()
# connection = "http://{}@{}".format(auth, ip_port)
# connections.create_connection(hosts=[connection])
headers = {
    'Content-Type': 'application/json'
}


# Get all the tagged documents for the list of user models
def scroll_documents(user_model_ids, field, is_include_extracted):
    document_scroll_id_url = base_url + "/document_index/_search?scroll=10m"
    documents_count_url = base_url + "/document_index/_count"
    document_scroll_url = base_url + "/_search/scroll"
    documents_count_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "userModelId.keyword": user_model_ids
                        }
                    },
                    {
                        "term": {
                            "status.keyword": {
                                "value": "TAGGED"
                            }
                        }
                    },
                    {
                        "term": {
                            "taggedFields.keyword": {
                                "value": field
                            }
                        }
                    }
                ]
            }
        }
    }
    documents_query = {
        "_source": [
            "id"
        ],
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "userModelId.keyword": user_model_ids
                        }
                    },
                    {
                        "term": {
                            "status.keyword": {
                                "value": "TAGGED"
                            }
                        }
                    },
                    {
                        "term": {
                            "taggedFields.keyword": {
                                "value": field
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "id.keyword": {
                    "order": "desc"
                }
            }
        ]
    }
    documents_count_resp = requests.get(url=documents_count_url, json=documents_count_query, headers=headers).json()
    print(documents_count_resp)
    documents_count = documents_count_resp["count"]
    print("Total documents to fetch: ", documents_count)
    scroll_resp = requests.get(url=document_scroll_id_url, json=documents_query, headers=headers).json()
    all_docs = scroll_resp["hits"]["hits"]
    scroll_id = scroll_resp["_scroll_id"]
    document_scroll_query = {
        "scroll": "10m",
        "scroll_id": scroll_id
    }
    while scroll_resp["hits"]["hits"]:
        print("remaining pages: ", documents_count - len(all_docs))
        scroll_resp = requests.get(url=document_scroll_url, json=document_scroll_query, headers=headers).json()
        all_docs += scroll_resp["hits"]["hits"]
    return_resp = [doc["_source"] for doc in all_docs]
    return return_resp


# get all the pages of document_ids with their ocrData from page_index
def scroll_all_pages(document_ids):
    # print(document_ids)
    page_scroll_id_url = base_url + "/page_index/_search?scroll=10m"
    page_search_count_url = base_url + "/page_index/_count"
    page_scroll_url = base_url + "/_search/scroll"
    page_query = {
        "size": 3000,
        "query": {
            "bool": {
                "must": [
                    {"terms": {
                        "documentId.keyword": document_ids
                    }}
                ]
            }
        },
        "sort": [
            {
                "documentId.keyword": {
                    "order": "desc"
                }
            }
        ]
    }
    page_count_query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {
                        "documentId.keyword": document_ids
                    }}
                ]
            }
        }
    }
    count_resp = requests.get(url=page_search_count_url, json=page_count_query, headers=headers).json()
    print("Page count resp: ", count_resp)
    scroll_resp = requests.get(url=page_scroll_id_url, json=page_query, headers=headers).json()
    # print(scroll_resp)
    all_pages = scroll_resp["hits"]["hits"]
    scroll_id = scroll_resp["_scroll_id"]
    page_scroll_query = {
        "scroll": "10m",
        "scroll_id": scroll_id
    }
    while scroll_resp["hits"]["hits"]:
        print("remaining pages: ", count_resp["count"] - len(all_pages))
        scroll_resp = requests.get(url=page_scroll_url, json=page_scroll_query, headers=headers).json()
        all_pages += scroll_resp["hits"]["hits"]
        # print(json.dumps(scroll_resp))
    return_resp = [doc["_source"] for doc in all_pages]
    return return_resp


# get corrected values for doc_ids and fields from the customer_field index
def scroll_customer_field(doc_ids, fields, include_extracted=False):
    field_index_id_url = base_url + "/customer_field/_search?scroll=10m"
    field_index_count_url = base_url + "/customer_field/_count"
    field_scroll_url = base_url + "/_search/scroll"
    corrected_field_query = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "documentId.keyword": doc_ids
                        }
                    },
                    {
                        "terms": {
                            "fieldName.keyword": fields
                        }
                    },
                    {
                        "exists": {
                            "field": "correctedStartIndex"
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "documentId.keyword": {
                    "order": "desc"
                }
            }
        ]
    }
    corrected_field_count_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "documentId.keyword": doc_ids
                        }
                    },
                    {
                        "terms": {
                            "fieldName.keyword": fields
                        }
                    },
                    {
                        "exists": {
                            "field": "correctedStartIndex"
                        }
                    }
                ]
            }
        }
    }
    count_resp = requests.get(url=field_index_count_url, json=corrected_field_count_query, headers=headers).json()
    print("Corrected fields to be fetched: ", count_resp["count"])
    scroll_resp = requests.get(url=field_index_id_url, json=corrected_field_query, headers=headers).json()
    all_corected_values = scroll_resp["hits"]["hits"]
    scroll_id = scroll_resp["_scroll_id"]
    field_scroll_query = {
        "scroll": "10m",
        "scroll_id": scroll_id
    }
    while scroll_resp["hits"]["hits"]:
        print("remaining corrected values: ", count_resp["count"] - len(all_corected_values))
        scroll_resp = requests.get(url=field_scroll_url, json=field_scroll_query, headers=headers).json()
        all_corected_values += scroll_resp["hits"]["hits"]
    return_resp = [doc["_source"] for doc in all_corected_values]
    return return_resp


# merge tokens if they have nothing but space between them else treat them as different occurrence
def merge_ents(raw_text, local_ents):
    new_ents = []

    # ORDER
    # startI, endI, label, parent, token, sx, sy, ex, ey
    for ent_combo in combinations(local_ents, 2):
        if ent_combo[0][2] == ent_combo[1][2]:
            ent1, ent2 = ent_combo if ent_combo[0][0] < ent_combo[1][0] else (ent_combo[1], ent_combo[0])

            text_inter = raw_text[int(ent1[1]):int(ent2[0])]
            tokens_len = len(raw_text[ent1[0]:ent1[1]].replace(" ", "")) + \
                         len(raw_text[ent2[0]:ent2[1]].replace(" ", ""))

            tagged_text_len = len(raw_text[ent1[0]:ent2[1]].replace(" ", ""))

            if tokens_len == tagged_text_len:
                return merge_ents(raw_text, [(ent1[0], ent2[1], ent1[2], ent1[3], ent1[4], ent2[5], ent2[6])] +
                                  [en for en in local_ents if en not in [ent1, ent2]])

            elif len(text_inter.strip(" ")) == 0 and 10 < len(text_inter) >= 50:
                print(ent1[2], ":", raw_text[int(ent1[0]):int(ent2[1])])
                print()
    f_ents = local_ents if len(new_ents) == 0 else new_ents
    return [(ent[0], ent[1], ent[2], ent[3], ent[4], ent[5], ent[6], raw_text[int(ent[0]): int(ent[1])])
            for ent in f_ents]


def get_bin_file(user_model_ids, field_name, is_multi_value=False, is_include_extracted=False):
    print(f"field: {field_name}, field_type: {is_multi_value}")

    # create the directories
    pathlib.Path(BIN_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    # get all document ids for data
    if not os.path.exists(f"{DATA_DIR}/doc_ids_{field_name}.json"):
        doc_ids = scroll_documents(user_model_ids, field_name, is_include_extracted)
        with open(f"{DATA_DIR}/doc_ids_{field_name}.json", "w") as fp:
            json.dump(doc_ids, fp, indent=2)
    else:
        with open(f"{DATA_DIR}/doc_ids_{field_name}.json", "r") as fp:
            doc_ids = json.load(fp)
    doc_ids = natsorted([doc["id"] for doc in doc_ids])

    # get all pages for document ids
    if os.path.exists(f"{DATA_DIR}/pages_{field_name}.json"):
        with open(f"{DATA_DIR}/pages_{field_name}.json", "r") as fp:
            data = fp.read()
            pages = json.loads(data)
        print("total num of pages: ", len(pages))
    else:
        pages = scroll_all_pages(doc_ids)

    # get all doc ids for fail pages
    ocr_failed_doc_ids = [page["documentId"] for page in pages
                          if "ocrData" not in page or "rawText" not in page["ocrData"]]
    ocr_failed_doc_ids = list(set(ocr_failed_doc_ids))

    # remove all failed doc ids from list
    doc_ids = [doc_id for doc_id in doc_ids if doc_id not in ocr_failed_doc_ids]

    if not os.path.exists(f"{DATA_DIR}/pages_{field_name}.json"):
        with open(f"{DATA_DIR}/pages_{field_name}.json", "w") as fp:
            fp.write(json.dumps(pages))

    if not os.path.exists(f"{DATA_DIR}/corrected_values_{field_name}.json"):
        corrected_values = scroll_customer_field(doc_ids, [field_name], is_include_extracted)
        with open(f"{DATA_DIR}/corrected_values_{field_name}.json", "w") as fp:
            fp.write(json.dumps(corrected_values))
    else:
        with open(f"{DATA_DIR}/corrected_values_{field_name}.json", "r") as fp:
            corrected_values = json.load(fp)
    spacy_data = []

    # create correction by document id
    correction_groups = groupby(corrected_values, key=lambda x: x['documentId'])
    entities_report = {"report_data": {}}
    invalid_documents_ids = []
    pages_missing_doc_ids = []

    for key, correction_group in correction_groups:

        # to debug a particular document ID
        # if key != "5786a424-351c-11eb-a46b-a13ff74a52d4_document-1":
        #     continue

        entity_list = []
        file_names = []
        corrected_doc_id = key
        corrected_document_pages = {page.get("id"): page for page in pages if corrected_doc_id == page["documentId"]}
        # has_all_pages = check_has_all_pages([int(page_id.split('_page-')[-1]) for page_id in
        #                                      corrected_document_pages.keys()])
        # if not has_all_pages:
        #     pages_missing_doc_ids.append(key)
        #     continue

        raw_text_list_in_order = [page["ocrData"]["rawText"] for page in
                                  natsorted(corrected_document_pages.values(), key=lambda l: l["pageNumber"])]
        raw_text_list_len_in_order = [len(raw_text) for raw_text in raw_text_list_in_order]

        # loop each corrections
        validation_data = []
        for corrected_value in list(correction_group):
            corrected_word_info_list = corrected_value.get("correctedWordInfoList")
            corrected_value_str = corrected_value.get("correctedValue")
            corrected_page_no = corrected_value["pageNumber"]
            vendor_type = corrected_value["vendorType"]
            fileName = corrected_value["fileName"]
            corrected_token_ids = []
            # print("First corrected word info list: ", corrected_word_info_list)
            if corrected_word_info_list:
                corrected_token_ids = list(set([token.get("id") for token in corrected_word_info_list]))

            corrected_page_id = corrected_doc_id + "_page-" + str(corrected_page_no)
            adding_index = sum(raw_text_list_len_in_order[:int(corrected_page_no) - 1]) + corrected_page_no - 1
            # print("adding index", adding_index)
            corrected_word_info_list = []
            page_data = corrected_document_pages.get(corrected_page_id)
            if page_data:
                match_tokens = [token for token in page_data["ocrData"]["pages"][0]["tokens"]
                                if token["id"] in corrected_token_ids]
                # Not change the original token
                temp_tokens = deepcopy(match_tokens)
                for temp_token in temp_tokens:
                    temp_token["startIndex"] = temp_token["startIndex"] + adding_index
                    temp_token["endIndex"] = temp_token["endIndex"] + adding_index + 1
                    corrected_word_info_list.append(temp_token)
            corrected_word_info_list = natsorted(corrected_word_info_list, key=lambda l: l["startIndex"])

            # currently not using this if condition will need to modified script accordingly
            if is_multi_value:
                ents = []
                validation_ent_data = []
                # startI, endI, label, parent, token, sx, sy, ex, ey
                try:
                    merge_ents_list = [(ent["startIndex"], ent["endIndex"], corrected_value["fieldName"], ent["startX"],
                                        ent["startY"], ent["endX"], ent["endY"]) for ent in corrected_word_info_list]
                except BaseException as e:
                    print(e)
                    exit(1)
                spacy_ents = merge_ents("\n".join(raw_text_list_in_order), merge_ents_list)
                for spacy_ent in spacy_ents:
                    ents.append((spacy_ent[0], spacy_ent[1], spacy_ent[2]))
                    validation_ent_data.append((spacy_ent[0], spacy_ent[1], spacy_ent[2],
                                                spacy_ent[-1]))

            else:
                if corrected_word_info_list:
                    ents = [(corrected_word_info_list[0]["startIndex"], corrected_word_info_list[-1]["endIndex"],
                             corrected_value["fieldName"])]
                    filename = [(fileName)]
                    validation_ent_data = [(ents[0][0], ents[0][1], ents[0][2], corrected_value_str, vendor_type)]

                else:
                    start_index = corrected_value["correctedStartIndex"] + adding_index
                    end_index = corrected_value["correctedEndIndex"] + adding_index + 1
                    ents = [(start_index, end_index, corrected_value["fieldName"])]
                    filename = [(fileName)]
                    validation_ent_data = [(ents[0][0], ents[0][1], ents[0][2], corrected_value_str, vendor_type)]

            validation_data.extend(validation_ent_data)
            entity_list.extend(ents)
            file_names.extend(filename)

        # remove duplicate ents
        entity_list, validation_data = list(set(entity_list)), list(set(validation_data))
        report_response = check_entities("\n".join(raw_text_list_in_order), validation_data, file_names)
        if not INCLUDE_INVALID_ENTS:
            # sort the entries on start index
            entity_list = natsorted(entity_list, key=lambda x: x[0])
            report_response = natsorted(report_response, key=lambda x: x.get('start'))
            # to remove invalid entries
            entity_list = [ent for ent, report_ent in zip(entity_list, report_response) if not report_ent.get(
                "is_invalid_entity")]

        if entity_list:
            spacy_tup = ("\n".join(raw_text_list_in_order), {"entities": entity_list}, file_names)
            spacy_data.append(spacy_tup)
        else:
            invalid_documents_ids.append(key)

        entities_report['report_data'][key] = report_response

    with open(f"{BIN_DIR}/data_set_{field_name}.bin", "wb") as fp:
        print(f"Total {len(spacy_data)} document found for {field_name}")
        pickle.dump(spacy_data, fp)

    if GENERATE_REPORT:
        pathlib.Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

        with open(f"{REPORT_DIR}/doc_ids_missing_pages_{field_name}.json", "w") as fp:
            json.dump(pages_missing_doc_ids, fp, indent=2)

        with open(f"{REPORT_DIR}/invalid_doc_ids_{field_name}.json", "w") as fp:
            json.dump(invalid_documents_ids, fp, indent=2)

        with open(f"{REPORT_DIR}/report_{field_name}.json", "w") as fp:
            total_entities = [v for values in list(entities_report['report_data'].values()) for v in values]
            if total_entities:
                entities_report['total_valid_doc_count'] = len(spacy_data)
                entities_report['total_invalid_doc_count'] = len(invalid_documents_ids)
                entities_report['total_missing_page_doc_count'] = len(pages_missing_doc_ids)

                invalid_entities = [v for v in total_entities if v['is_invalid_entity']]
                per_invalid_entities = (len(invalid_entities) / len(total_entities)) * 100
                entities_report['total_entities'] = len(total_entities)
                entities_report['invalid_entities'] = len(invalid_entities)
                entities_report['invalid_per'] = per_invalid_entities
                entities_report['valid_entities'] = len(total_entities) - len(invalid_entities)

                # similarity report
                unsimilar_entities = [v for v in total_entities if v['is_unsimilar']]
                per_unsimilar_entities = (len(unsimilar_entities) / len(total_entities)) * 100
                entities_report['unsimilar_entities'] = len(unsimilar_entities)
                entities_report['unsimilar_entities_per'] = per_unsimilar_entities
                entities_report['similar_entities'] = len(total_entities) - len(unsimilar_entities)

            fp.write(json.dumps(entities_report, indent=2))

    return f"{BIN_DIR}/data_set_{field_name}.bin", f"{REPORT_DIR}/report_{field_name}.json"


def check_entities(raw_text, entities, file_names):
    report = []
    for start, end, label, text, vendor in entities:
        train_text, text = raw_text[start: end], text.strip()
        total_length = end - start
        try:
            match_per = distance.get_jaro_distance(" ".join(text.split()), " ".join(train_text.split()))
        except:
            match_per = 0.0
        is_invalid = text.lower() != train_text.lower() or len(train_text) != len(text)
        data = dict(total_lenght=total_length,
                    text_lenght=len(text),
                    train_text_lenght=len(train_text),
                    length_diff=len(text) - len(train_text),
                    text=text, train_text=train_text,
                    is_invalid_entity=is_invalid,
                    start=start, end=end,
                    similarity_match=match_per,
                    is_unsimilar=False if match_per >= MATCH_THRESHOLD else True,
                    label=label,
                    doc_type=vendor,
                    file_names=file_names
                    )
        report.append(data)
    return report


def check_has_all_pages(page_nums):
    return len(page_nums) == page_nums[-1]


if __name__ == "__main__":
    print("Starting...")
    # user model ids need to be populated in user models
    user_models = ["895823ba-9fd3-4fbb-ae81-06a53b32a916"]
    name_of_field = ["INSURED_ADDRESS_ZIP_CODE", "INSURED_ADDRESS_CITY"]
    is_multi_field = False
    for field in name_of_field:
        print(get_bin_file(user_models, field, is_multi_field))
        print("Finished")
