import json
import os
import glob
import pandas as pd

base_dir = 'report'

all_report_files = glob.glob(f'{base_dir}/report*.json')

summary = []
for f_path in all_report_files:
    print(f_path)
    try:
        with open(str(f_path), "r") as fp:
            doc_ids = json.load(fp)
        report_entity = os.path.splitext(os.path.basename(f_path))[0].replace("report_", "")

        total_valid_doc_count = doc_ids['total_valid_doc_count']
        total_invalid_doc_count = doc_ids['total_invalid_doc_count']
        # total_missing_page_doc_count = doc_ids['total_missing_page_doc_count']

        total_entities = doc_ids['total_entities']
        valid_entities = doc_ids['valid_entities']
        invalid_entities = doc_ids['invalid_entities']
        invalid_per = doc_ids['invalid_per']

        similar_entities = doc_ids['similar_entities']
        unsimilar_entities = doc_ids['unsimilar_entities']
        unsimilar_entities_per = doc_ids['unsimilar_entities_per']

        temp = [report_entity, total_valid_doc_count, total_invalid_doc_count, total_entities, valid_entities,
                invalid_entities, invalid_per, similar_entities, unsimilar_entities, unsimilar_entities_per]
        summary.append(temp)
    except:
        pass

columns = ['report_entity', 'total_valid_doc_count', 'total_invalid_doc_count', 'total_entities', 'valid_entities',
           'invalid_entities', 'invalid_per', 'similar_entities', 'unsimilar_entities', 'unsimilar_entities_per']

summary.sort(key=lambda x: x[3], reverse=True)
df = pd.DataFrame(summary, columns=columns)
print(df.to_csv("summary.csv"))
