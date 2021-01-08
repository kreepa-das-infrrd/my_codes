import pandas as pd
import csv

df = pd.read_csv('correct_doc_type_mapping.csv')
wrng_doc_type = list(df['SAVED'])
original_doc_type = list(df['ORIGINAL'])

zipped = zip(wrng_doc_type, original_doc_type)

mapping_dict = {i[0]:i[1] for i in zipped}

with open('correct_release1_test_set - correct_release1_test_set.csv', 'r') as fp:
    csvreader = csv.reader(fp)
    fields = next(csvreader)

    rows = []

    for row in csvreader:
        if row[2] in mapping_dict.keys():
            row[2] = mapping_dict[row[2]]
            rows.append(row)
        else:
            rows.append(row)

with open('correct_release1_test_doc_type_mapping.csv', 'w') as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)

    # writing the fields
    csvwriter.writerow(fields)

    # writing the data rows
    csvwriter.writerows(rows)