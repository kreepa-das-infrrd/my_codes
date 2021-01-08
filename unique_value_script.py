import pickle
import csv
from copy import deepcopy

BIN_FILE = 'zipped/RENEWAL/data_set_RENEWAL.bin'

report_data = dict()

if __name__ == '__main__':
    with open(BIN_FILE, "rb") as fp:
        data = pickle.load(fp)

    for file_data in data:
        raw_text, entities = file_data[0], file_data[1].get("entities")
        for start, end, field in entities:
            field_text = raw_text[start:end].lower()
            field_data = report_data.get(field, {})
            field_data[field_text] = field_data.get(field_text, 0) + 1
            report_data[field] = field_data

    # print(report_data)

    header = []
    for field in report_data.keys():
        header.extend([field, f"{field}_COUNT"])

    with open(f'renewal_unique_report.csv', 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()

        temp_data = deepcopy(report_data)
        max_column = max([len(list(v.keys())) for v in temp_data.values()])
        for i in range(max_column):
            row = {}
            for field in temp_data.keys():
                all_items = list(temp_data[field].items())
                if i < len(all_items):
                    value, count = all_items[i]
                    row[field] = value
                    row[f"{field}_COUNT"] = count
            writer.writerow(row)
