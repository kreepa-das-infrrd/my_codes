import pickle
import csv
from copy import deepcopy

BIN_FILE = 'zipped/RENEWAL/data_set_RENEWAL.bin'

report_data = dict()

if __name__ == '__main__':
    with open('summary.csv', "r") as fp:
        data = csv.reader(fp)
        fields = next(data)

    for file_data in data:
        field_name, value, vendor = file_data[0], file_data[1], file_data[2]
        field_data = {}
        field_data = field_data.get(field_name, {})
        field_data[value] = field_data.get(value, 0) + 1
        field_data['vendor'] = vendor
        report_data[field_name] = field_data

    # print(report_data)

    header = []
    for field in report_data.keys():
        header.extend([field, f"{field}_COUNT", f"{field}_vendor"])

    with open(f'summary_field.csv', 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()

        temp_data = deepcopy(report_data)
        max_column = max([len(list(v.keys())) for v in temp_data.values()])
        for i in range(max_column):
            row = {}
            for field in temp_data.keys():
                all_items = list(temp_data[field].items())
                if i < len(all_items):
                    value, count, vendor = all_items[i]
                    row[field] = value
                    row[f"{field}_COUNT"] = count
                    row[f"{field}_vendor"] = vendor
            writer.writerow(row)
