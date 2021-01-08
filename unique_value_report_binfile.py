import pickle
import csv
import pathlib
import pandas as pd

counter = 0

base_dir = 'bin_data/'

p = pathlib.Path(base_dir).glob('data_set_*.bin')
json_data = []
json_dict = {}

for bin_file in p:
    with open(f'{bin_file}', 'rb') as fp:
        data = pickle.load(fp)
        str_file = str(bin_file)
        csv_data = []
        val_list = []
        for doc in data:
            value = doc[1].get('entities')
            for tup in value:
                start = tup[0]
                end = tup[1]
                val_list.append(doc[0][start:end])
        val_set = set(val_list)
        frequency = []
        for val in val_set:
            val_frq = []
            val_frq.append(val)
            val_frq.append(val_list.count(val))
            frequency.append(val_frq)

        field_name = str_file.split('/')[-1].replace('data_set_', '').rsplit('.')[0]
        json_dict[field_name] = frequency
        json_data.append(json_dict)

        # if counter == 0:
        fields = [f'{field_name}', f'{field_name}_COUNT']
        with open(f'Unique_value_frequency_summary.csv', 'a') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)

            write.writerow(fields)
            write.writerows(frequency)

            counter += 1

        # else:
        #     df = pd.read_csv('Unique_value_frequency_summary.csv')
        #     field_name = str_file.split('/')[-1].replace('data_set_', '').rsplit('.')[0]
        #     fields = [f'{field_name}', f'{field_name}_COUNT']
        #     df[f'{fields[0]}'] = [f[0] for f in frequency]
        #     df[f'{fields[1]}'] = [f[1] for f in frequency]
        #     df.to_csv('Unique_value_frequency_summary.csv', index=False, mode='a')


