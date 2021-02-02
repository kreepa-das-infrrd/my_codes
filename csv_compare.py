import pandas as pd
import csv

df_exclude = pd.read_csv('/home/user/Downloads/SN_Single_entity_600_set-12-01-2021_17_30_59 - Sheet17.csv')
saved_column = list(df_exclude['Test_Filename'])

with open('resultINSURED_ADDRESS_CITY.csv', 'r') as t2:
    filetwo = csv.reader(t2)
    fields = next(filetwo)
    rows = []

    for row in filetwo:
        if row[0].strip() in saved_column:
            rows.append(row)

columns = ['file name', 'expected', 'extracted', 'status']

df = pd.DataFrame(rows, columns=columns)
print(df.to_csv("check.csv", index=False))


df_exclude = pd.read_csv('check.csv')
saved_column = list(df_exclude['file name'])

with open('/home/user/Downloads/SN_Single_entity_600_set-12-01-2021_17_30_59 - Sheet17.csv', 'r') as t2:
    filetwo = csv.reader(t2)
    fields = next(filetwo)
    rows = []

    for row in filetwo:
        if row[0].strip() in saved_column:
            rows.append(row)

columns = ['file name', 'expected', 'extracted']

df = pd.DataFrame(rows, columns=columns)
print(df.to_csv("check1.csv", index=False))