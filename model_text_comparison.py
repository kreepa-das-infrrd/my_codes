import os
import pickle
from pathlib import Path
import spacy
from tqdm import tqdm
import pandas as pd
bin_path = "bin_data/data_set_INSURED_ADDRESS_CITY.bin"
model_path = "models/INSURED_ADDRESS_CITY/extraction"
test_field = ["INSURED_ADDRESS_CITY"]
with open(bin_path, "rb") as file_:
    data_set = pickle.load(file_)
parsed = []
model_to_test = spacy.load(Path(model_path).absolute())
def pretty_values(list_):
    return " | ".join(list_) if len(list_) > 0 else ""
for text, entities, file_name in tqdm(data_set):
    expected_values = [text[val[0]: val[1]].replace("\n", "<newline>") for val in
                       sorted(entities["entities"], key=lambda x: x[0]) if
                       val[2] in test_field]
    extracted_values = [val.text.replace("\n", "<newline>") for val in model_to_test(text).ents if
                        val.label_ in test_field]
    if file_name[0] is None:
        file_name[0] = ""
    status = pretty_values(extracted_values) == pretty_values(expected_values)
    parsed.append((file_name[0], pretty_values(expected_values), pretty_values(extracted_values), str(status)))
# with open("results1.tsv", "w") as file_:
#     file_.writelines(["\t".join(val) + "\n" for val in parsed])
columns = ['file name', 'expected', 'extracted', 'status']

df = pd.DataFrame(parsed, columns=columns)
print(df.to_csv("resultINSURED_ADDRESS_CITY.csv", index=False))