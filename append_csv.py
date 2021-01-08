import pandas as pd
import pathlib

# base_dir = 'unique_value_folder/'
#
# p = pathlib.Path(base_dir).glob('*.csv')
#
unique_dir = "unique_csv_value_folder"
pathlib.Path(unique_dir).mkdir(parents=True, exist_ok=True)
#
# df = pd.read_csv('AGENT_PHONE_NUMBER_Unique_value_frequency_summary.csv')
# couter = 0
#
# for file in p:
#     if couter > 0:
#         df = pd.read_csv(f'{unique_dir}/unique_value_frequency.csv')
#     df2 = pd.read_csv(file)
#     df_new = df.join(df2, how='right')
#     df_new.to_csv(f'{unique_dir}/unique_value_frequency.csv', index=False)
#     couter += 1

df_unique = pd.read_csv(f'{unique_dir}/unique_value_frequency.csv')
print(df_unique.shape)

