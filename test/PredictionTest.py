import os
import pandas as pd
from datetime import datetime

from mlkit.Prediction import Model, DataLoader
import pickle
from util.Processor import api_to_df, process_data_for_db_insert, insert_on_exchange_db

# path = r'C:\Users\bechitra\PycharmProjects\CurrencyExchangeApi\res\csv'
# loader = DataLoader(data_path=path)
# loader.load()
# df_map = loader.get_data_dictionary()
#
# model = Model(df_map=df_map)
# model.train(limit=180)
# result = model.get_result_dictionary()
# r_path = r'C:\Users\bechitra\PycharmProjects\CurrencyExchangeApi\mlkit'
# loader.dump(result=result, path=r_path, filename='generated')

# r_path = r'C:\Users\bechitra\PycharmProjects\CurrencyExchangeApi\mlkit\prediction.pckl'

# file = pickle.load(open(r_path, 'rb'))
#
# for f in file.keys():
#     df = file[f]
#     row, _ = df.shape
#     print(df.iloc[row-1])
# today = datetime.today().strftime('%Y-%m-%d')
# df = pd.read_csv('daily_log.csv')
# filtered = df[df.date == datetime.today().strftime('%Y-%m-%d')]
#
# for index, row in filtered.iterrows():
#     print(row.keys()[:-3])
#
# print(filtered.columns)

# print(rows.columns)
# api_to_df()

dict_ = process_data_for_db_insert()

insert_on_exchange_db(None, dict_)

