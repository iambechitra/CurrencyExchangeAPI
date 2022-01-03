import glob
import os
import pickle
import pandas as pd
from fbprophet import Prophet


class DataLoader:
    def __init__(self, data_path=None, model_path=None, model_name='result'):
        self.__model_path = model_path
        self.__data_path = data_path
        self.__result = None
        self.__model_name = model_name
        self.__data_dict = {}

    def load(self):
        if self.__model_path is None and self.__data_path is None:
            return {False: None}
        elif self.__model_path is None and self.__data_path is not None:
            return self.__data_loader(self.__data_path)
        elif self.__model_path is not None and self.__data_path is None:
            return self.__model_loader(path=self.__model_path, filename=self.__model_name)
        else:
            self.__data_loader(self.__data_path)
            self.__model_loader(path=self.__model_path, filename=self.__model_name)
            return {True: True}

    def __data_loader(self, path):
        datasets = glob.glob(path + os.sep + '*.csv')

        print(f'Total datasets = {len(datasets)}')
        for index, link in enumerate(datasets):
            print(f'Processing Data[{index + 1}]')
            print(link)
            key = link.split(os.sep)[-1].split('=')[0]
            df = pd.read_csv(link)
            self.__data_dict[key] = df
            print(f'{key}: Data[{index + 1}] Processing complete...')

        return self.__data_dict

    def __model_loader(self, path, filename='result'):
        glb = glob.glob(path + os.sep + filename + '.pckl')
        link = glb[0] if len(glb) > 0 else glb

        with open(link, 'rb') as fin:
            model = pickle.load(fin)

        self.__result = model

        return model

    def dump(self, result, path=None, filename='prediction'):
        if path is None and self.__model_path is None:
            return False

        if path is not None:
            file_path = r'' + path + os.sep + filename + '.pckl'
            with open(file_path, 'wb') as fout:
                pickle.dump(result, fout)

        else:
            file_path = r'' + self.__model_path + os.sep + filename + '.pckl'
            with open(file_path, 'wb') as fout:
                pickle.dump(result, fout)

        return True

    def get_data_dictionary(self):
        return self.__data_dict  # can return None

    def get_result(self):
        return self.__result  # can return None


class Model:
    def __init__(self, df_map):
        self.__df_map = df_map
        self.__result_dict = {}

    def train(self, limit=30):

        if limit < 30:
            limit = 30

        for index, key in enumerate(self.__df_map):
            print(f'Serial Number : {index + 1}')
            print(f'Training Model for Currency: {key}')
            df = self.__df_map[key]
            chunk = pd.DataFrame()
            model = Prophet(daily_seasonality=True)
            chunk['ds'] = pd.to_datetime(df['date'])
            chunk['y'] = df['avg_rate']
            model.fit(chunk)
            future = model.make_future_dataframe(periods=limit)
            forecast = model.predict(future)
            row, _ = forecast.shape
            forecast['from'] = ['USD' for _ in range(row)]
            index_key = key.split(os.sep)[-1]
            print(f'Index Key: {index_key}')
            forecast['to'] = [index_key for _ in range(row)]
            self.__result_dict[index_key] = forecast
            print(f'{key}: Model Training Complete...')

    def get_result_dictionary(self):
        return self.__result_dict


class Prediction:
    def __init__(self, result_dict, limit=30):
        self.__result_dict = result_dict

        if limit >= 30:
            self.limit = limit
        else:
            self.limit = 30

    def get_next_day_forecast(self, currency, base=None):
        if self.__result_dict is None: return None

        df = self.__result_dict[currency]
        row, col = df.shape

        if base is None:
            start = row - self.limit
            end = row - self.limit + 1
            return df[start:end][['ds', 'yhat']]
        else:
            start = (df[df['ds'] == base].index[0] + 1)
            if start != 1 and start + 1 <= row:
                return df[start:start + 1][['ds', 'yhat']]
            else:
                return None

    def get_next_seven_days_forecast(self, currency, base=None):
        if self.__result_dict is None: return None
        df = self.__result_dict[currency]
        row, col = df.shape

        if base is None:
            start = row - self.limit
            end = (row - self.limit) + 7
            return df[start:end][['ds', 'yhat']]
        else:
            start = (df[df['ds'] == base].index[0] + 1)
            end = start + 7
            if start != 1 and end <= row:
                return df[start:end][['ds', 'yhat']]
            else:
                return None

    def get_next_fourteen_days_forecast(self, currency, base=None):
        if self.__result_dict is None: return None
        df = self.__result_dict[currency]
        row, col = df.shape

        if base is None:
            start = row - self.limit
            end = (row - self.limit) + 14
            return df[start:end][['ds', 'yhat']]

        else:
            start = (df[df['ds'] == base].index[0] + 1)
            end = start + 14
            if start != 1 and end <= row:
                return df[start:end][['ds', 'yhat']]
            else:
                return None

    def get_next_thirty_days_forecast(self, currency, base=None):
        if self.__result_dict is None: return None
        df = self.__result_dict[currency]
        row, col = df.shape

        if base is None:
            start = row - self.limit
            end = (row - self.limit) + 30
            return df[start:end][['ds', 'yhat']]
        else:
            start = (df[df['ds'] == base].index[0] + 1)
            end = start + 30
            if start != 1 and end <= row:
                return df[start:end][['ds', 'yhat']]
            else:
                return None

    def get_all_days_forecast(self, currency):
        if self.__result_dict is None: return None
        df = self.__result_dict[currency]
        row, col = df.shape
        start = row - self.limit
        return df[start:][['ds', 'yhat']]
