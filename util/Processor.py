import glob
import os
import pickle
from datetime import datetime

import requests
import pandas as pd
from models.Tables import ExchangeRate, PredictionRate
from mlkit.Prediction import Model, DataLoader

EXCHANGE_COLUMNS = ['id', 'date', 'from_currency', 'to_currency', 'open_rate', 'close_rate', 'high_rate', 'low_rate',
                    'avg_rate']

PREDICTION_COLUMNS = ['id', 'date', 'from_currency', 'to_currency', 'avg_rate']

CSV_LOCATION = os.path.join(os.getcwd(), 'res\daily_log.csv')
PICKLE_LOCATION = os.path.join(os.getcwd(), 'mlkit\generated.pckl')
RAW_CSV = os.path.join(os.getcwd(), 'res\csv')


def to_df(x=None):
    data = glob.glob(RAW_CSV + "/*.csv")
    from_currency = 'USD'
    if x is not None:
        from_currency = x

    _new_df = pd.DataFrame()

    for d in data:
        df = pd.read_csv(d)
        df.drop(labels=['Volume'], axis=1, inplace=True)
        to_currency = d.split('\\')[-1].split('=')[0]
        row, _ = df.shape
        _to = [to_currency for i in range(row)]
        _from = [from_currency for i in range(row)]
        df.insert(1, 'From', _from, True)
        df.insert(2, 'To', _to, True)

        _new_df = _new_df.append(df, ignore_index=True)
    _new_df['Date'] = pd.to_datetime(_new_df['Date']).dt.date
    return _new_df


def to_dataframe_from_db(lst_item, columns):
    temp = []
    for lst in lst_item:
        temp.append(lst)

    return pd.DataFrame(columns=columns, data=temp)


def on_exchange_rate_from_db_to_df():
    raw = ExchangeRate.query.all()

    lst = []

    for row in raw:
        lst.append(row.serialize)

    return pd.DataFrame(lst)


def to_database(df=None, db_ref=None):
    if df is None or db_ref is None:
        return
    else:
        df.dropna(inplace=True)
        nrows, _ = df.shape
        for index, row in df.iterrows():
            # print('row')
            try:
                __row = ExchangeRate(
                    date=row.Date,
                    from_currency=row.From,
                    to_currency=row.To,
                    open_rate=row.Open,
                    close_rate=row.Close,
                    high_rate=row.High,
                    low_rate=row.Low,
                    avg_rate=row['Adj Close']
                )
                db_ref.session.add(__row)
                print(f'Insertion Completed on {index}/{nrows}')
            except Exception as e:
                print(f'Exception Occurred {index}/{nrows}: {e}')
                continue
        db_ref.session.commit()


def prediction_to_database(db_ref):
    if db_ref is None:
        return
    else:
        file = pickle.load(open(PICKLE_LOCATION, 'rb'))

        for f in file.keys():
            df = file[f]
            df = df.dropna()
            for index, row in df.iterrows():
                try:
                    date = row['ds']
                    rate = row['yhat']
                    _from = row['from']
                    _to = row['to']

                    __row = PredictionRate(
                        date=date,
                        from_currency=_from,
                        to_currency=_to,
                        exchange_rate=rate
                    )
                    db_ref.session.add(__row)
                except Exception as e:
                    print(f'Exception Occurred: {e}')
                    continue
        db_ref.session.commit()


def api_to_df():
    link = "https://openexchangerates.org/api/latest.json?app_id=2ec8f4fc09e44965bf8da0ad2b42626b"
    f = requests.get(link)
    data = f.json()
    timestamp = data['timestamp']
    date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S').__str__()[:10]
    rates = data['rates']
    rates['timestamp'] = timestamp
    rates['date'] = date
    rates['base'] = 'USD'

    print(rates)
    print(os.path.join(os.getcwd(), 'res\daily_log.csv'))
    if not os.path.isfile('res\daily_log.csv'):
        pd.DataFrame(rates, index=[0]).to_csv(CSV_LOCATION, index=False)
    else:
        df = pd.read_csv(CSV_LOCATION)
        row, _ = df.shape
        df.append(pd.DataFrame(rates, index=[0])).to_csv(CSV_LOCATION, index=False)


def insert_on_db_as_scheduled(db_ref, context):
    _dict = process_data_for_db_insert()
    if context is not None:
        with context:
            insert_on_exchange_db(db_ref, _dict)
    else:
        print('No Context Found!')


def get_transformation_of_daily_data(base, target, df):
    row = df.iloc[-1]
    response = {}
    if base == 'USD':
        response['base'] = base
        response['date'] = row['date']
        response['timestamp'] = str(row['timestamp'])
        for tar in target:
            response[tar] = row[tar]

    else:
        base_f = float(row[base])
        response['base'] = base
        response['date'] = row['date']
        response['timestamp'] = str(row['timestamp'])
        for tar in target:
            response[tar] = str(float(row[tar]) / base_f)

    return response


def get_transformation_on_range_data(base, target, lst=None):
    row1, _ = base.shape
    row2, _ = target.shape
    tpl = [] if lst is None else lst
    if row1 > row2:
        for index, row in target.iterrows():
            base_row = base.iloc[index]
            _target = row['to_currency']
            _from = base_row['to_currency']
            _open = str(float(row['open_rate']) / float(base_row['open_rate']))
            _close = str(float(row['close_rate']) / float(base_row['close_rate']))
            _high = str(float(row['high_rate']) / float(base_row['high_rate']))
            _low = str(float(row['low_rate']) / float(base_row['low_rate']))
            _avg = str(float(row['avg_rate']) / float(base_row['avg_rate']))

            tpl.append({
                'date': row['date'][:10],
                'from': _from,
                'to': _target,
                'rate': _avg
            })
    else:
        for index, row in base.iterrows():
            target_row = target.iloc[index]
            _target = target_row['to_currency']
            _from = row['to_currency']
            _open = str(float(target_row['open_rate']) / float(row['open_rate']))
            _close = str(float(target_row['close_rate']) / float(row['close_rate']))
            _high = str(float(target_row['high_rate']) / float(row['high_rate']))
            _low = str(float(target_row['low_rate']) / float(row['low_rate']))
            _avg = str(float(target_row['avg_rate']) / float(row['avg_rate']))

            tpl.append({
                'date': target_row['date'][:10],
                'from': _from,
                'to': _target,
                'rate': _avg
            })

    return tpl


def get_prediction_transformation_on_range_data(base, target):
    row1, _ = base.shape
    row2, _ = target.shape
    tpl = []
    if row1 > row2:
        for index, row in target.iterrows():
            base_row = base.iloc[index]
            _target = row['to_currency']
            _from = base_row['to_currency']
            _avg = str(float(row['avg_rate']) / float(base_row['avg_rate']))

            tpl.append({
                'date': row['date'][:10],
                'from': _from,
                'to': _target,
                'rate': _avg
            })
    else:
        for index, row in base.iterrows():
            target_row = target.iloc[index]
            _target = target_row['to_currency']
            _from = row['to_currency']
            _avg = str(float(target_row['avg_rate']) / float(row['avg_rate']))

            tpl.append({
                'date': target_row['date'][:10],
                'from': _from,
                'to': _target,
                'rate': _avg
            })

    return tpl


def process_data_for_db_insert():
    today = datetime.today().strftime('%Y-%m-%d')
    # print(os.getcwd())
    df = pd.read_csv(CSV_LOCATION)
    filtered = df[df.date == today]
    nrows, _ = filtered.shape
    currency_columns = list(filtered.columns)[:-3]
    # print(currency_columns)
    counter = 0
    currency_processed_dict = {}
    for index, row in filtered.iterrows():
        counter += 1
        if counter == 1:
            print(f'coming open: {counter}:{index}')
            for currency in currency_columns:
                currency_processed_dict[currency] = {
                    'date': row['date'],
                    'open': float(row[currency]),
                    'close': float(row[currency]),
                    'high': float(row[currency]),
                    'low': float(row[currency]),
                    'avg': float(row[currency])
                }

        elif counter == nrows:
            print(f'coming close: {counter}:{index}')
            for currency in currency_columns:
                curr = float(row[currency])
                currency_processed_dict[currency]['close'] = curr
                if curr > currency_processed_dict[currency]['high']:
                    currency_processed_dict[currency]['high'] = curr
                if curr < currency_processed_dict[currency]['low']:
                    currency_processed_dict[currency]['low'] = curr

                currency_processed_dict[currency]['avg'] = (currency_processed_dict[currency]['avg'] + curr) / 2
        else:
            print(f'coming: {counter}:{index}')
            for currency in currency_columns:
                curr = float(row[currency])
                if curr > currency_processed_dict[currency]['high']:
                    currency_processed_dict[currency]['high'] = curr
                if curr < currency_processed_dict[currency]['low']:
                    currency_processed_dict[currency]['low'] = curr

                currency_processed_dict[currency]['avg'] = (currency_processed_dict[currency]['avg'] + curr) / 2

    return currency_processed_dict


def insert_on_exchange_db(db_ref, currency_dict):
    for key in currency_dict.keys():
        _date, _open, _close, _high, _low, _avg = currency_dict[key]['date'], currency_dict[key]['open'], \
                                                  currency_dict[key]['close'], currency_dict[key]['high'], \
                                                  currency_dict[key]['low'], currency_dict[key]['avg']

        from_currency = 'USD'
        to_currency = key

        try:
            __row = ExchangeRate(
                date=datetime.strptime(_date, '%Y-%m-%d'),
                from_currency=from_currency,
                to_currency=to_currency,
                open_rate=_open,
                close_rate=_close,
                high_rate=_high,
                low_rate=_low,
                avg_rate=_avg
            )
            db_ref.session.add(__row)
        except Exception as e:
            print(f'Exception Occurred: {e}')
            continue

    db_ref.session.commit()
    print('Insertion Success!')


def path():
    print(CSV_LOCATION)


def is_insertion_possible(dataframe, count=8):
    today = datetime.today().strftime('%Y-%m-%d')
    filtered = dataframe[dataframe.date == today]
    rows, _ = filtered.shape

    if rows >= count:
        return True

    return False


def generate_pickle_file(db_ref, context, directory, from_db=False):
    if from_db:
        print(f'Training will start shortly\nData Source DB')
        query = 'SELECT DISTINCT to_currency FROM ExchangeRate'
        if context is None:
            df = on_exchange_rate_from_db_to_df()
            lst = db_ref.session.execute(query)
        else:
            with context:
                df = on_exchange_rate_from_db_to_df()
                lst = db_ref.session.execute(query)

        _temp = []
        for l in lst:
            _temp.append(l[-1])
        print(f'DISTINCT KEY:\n{_temp}')
        df_map = {}
        for temp in _temp:
            filtered = df[df.to_currency == temp]
            row, _ = filtered.shape

            if row > 150:
                _filtered = filtered.drop(columns=['from_currency', 'to_currency'])
                df_map[temp] = _filtered

        model = Model(df_map=df_map)
        model.train(limit=180)
        result = model.get_result_dictionary()
        data_loader = DataLoader(data_path=RAW_CSV)
        data_loader.dump(result=result, path=directory, filename='generated')

    else:
        print(f'Training will start shortly\nData Source Local CSV')
        loader = DataLoader(data_path=RAW_CSV)
        loader.load()
        df_map = loader.get_data_dictionary()

        model = Model(df_map=df_map)
        model.train(limit=180)
        result = model.get_result_dictionary()
        loader.dump(result=result, path=directory, filename='generated')
