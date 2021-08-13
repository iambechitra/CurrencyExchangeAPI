import os
import pandas as pd
from datetime import datetime, timedelta

from flask import request, jsonify
from flask_sqlalchemy import SQLAlchemy

from models.Tables import to_json_exchange, to_json_prediction
from util.Processor import get_transformation_of_daily_data, to_dataframe_from_db, PREDICTION_COLUMNS, EXCHANGE_COLUMNS, \
    get_transformation_on_range_data, get_prediction_transformation_on_range_data, CSV_LOCATION

db = SQLAlchemy()


def index():
    return 'Running...'


def request_data_on_daily():
    df = pd.read_csv(CSV_LOCATION)
    currency = request.args.get('currency')
    print(currency)
    base = request.args.get('base')
    resp = get_transformation_of_daily_data(base, currency.split(','), df)
    print(resp)
    return jsonify(resp)


def request_data_on_range():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    __end = datetime.strptime(end_date, '%Y-%m-%d')
    _from = request.args.get('from')
    _to = request.args.get('to')
    lst = []
    if _from == 'USD':
        query_builder = f'SELECT * FROM ExchangeRate WHERE date >= "{start_date}" AND date <= ' \
                        f'"{(__end + timedelta(days=1)).__str__()[:10]}" ' \
                        f'AND from_currency="{_from}" AND to_currency="{_to}" ORDER BY date DESC'

        # query_items = ExchangeRate.query.filter(
        #     and_(
        #         ExchangeRate.date > start_date,
        #         ExchangeRate.date < end_date,
        #         ExchangeRate.from_currency == _from,
        #         ExchangeRate.to_currency == _to
        #     )
        # ).order_by(ExchangeRate.date.desc())
        # plan_event = index()

        all_entry = db.session.execute(query_builder)
        for index, item in enumerate(all_entry):
            if index == 0:
                _, date, *_ = item
                query_end_date = datetime.strptime(date[:10], '%Y-%m-%d')
                print(f'end_date: {__end} and db_end: {query_end_date}')
                dp1 = query_end_date + timedelta(days=1)
                if __end - timedelta(days=1) <= query_end_date:
                    print('Here')
                    continue
                else:
                    prediction_query = f"SELECT * FROM PredictionRate WHERE date >= '{dp1.__str__()[:10]}' AND date <= " \
                                       f"'{(__end + timedelta(days=1)).__str__()[:10]}'" \
                                       f'AND from_currency="{_from}" AND to_currency="{_to}" ORDER BY date DESC'
                    print(prediction_query)
                    predicted = db.session.execute(prediction_query)

                for predict in predicted:
                    lst.append(to_json_prediction(predict))

            lst.append(to_json_exchange(item))
    else:
        query_builder1 = f'SELECT * FROM ExchangeRate WHERE date >= "{start_date}" AND date <= ' \
                         f'"{(__end + timedelta(days=1)).__str__()[:10]}" ' \
                         f'AND from_currency="USD" AND to_currency="{_from}" ORDER BY date DESC'
        query_builder2 = f'SELECT * FROM ExchangeRate WHERE date >= "{start_date}" AND date <= ' \
                         f'"{(__end + timedelta(days=1)).__str__()[:10]}" ' \
                         f'AND from_currency="USD" AND to_currency="{_to}" ORDER BY date DESC'

        result_from = db.session.execute(query_builder1)
        result_to = db.session.execute(query_builder2)

        df1 = to_dataframe_from_db(result_from, EXCHANGE_COLUMNS)
        df2 = to_dataframe_from_db(result_to, EXCHANGE_COLUMNS)

        row1, _ = df1.shape
        row2, _ = df2.shape

        if row2 > row1:
            row = df1.iloc[0]
        else:
            row = df2.iloc[0]

        date_finally = row['date'][:10]
        print(date_finally)

        query_end_date = datetime.strptime(date_finally, '%Y-%m-%d')
        dp1 = query_end_date + timedelta(days=1)
        if __end - timedelta(days=1) <= query_end_date:
            print('No need to read prediction...')
        else:
            prediction_query1 = f"SELECT * FROM PredictionRate WHERE date >= '{dp1.__str__()[:10]}' AND date <= " \
                                f"'{(__end + timedelta(days=1)).__str__()[:10]}'" \
                                f'AND from_currency="USD" AND to_currency="{_from}" ORDER BY date DESC'
            prediction_query2 = f"SELECT * FROM PredictionRate WHERE date >= '{dp1.__str__()[:10]}' AND date <= " \
                                f"'{(__end + timedelta(days=1)).__str__()[:10]}'" \
                                f'AND from_currency="USD" AND to_currency="{_to}" ORDER BY date DESC'

            predicted1 = db.session.execute(prediction_query1)
            predicted2 = db.session.execute(prediction_query2)

            pdf1 = to_dataframe_from_db(predicted1, PREDICTION_COLUMNS)
            pdf2 = to_dataframe_from_db(predicted2, PREDICTION_COLUMNS)

            lst = get_prediction_transformation_on_range_data(pdf1, pdf2)

        # print(len(result_from), len(result_to))
        return jsonify(get_transformation_on_range_data(df1, df2, lst if len(lst) > 0 else None))

        # if _from != 'USD':

    return jsonify(lst)
