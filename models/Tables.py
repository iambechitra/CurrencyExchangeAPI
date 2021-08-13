from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


def to_json_prediction(tpl):
    id, date, from_currency, to_currency, exchange_rate = tpl

    return {
        'date': date[:10],
        'from': from_currency,
        'to': to_currency,
        'rate': exchange_rate
    }


def to_json_exchange(tpl):
    id, date, from_currency, to_currency, open_rate, close_rate, high_rate, low_rate, avg_rate = tpl
    return {
        'date': date[:10],
        'from': from_currency,
        'to': to_currency,
        'rate': avg_rate
    }


class PredictionRate(db.Model):
    __tablename__ = 'PredictionRate'

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    from_currency = db.Column(db.String, nullable=False)
    to_currency = db.Column(db.String, nullable=False)
    exchange_rate = db.Column(db.Float, nullable=False)

    @property
    def serialize(self):
        return {
            'id': self.id,
            'date': self.date,
            'from_currency': self.from_currency,
            'to_currency': self.to_currency,
            'avg_rate': self.exchange_rate
        }


class ExchangeRate(db.Model):
    __tablename__ = 'ExchangeRate'

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    from_currency = db.Column(db.String, nullable=False)
    to_currency = db.Column(db.String, nullable=False)
    open_rate = db.Column(db.Float, nullable=False)
    close_rate = db.Column(db.Float, nullable=False)
    high_rate = db.Column(db.Float, nullable=False)
    low_rate = db.Column(db.Float, nullable=False)
    avg_rate = db.Column(db.Float, nullable=False)

    @property
    def serialize(self):
        return {
            'id': self.id,
            'date': self.date,
            'from_currency': self.from_currency,
            'to_currency': self.to_currency,
            'open_rate': self.open_rate,
            'close_rate': self.close_rate,
            'high_rate': self.high_rate,
            'low_rate': self.low_rate,
            'avg_rate': self.avg_rate
        }
