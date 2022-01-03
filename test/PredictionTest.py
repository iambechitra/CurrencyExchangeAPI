from flask import Flask, jsonify
from flask_apscheduler import APScheduler
from flask_migrate import Migrate

from models.Tables import *
from routes.ExchangeRateBlueprint import exchangeBlueprint
from util.Logger import *
from util.Processor import *

app = Flask(__name__)
app.config.from_object('config')

db.init_app(app)
migrate = Migrate(app, db)

if __name__ == '__main__':
    print(f'App is started on : {datetime.now()}')
    with app.app_context():
        query = db.session.query(ExchangeRate.to_currency.distinct().label('to_currency'))
        lst = [row.to_currency for row in query.all()]

