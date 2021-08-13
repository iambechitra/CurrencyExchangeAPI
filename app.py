from flask import Flask
from flask_apscheduler import APScheduler
from flask_migrate import Migrate

from models.Tables import db
from routes.ExchangeRateBlueprint import exchangeBlueprint
from util.Processor import *

app = Flask(__name__)
app.config.from_object('config')

db.init_app(app)
migrate = Migrate(app, db)

app.register_blueprint(exchangeBlueprint, url_prefix='/exchange')


@app.route('/')
def index():
    return 'Running on root...'


def schedule_job():
    api_to_df()


def schedule_insert():
    insert_on_db_as_scheduled(db, app.app_context())


def schedule_train():
    generate_pickle_file(db_ref=db, context=app.app_context(), directory=os.path.join(os.getcwd(), 'mlkit'),
                         from_db=True)


if __name__ == '__main__':
    with app.app_context():
        scheduler = APScheduler()
        scheduler.init_app(app)
        scheduler.add_job(id='api_call', func=schedule_job, trigger='interval', hours=1)
        scheduler.add_job(id='database_insertion', func=schedule_insert, trigger='interval', days=1)
        scheduler.add_job(id='model_train', func=schedule_train, trigger='interval', days=7)
        scheduler.start()

        if not os.path.isfile('res\daily_log.csv'):
            print('Not visiting here means visiting...')
            api_to_df()

        if not os.path.isfile('res\exchange_rate.db'):
            db.create_all()
            df = to_df()
            to_database(df, db)
            generate_pickle_file(db_ref=db, context=None, directory=os.path.join(os.getcwd(), 'mlkit'), from_db=True)
            prediction_to_database(db)

    app.run(debug=True)
