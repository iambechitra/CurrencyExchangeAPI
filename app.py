from flask import Flask, jsonify
from flask_apscheduler import APScheduler
from flask_migrate import Migrate

from models.Tables import db
from routes.ExchangeRateBlueprint import exchangeBlueprint
from util.Logger import *
from util.Processor import *

app = Flask(__name__)
app.config.from_object('config')

db.init_app(app)
migrate = Migrate(app, db)
flag = False

app.register_blueprint(exchangeBlueprint, url_prefix='/exchange')

log_directory = os.path.join(os.getcwd(), 'log')


@app.route('/')
def index():
    _str = {
        "STATUS": "INSTALLATION OK",
        "EXAMPLE": {
            "RANGE DATA": ".../exchange/filter?from=USD&to=JPY&start=2005-10-15&end=2022-08-08",
            "DAILY DATA": ".../exchange/daily?currency=JPY,EUR,USD&base=BDT",
            "LIST OF AVAILABLE CURRENCIES": ".../exchange/list"
        },
        "NOTE": "In Example '...' will be replaced with localhost:port"
    }
    return jsonify(_str)


def schedule_job():
    api_to_df()
    write_log(location=log_directory, message=on_invoke('schedule_job'))


def schedule_insert():
    status = insert_on_db_as_scheduled(db, app.app_context())
    write_log(location=log_directory, message=on_write('schedule_insert'))
    if status:
        file_ = os.path.join(os.getcwd(), 'res')+os.sep+'daily_log.csv'
        print(f'File will be deleted from the path given bellow:\n {file_}')
        os.remove(file_)



def schedule_train():
    # global flag
    # if flag:
    #     print('Skipped training...')
    #     return
    # else:
    #     flag = True
    generate_pickle_file(db_ref=db, context=app.app_context(), directory=os.path.join(os.getcwd(), 'mlkit'),
                         from_db=True)
    prediction_to_database(context=app.app_context(), db_ref=db, drop=True)
    write_log(location=log_directory, message=on_invoke('schedule_train'))


if __name__ == '__main__':
    print(f'App is started on : {datetime.now()}')
    with app.app_context():
        scheduler = APScheduler()
        scheduler.init_app(app)
        scheduler.add_job(id='api_call', func=schedule_job, trigger='interval', hours=1)
        scheduler.add_job(id='database_insertion', func=schedule_insert, trigger='interval', days=1)
        scheduler.add_job(id='model_train', func=schedule_train, trigger='interval', days=7)
        scheduler.start()

        if not os.path.isfile('res' + os.sep + 'daily_log.csv'):
            api_to_df()

        if not os.path.isfile('res' + os.sep + 'exchange_rate.db'):
            db.create_all()
            df = to_df()
            to_database(df, db)
            generate_pickle_file(db_ref=db, context=app.app_context(), directory=os.path.join(os.getcwd(), 'mlkit'),
                                 from_db=True)
            prediction_to_database(context=app.app_context(), db_ref=db)

    app.run(debug=True, use_reloader=False)
