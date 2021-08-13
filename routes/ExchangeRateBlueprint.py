from flask import Blueprint

from controllers.ExchangeRateController import *

exchangeBlueprint = Blueprint('ExchangeRateBlueprint', __name__)

exchangeBlueprint.route('/', methods=['GET'])(index)
exchangeBlueprint.route('/filter', methods=['GET'])(request_data_on_range)
exchangeBlueprint.route('/daily', methods=['GET'])(request_data_on_daily)
