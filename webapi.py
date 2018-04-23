import time
import logging
import logging.handlers
import json

from lib.config import ConfigurationManager
from adapters.influxdb_adapter import influxdb_adapter
from adapters.mysqldb_adapter import mysqldb_adapter
from adapters.utility import *

from flask import Flask, request
from flask_cors import CORS

from services.kline_service import kline_service

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

file_logger_handler = logging.handlers.TimedRotatingFileHandler('logs/webapi.log', when = 'D', interval = 1, backupCount = 10)
file_logger_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler.setFormatter(formatter)
file_logger_handler.setLevel(logging.DEBUG)
logger.addHandler(file_logger_handler)

app = Flask(__name__)
CORS(app)

@app.route('/api/markets', methods=['GET'])
def api_markets():
    support_markets = [{ "name": key, "title": setting['title'], "order": setting['order'] } for key, setting in settings['markets'].items()]
    return json.dumps(sorted(support_markets, lambda x, y: x['order'] - y['order']))

@app.route('/api/symbols', methods=['GET'])
def api_symbols():
    setting = settings['symbols']
    support_symbols = [{ "name": setting['default'][index], "title": setting['title'][index] } for index in range(0, len(setting['default']))]
    return json.dumps(support_symbols)

@app.route('/api/kline', methods=['GET'])
def api_kline():
    market = request.args.get('m', '')
    symbol = request.args.get('s', '')

    client = settings['db_adapter']
    support_markets = settings['markets'].keys()
    support_symbols = settings['symbols']['default']

    if not market in support_markets or not symbol in support_symbols:
        return 'not supported'

    client.open()
    service = kline_service(client, settings)
    kline =  service.get_kline_by_market_symbol(market, symbol, settings['kline']['size'])
    client.close()

    columns = kline['series'][0]['columns']
    for i in range(0, len(columns)):
        if columns[i] == 'time':
            time_index = i
        elif columns[i] == 'open':
            open_index = i
        elif columns[i] == 'close':
            close_index = i
        elif columns[i] == 'low':
            low_index = i
        elif columns[i] == 'high':
            high_index = i

    timezone_offset = settings['timezone_offset']

    ticks = kline['series'][0]['values']
    ticks.sort(lambda x, y: cmp(x[time_index], y[time_index]))

    kline = [[
        get_timestamp_str(tick[time_index], 0),
        float(tick[open_index]),
        float(tick[close_index]),
        float(tick[low_index]),
        float(tick[high_index])
    ] for tick in ticks]

    return json.dumps(kline)

if __name__ == '__main__':
    global settings

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])

    app.run(debug = True, threaded = True, host = '0.0.0.0', port = 5000)
