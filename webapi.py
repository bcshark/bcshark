import os
import sys
import time
import logging
import logging.handlers
import json
import csv

from lib.config import ConfigurationManager
from adapters.influxdb_adapter import influxdb_adapter
#from adapters.mysqldb_adapter import mysqldb_adapter
from adapters.utility import *

from flask import Flask, request
from flask_cors import CORS

from services.kline_service import kline_service

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

file_logger_handler = logging.handlers.TimedRotatingFileHandler(os.path.normpath(os.path.join(sys.path[0], 'logs/webapi.log')), when = 'D', interval = 1, backupCount = 10)
file_logger_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
file_logger_handler.setFormatter(formatter)
file_logger_handler.setLevel(logging.DEBUG)
logger.addHandler(file_logger_handler)

app = Flask(__name__)
CORS(app)

def get_symbols_from_csv(file_path):
    symbols_dict = {}

    with open(os.path.normpath(os.path.join(sys.path[0], file_path)), 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in spamreader:
            symbols_dict[row[0]] = row[1:]

    return symbols_dict

@app.route('/tv/history', methods=['GET'])
def tv_history():
    symbol = request.args.get('symbol', '')
    resolution = request.args.get('resolution', '')
    from_time = request.args.get('from', '')
    to_time = request.args.get('to', '')

    client = settings['db_adapter']
    support_markets = settings['markets'].keys()
    support_symbols = settings['symbols']['default']

    if not symbol == 'Index':
        return 'not supported'

    try:
        client.open()
        service = kline_service(client, settings)
        kline =  service.get_tvkline_by_market_symbol(symbol, long(from_time), long(to_time), settings['kline']['size'])
    finally:
        client.close()

    if kline:
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

        kline = {
            "s": "ok",
            "t": [tick[time_index] for tick in ticks],
            "o": [tick[open_index] for tick in ticks],
            "c": [tick[close_index] for tick in ticks],
            "h": [tick[high_index] for tick in ticks],
            "l": [tick[low_index] for tick in ticks],
        }
    else:
        #kline = { "s": "no_data", "nextTime": long(time.time() + 60) }
        kline = { "s": "no_data" }

    return json.dumps(kline)

@app.route('/tv/symbols', methods=['GET'])
def tv_symbols():
    ret = {
        "name": "Index",
        "exchange-traded": "Market",
        "exchange-listed": "Market",
        "timezone": "China/Shanghai",
        "minmov": 1,
        "minmov2": 0,
        "pointvalue": 1,
        "session": "0930-1630",
        "has_intraday": False,
        "has_no_volume": False,
        "description": "Market Index",
        "type": "stock",
        "supported_resolutions": ["D", "2D", "3D", "W", "3W", "M", "6M"],
        "pricescale": 100,
        "ticker": "Index"
    }

    return json.dumps(ret)

@app.route('/tv/1.1/study_templates', methods=['GET'])
def tv_study_templates():
    ret = {
        "status": "ok",
        "data": [
            { "name": "Best" },
            { "name": "Fav 1" }
        ]
    }

    return json.dumps(ret)

@app.route('/tv/time', methods=['GET'])
def tv_time():
    return str(int(time.time()))

@app.route('/tv/config', methods=['GET'])
def tv_config():
    ret = {
        "supports_search": True,
        "supports_group_request": False,
        "supports_marks": True,
        "supports_timescale_marks": True,
        "supports_time": True,
        "exchanges": [
            {
                "value": "",
                "name": "All Exchanges",
                "desc": ""
            }, {
                "value": "NasdaqNM",
                "name": "NasdaqNM",
                "desc": "NasdaqNM"
            }, {
                "value": "NYSE",
                "name": "NYSE",
                "desc": "NYSE"
            }, {
                "value": "NCM",
                "name": "NCM",
                "desc": "NCM"
            }, {
                "value": "NGM",
                "name": "NGM",
                "desc": "NGM"
            }
        ],
        "symbols_types": [
            {
                "name": "All types",
                "value": ""
            }, {
                "name": "Stock",
                "value": "stock"
            }, {
                "name": "Index",
                "value": "index"
            }
        ],
        "supported_resolutions": ["D", "2D", "3D", "W", "3W", "M", "6M"]
    }

    return json.dumps(ret)

@app.route('/api/markets', methods=['GET'])
def api_markets():
    support_markets = [{ "name": key, "title": setting['title'], "order": setting['order'] } for key, setting in settings['markets'].items()]
    return json.dumps(sorted(support_markets, lambda x, y: x['order'] - y['order']))

@app.route('/api/symbols', methods=['GET'])
def api_symbols():
    setting = settings['symbols']
    support_symbols = [{ "name": setting['default'][index], "title": setting['_title'][index] } for index in range(0, len(setting['default']))]
    return json.dumps(support_symbols)

@app.route('/api/kline', methods=['GET'])
def api_kline():
    market = request.args.get('m', '')
    symbol = request.args.get('s', '')

    client = settings['db_adapter']
    support_markets = settings['markets'].keys()
    support_symbols = settings['symbols']['default']

    if (not market == 'market_index') and (not market in support_markets or not symbol in support_symbols):
        return 'not supported'

    try:
        client.open()
        service = kline_service(client, settings)
        kline =  service.get_kline_by_market_symbol(market, symbol, settings['kline']['size'])
    finally:
        client.close()

    if kline:
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
    else:
        return None

if __name__ == '__main__':
    global settings

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])
    settings['symbols'] = get_symbols_from_csv(settings['symbols']['path'])

    app.run(debug = True, threaded = True, host = '0.0.0.0', port = 5000)
