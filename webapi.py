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

    service = kline_service(client, settings)
    kline =  service.get_tvkline_by_market_symbol(symbol, long(from_time), long(to_time), resolution, settings['kline']['size'])

    if kline and kline.has_key('series'):
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
            elif columns[i] == 'volume':
                volume_index = i

        timezone_offset = settings['timezone_offset']

        ticks = kline['series'][0]['values']
        ticks.sort(lambda x, y: cmp(x[time_index], y[time_index]))

        kline = {
            "s": "ok",
            "t": [],    #timestamp
            "o": [],    #open
            "c": [],    #close
            "h": [],    #high
            "l": [],    #low
            "v": []     #volume
        }

        for index in range(len(ticks)):
            tick = ticks[index]

            # remove the timeslot contains no data
            if tick[open_index] == None or tick[close_index] == None or tick[high_index] == None or tick[low_index] == None:
                continue

            kline["t"].append(tick[time_index])
            kline["o"].append(tick[open_index])
            kline["c"].append(tick[close_index])
            kline["h"].append(tick[high_index])
            kline["l"].append(tick[low_index])
            kline["v"].append(tick[volume_index])

    else:
        #kline = { "s": "no_data", "nextTime": long(time.time() + 60) }
        kline = { "s": "no_data" }

    return json.dumps(kline)

@app.route('/tv/symbols', methods=['GET'])
def tv_symbols():
    symbol = request.args.get('symbol', '')
    ret = None

    # if symbol is 'Market:Symbol' format, only get the last symbol part
    if ':' in symbol:
        symbol = symbol[symbol.find(':') + 1:]

    if symbol and not symbol.lower() in ['index', 'innovation']:
        support_symbols = settings['symbols']['default']
        support_symbols_title = settings['symbols']['_title']

        for index in range(len(support_symbols)):
            symbol = symbol.lower()

            if symbol == support_symbols[index]:
                title = support_symbols_title[index]

                ret = {
                    "name": title,
                    "exchange-traded": "Market",
                    "exchange-listed": "Market",
                    "timezone": "UTC",
                    "minmov": 1,
                    "minmov2": 0,
                    "pointvalue": 1,
                    "session": "24x7",
                    "has_intraday": True,
                    "has_daily": True,
                    "has_weekly_and_monthly": True,
                    "has_no_volume": False,
                    "description": title,
                    "type": "bitcoin",
                    "supported_resolutions": [ "1", "15", "30", "60", "D", "2D", "3D", "W", "3W", "M", "6M" ],
                    "intraday_multipliers": [ "1", "15", "30", "60" ],
                    "pricescale": 100,
                    "ticker": symbol
                }
    else:
        ret = {
            "name": symbol,
            "exchange-traded": "Market",
            "exchange-listed": "Market",
            "timezone": "UTC",
            "minmov": 1,
            "minmov2": 0,
            "pointvalue": 1,
            "session": "24x7",
            "has_intraday": True,
            "has_daily": True,
            "has_weekly_and_monthly": True,
            "has_no_volume": False,
            "description": symbol,
            "type": "bitcoin",
            "supported_resolutions": [ "1", "15", "30", "60", "D", "2D", "3D", "W", "3W", "M", "6M" ],
            "intraday_multipliers": [ "1", "15", "30", "60" ],
            "pricescale": 100,
            "ticker": symbol
        }

    return json.dumps(ret)

@app.route('/tv/1.1/study_templates', methods=['GET'])
def tv_study_templates():
    ret = {
        "status": "ok",
        "data": [ ]
    }

    return json.dumps(ret)

@app.route('/tv/time', methods=['GET'])
def tv_time():
    return str(int(time.time()))

@app.route('/tv/config', methods=['GET'])
def tv_config():
    support_markets = [market for market in settings['markets'].keys() if not market in ['default', '_title', 'k10_daily_rank', 'k10_index_calc', 'k30_index_calc', 'market_ticks_validator']]
    exchanges = [{ "value": "", "name": "All Exchanges", "desc": "" }]
    exchanges.extend([{
        "value": market,
        "name": market,
        "desc": market
    } for market in support_markets])
    symbols_types = [
        {
            "name": "All types",
            "value": ""
        },{
            "name": "Bitcoin",
            "value": "bitcoin"
        }
    ]

    ret = {
        "supports_search": True,
        "supports_group_request": False,
        "supports_marks": False,
        "supports_timescale_marks": False,
        "supports_time": False,
        "allow_symbol_change": True,
        "exchanges": exchanges,
        "symbols_types": symbols_types,
        #"intraday_multipliers": [ "1", "15", "30", "60" ],
        "supported_resolutions": [ "1", "15", "30", "60", "D", "2D", "3D", "W", "3W", "M", "6M" ]
    }

    return json.dumps(ret)

@app.route('/tv/search', methods=['GET'])
def tv_search():
    # http://18.218.165.228:5000/tv/search?limit=30&query=B&type=&exchange=
    query = request.args.get('query', '')
    limit = request.args.get('limit', '')
    exchange = request.args.get('exchange', '')

    support_symbols = settings['symbols']['default']
    support_symbols_title = settings['symbols']['_title']

    if exchange:
        support_markets = [exchange]
    else:
        support_markets = [market for market in settings['markets'].keys() if not market in ['default', '_title', 'k10_daily_rank', 'k10_index_calc', 'k30_index_calc', 'market_ticks_validator']]

    matches = []

    for index in range(len(support_symbols)):
        symbol = support_symbols[index]
        title = support_symbols_title[index]

        if query.lower() in symbol.lower():
            markets = [market for market in support_markets if settings['symbols'][market][index]]
            results = [{
                "description": title,
                "full_name": symbol,
                "exchange": market,
                "symbol": symbol,
                "type": "bitcoin",
            } for market in markets]

            if results:
                matches.extend(results)

    return json.dumps(matches)

@app.route('/api/markets', methods=['GET'])
def api_markets():
    support_markets = [{ "name": key, "title": setting['title'], "order": setting['order'] } for key, setting in settings['markets'].items()]
    return json.dumps(sorted(support_markets, lambda x, y: x['order'] - y['order']))

@app.route('/api/symbols', methods=['GET'])
def api_symbols():
    setting = settings['symbols']
    support_symbols = [{ "name": setting['default'][index], "title": setting['_title'][index] } for index in range(0, len(setting['default']))]
    return json.dumps(support_symbols)

@app.route('/api/topcoins', methods=['GET'])
def api_topcoins():
    count = request.args.get('c', '')

    client = settings['db_adapter']
    service = kline_service(client, settings)

    to_time = long(time.time())
    from_time = to_time - 24 * 3600
    rank = service.query_symbol_daily_rank(from_time, to_time, int(count))

    if rank:
        top_coins = [
            { "name": coin[1], "symbol": "%susdt" % coin[1].lower(), "latest_price": coin[2], "rank": coin[3], "fullname": coin[4], "market_cap_usd": coin[5], "percent_change_24h": coin[6], "volume_usd_24h": coin[7] }
            for coin in rank
        ]

        return json.dumps(top_coins)
    else:
        return json.dumps({})

@app.route('/api/kline', methods=['GET'])
def api_kline():
    market = request.args.get('m', '')
    symbol = request.args.get('s', '')

    client = settings['db_adapter']
    support_markets = settings['markets'].keys()
    support_symbols = settings['symbols']['default']

    if (not market == 'market_index') and (not market in support_markets or not symbol in support_symbols):
        return 'not supported'

    service = kline_service(client, settings)
    kline = service.get_kline_by_market_symbol(market, symbol, '1', settings['kline']['size'])

    if kline and kline.has_key('series'):
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
            elif columns[i] == 'volume':
                volume_index = i

        timezone_offset = settings['timezone_offset']

        ticks = kline['series'][0]['values']
        ticks.sort(lambda x, y: cmp(x[time_index], y[time_index]))

        kline = [[
            get_timestamp_str_short(tick[time_index], 0),
            float(tick[open_index]),
            float(tick[close_index]),
            float(tick[low_index]),
            float(tick[high_index]),
            float(tick[volume_index])
        ] for tick in ticks if not (tick[open_index] == None or tick[close_index] == None or tick[high_index] == None or tick[low_index] == None)]

        return json.dumps(kline)
    else:
        return json.dumps({})

@app.route('/api/trend', methods=['GET'])
def api_trend():
    symbol = request.args.get('s', '')

    client = settings['db_adapter']
    support_markets = settings['markets'].keys()
    support_symbols = settings['symbols']['default']
    to_time = int(time.time())
    from_time = to_time - 4 * 24 * 3600

    if not symbol in support_symbols:
        return 'not supported'

    service = kline_service(client, settings)
    kline = service.get_trend_by_symbol(symbol, from_time, to_time, 'D')

    if kline and kline.has_key('series'):
        columns = kline['series'][0]['columns']
        for i in range(0, len(columns)):
            if columns[i] == 'time':
                time_index = i
            elif columns[i] == 'price':
                price_index = i

        timezone_offset = settings['timezone_offset']

        ticks = kline['series'][0]['values']
        ticks.sort(lambda x, y: cmp(x[time_index], y[time_index]))

        kline = [[
            get_timestamp_str_date(tick[time_index], 0),
            float(tick[price_index])
        ] for tick in ticks if not (tick[price_index] == None)]

        return json.dumps(kline)
    else:
        return json.dumps({})

if __name__ == '__main__':
    global settings

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])
    settings['symbols'] = get_symbols_from_csv(settings['symbols']['path'])

    settings['db_adapter'].open()

    app.run(debug = True, threaded = True, host = '0.0.0.0', port = 5000)
