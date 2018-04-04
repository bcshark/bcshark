import time
import logging
import json

from influxdb import InfluxDBClient

from flask import Flask
from flask_cors import CORS

from services.kline_service import kline_service

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/kline/<market>/<symbol>', methods=['GET'])
def api_kline(market, symbol):
    global client

    service = kline_service(client)
    kline =  service.get_kline_by_market_symbol(market, symbol)

    columns = kline['series'][0]['columns']
    for i in range(0, len(columns) - 1):
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

    kline = [[
        value[time_index],
        value[open_index],
        value[close_index],
        value[low_index],
        value[high_index]
    ] for value in kline['series'][0]['values']]

    return json.dumps(kline)

if __name__ == '__main__':
    global settings
    global client

    settings = { 
        'logger': logger, 
        'timezone_offset': -8 * 3600,
        'influxdb': {
            'host': '127.0.0.1',
            'port': 8086,
            'username': 'root',
            'password': 'root',
            'database': 'market_index'
        },
        'symbols': [ 'btcusdt', 'eosbtc', 'ethbtc' ]
    }

    influxdb_conf = settings['influxdb']
    client = InfluxDBClient(host = influxdb_conf['host'], port = influxdb_conf['port'], database = influxdb_conf['database'])

    app.run(debug = True, threaded = True, host = '0.0.0.0', port = 5000)
