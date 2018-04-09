import time
import logging
import json

from lib.config import ConfigurationManager
from adapters.influxdb_adapter import influxdb_adapter
from adapters.mysqldb_adapter import mysqldb_adapter
from adapters.utility import *

from flask import Flask
from flask_cors import CORS

from services.kline_service import kline_service

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/kline/<market>/<symbol>', methods=['GET'])
def api_kline(market, symbol):
    client = settings['db_adapter']

    client.open()
    service = kline_service(client)
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

    kline = [[
        get_timestamp_str(value[time_index], 0),
        float(value[open_index]),
        float(value[close_index]),
        float(value[low_index]),
        float(value[high_index])
    ] for value in kline['series'][0]['values']]

    return json.dumps(kline)

if __name__ == '__main__':
    global settings

    settings = ConfigurationManager('config/global.json')
    settings['logger'] = logger
    settings['db_adapter'] = influxdb_adapter(settings['influxdb'])
    #settings['db_adapter'] = mysqldb_adapter(settings['mysqldb'])

    app.run(debug = True, threaded = True, host = '0.0.0.0', port = 5000)
