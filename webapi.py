import time
import logging
import json

from influxdb import InfluxDBClient

from flask import Flask, render_template
from flask_cors import CORS

from services.kline_service import kline_service

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/kline')
def api_kline():
    global client

    service = kline_service(client)
    kline =  service.get_kline_by_time_range(time.time(), time.time())

    return json.dumps(kline)

if __name__ == '__main__':
    global settings
    global client

    settings = { 
        'logger': logger, 
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

    app.run(debug = True)
