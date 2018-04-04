import requests

from influxdb import InfluxDBClient

from .utility import *

class collector(object):
    def __init__(this, settings):
        this.settings = settings
        this.logger = settings['logger']
        this.symbols = settings['symbols']
        this.timezone_offset = settings['timezone_offset']
        this.client = None

    def dispose(this):
        if this.client:
            this.client.close()

    def get_influxdb_client(this):
        if not this.client:
            influxdb_conf = this.settings['influxdb']
            this.client = InfluxDBClient(host = influxdb_conf['host'], port = influxdb_conf['port'], database = influxdb_conf['database'])
            this.create_database_if_not_exists(this.client, influxdb_conf['database'])

        return this.client

    def http_request_json(this, url):
        try:
            res = requests.get(url)

            return res.json()
        except Exception, e:
            return None

    def bulk_save_tick(this, market_name, symbol_name, ticks):
        measurement_name = 'ticks_%s' % market_name

        points = [{
            'measurement': measurement_name,
            'tags': {
                'market': market_name,
                'symbol': symbol_name
            },
            'time': get_timestamp_str(tick['id'], this.timezone_offset),
            'fields': {
                'time': tick['id'] + this.timezone_offset,
                'open': tick['open'],
                'close': tick['close'],
                'low': tick['low'],
                'high': tick['high'],
                'amount': tick['amount'],
                'volume': tick['vol'],
                'count': tick['count']
            }
        } for tick in ticks ]

        client = this.get_influxdb_client()
        client.write_points(points)

    def create_database_if_not_exists(this, client, database_name):
        if not filter(lambda db : db['name'] == database_name, client.get_list_database()):
            client.create_database(database_name) 
