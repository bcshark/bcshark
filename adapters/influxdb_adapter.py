from influxdb import InfluxDBClient

from .database_adapter import database_adapter
from .utility import *

class influxdb_adapter(database_adapter):
    def __init__(this, settings):
        super(influxdb_adapter, this).__init__(settings)

        this.client = None

    def open(this):
        if not this.client:
            this.client = InfluxDBClient(host = this.host, port = this.port, database = this.database)
            this.create_database_if_not_exists(this.database)

        return this.client

    def close(this):
        if this.client:
            this.client.close()
            this.client = None

    def create_database_if_not_exists(this, database_name):
        if not filter(lambda db : db['name'] == database_name, this.client.get_list_database()):
            this.client.create_database(database_name) 

    def generate_points_by_ticks(this, measurement_name, market_name, symbol_name, ticks):
        points = [{
            'measurement': measurement_name,
            'tags': {
                'market': market_name,
                'symbol': symbol_name
            },
            'time': get_timestamp_str(tick.time, tick.timezone_offset),
            'fields': {
                'time': tick.time + tick.timezone_offset,
                'open': float(tick.open),
                'close': float(tick.close),
                'low': float(tick.low),
                'high': float(tick.high),
                'amount': float(tick.amount),
                'volume': float(tick.volume),
                'count': float(tick.count),
                'period': tick.period
            }
        } for tick in ticks]

        return points

    def save_tick(this, market_name, symbol_name, tick):
        measurement_name = 'huobi_ticks'

        points = this.generate_points_by_ticks(measurement_name, market_name, symbol_name, [ tick ])

        this.client.write_points(points)

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        #measurement_name = 'ticks_%s' % market_name
        measurement_name = 'market_ticks'

        points = this.generate_points_by_ticks(measurement_name, market_name, symbol_name, ticks)

        this.client.write_points(points)

    def query(this, sql):
        result_set = this.client.query(sql)
        return result_set.raw
