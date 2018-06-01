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

    def generate_point_by_trade(this, measurement_name, market_name, symbol_name, trade):
        fields = None

        if isinstance(trade, dict):
            fields = trade

        point = {
            'measurement': measurement_name,
            'tags': {
                'market': market_name,
                'symbol': symbol_name
            },
            'time': get_timestamp_str(fields['time'], fields['timezone_offset']),
            'fields': fields
        }

        return point

    def generate_point_by_tick(this, measurement_name, market_name, symbol_name, tick):
        fields = None

        if isinstance(tick, dict):
            fields = tick
        elif isinstance(tick, object):
            fields = {
                'time': tick.time + tick.timezone_offset,
                'timezone_offset': tick.timezone_offset,
                'open': float(tick.open),
                'close': float(tick.close),
                'low': float(tick.low),
                'high': float(tick.high),
                'amount': float(tick.amount),
                'volume': float(tick.volume),
                'count': float(tick.count),
                'period': tick.period
            }
           
        if fields:
            point = {
                'measurement': measurement_name,
                'tags': {
                    'market': market_name,
                    'symbol': symbol_name
                },
                'time': get_timestamp_str(fields['time'], fields['timezone_offset']),
                'fields': fields
            }
            return point

        return None

    def generate_points_by_k10_rank(this, measurement_name, k10_rank):
        point = {
            'measurement': measurement_name,
            'tags': {
                'symbol': k10_rank.symbol
            },
            'time': get_timestamp_str(long(k10_rank.time)+k10_rank.timezone_offset, k10_rank.timezone_offset),
            'fields': {
                'id': k10_rank.id,
                'name': k10_rank.name,
                'price_usd': float(k10_rank.price_usd),
                'volume_usd_24h': float(k10_rank.volume_usd_24h),
                'market_cap_usd': float(k10_rank.market_cap_usd),
                'total_supply': float(k10_rank.total_supply),
                'max_supply': float(k10_rank.max_supply),
                'percent_change_1h': float(k10_rank.percent_change_1h),
                'percent_change_24h': float(k10_rank.percent_change_24h),
                'percent_change_7d': float(k10_rank.percent_change_7d),
                'rank': int(k10_rank.rank),
                'period': k10_rank.period
            }
        }

        return [ point ]

    def generate_points_by_k10_index(this, measurement_name, k10_index):
        point = {
            'measurement': measurement_name,
            'tags': {
                'symbol': 'k10',
            },
            'time': get_timestamp_str(long(k10_index['time']), k10_index['timezone_offset']),
            'fields': {
                'time': long(k10_index['time']) + k10_index['timezone_offset'],
                'high': float(k10_index['high']),
                'low': float(k10_index['low']),
                'open': float(k10_index['open']),
                'close': float(k10_index['close']),
                'volume': float(k10_index['volume']),
                'period': k10_index['period']
            }
        }

        return [ point ]

    def generate_points_validation(this, measurement_name, validation):
        point = {
            'measurement': measurement_name,
            'tags': {
                'symbol': validation.symbol,
                'market': validation.market
            },
            'time': get_timestamp_str(long(validation.time), validation.timezone_offset),
            'fields': {
                'period': validation.period,
                'table': validation.table,
                'msg':  validation.msg
            }
        }

        return [ point ]

    def query(this, sql, epoch = None):
        result_set = this.client.query(sql, epoch = epoch)
        return result_set.raw

    def save_trade(this, measurement_name, market_name, symbol_name, trade):
        points = [ this.generate_point_by_trade(measurement_name, market_name, symbol_name, trade) ]
        this.client.write_points(points)

    def save_tick(this, measurement_name, market_name, symbol_name, tick):
        points = [ this.generate_point_by_tick(measurement_name, market_name, symbol_name, tick) ]
        this.client.write_points(points)

    def save_market_ticks(this, market_name, symbol_name, ticks):
        measurement_name = 'market_ticks'

        points = [ this.generate_point_by_tick(measurement_name, market_name, symbol_name, tick) for tick in ticks ]
        this.client.write_points(points)

    def save_k10_daily_rank(this, table_name, k10_rank):
        points = this.generate_points_by_k10_rank(table_name, k10_rank)
        this.client.write_points(points)

    def save_k10_index(this, k10_index):
        measurement_name = 'k10_index'

        points = this.generate_points_by_k10_index(measurement_name, k10_index)
        this.client.write_points(points)

    def save_validation(this, validation):
        measurement_name = 'validation'

        points = this.generate_points_validation(measurement_name, validation)
        this.client.write_points(points)

    def update_db_re_gen_table_false(this):
        point = {
            'measurement': 're_generate_table',
            'time': get_timestamp_str(1526371200, 0),
            'fields': {
                'flag':  'false'
            }
        }
        this.client.write_points([ point ])
