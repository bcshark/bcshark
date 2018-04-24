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

    def generate_point_by_dict(this, measurement_name, market_name, symbol_name, dict_obj):
        point = {
            'measurement': measurement_name,
            'tags': {
                'market': market_name,
                'symbol': symbol_name
            },
            'time': get_timestamp_str(dict_obj['time'], dict_obj['timezone_offset']),
            'fields': dict_obj
        }
        return [ point ]

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

    def generate_points_by_k20_rank(this, measurement_name, k20_ranks):
        points = [{
            'measurement': measurement_name,
            'tags': {
                'symbol': k20_rank.symbol
            },
            'time': get_timestamp_str(long(k20_rank.time), k20_rank.timezone_offset),
            'fields': {
                'time': long(k20_rank.time) + long(k20_rank.timezone_offset),
                'id': k20_rank.id,
                'name': k20_rank.name,
                'rank': int(k20_rank.rank),
                'price_usd': float(k20_rank.price_usd),
                'price_btc': float(k20_rank.price_btc),
                'volume_usd_24h': float(k20_rank.volume_usd_24h),
                'market_cap_usd': float(k20_rank.market_cap_usd),
                'available_supply': float(k20_rank.available_supply),
                'total_supply': float(k20_rank.total_supply),
                'max_supply': float(k20_rank.max_supply),
                'percent_change_1h': float(k20_rank.percent_change_1h),
                'percent_change_24h': float(k20_rank.percent_change_24h),
                'percent_change_7d': float(k20_rank.percent_change_7d),
                'period': k20_rank.period
            }
        } for k20_rank in k20_ranks]

        return points

    def generate_points_by_k20_index(this, measurement_name, k20_index):

        print('index.timezone_offset:', k20_index['timezone_offset'])
        print('index.high:', float(k20_index['high']))
        print('index.low:', float(k20_index['low']))
        print('index.open:', float(k20_index['open']))
        print('index.close:', float(k20_index['close']))
        print('index.time:', get_timestamp_str(long(k20_index['time']), k20_index['timezone_offset']))
        print('index.time:', long(k20_index['time']) + k20_index['timezone_offset'])
        print('index.period:', k20_index['period'])

        point = {
            'measurement': measurement_name,
            'tags': {
                'time': long(k20_index['time']) + k20_index['timezone_offset']
            },
            'time': get_timestamp_str(long(k20_index['time']), k20_index['timezone_offset']),
            'fields': {
                'time': long(k20_index['time']) + k20_index['timezone_offset'],
                'high': float(k20_index['high']),
                'low': float(k20_index['low']),
                'open': float(k20_index['open']),
                'close': float(k20_index['close']),
                'period': k20_index['period']
            }
        }
        return [ point ]

    def save_tick(this, measurement_name, market_name, symbol_name, tick):
        points = this.generate_point_by_dict(measurement_name, market_name, symbol_name, tick)

        this.client.write_points(points)

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        measurement_name = 'market_ticks'

        points = this.generate_points_by_ticks(measurement_name, market_name, symbol_name, ticks)

        this.client.write_points(points)

    def query(this, sql):
        result_set = this.client.query(sql)
        return result_set.raw

    def bulk_save_k20_daily_rank(this, k20_ranks):
        points = this.generate_points_by_k20_rank("k20_daily_rank", k20_ranks)
        this.client.write_points(points)

    def save_k20_index(this, k20_index):
        points = this.generate_points_by_k20_index("k20_index", k20_index)
        this.client.write_points(points)