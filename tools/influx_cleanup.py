import sys
import getopt

from influxdb import InfluxDBClient

def print_usage():
    print "%s --host <db ip> --port <db port> --database <db name> --username <username> --password <password>" % sys.argv[0]

def resolve_params():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:o:d:u:p:", [ "host=", "port=", "database=", "username=", "password=" ])

        for opt, arg in opts:
            if opt in ("-h", "--host"):
                db_host = arg
            elif opt in ("-o", "--port"):
                db_port = int(arg)
            elif opt in ("-d", "--database"):
                db_database = arg
            elif opt in ("-u", "--username"):
                db_username = arg
            elif opt in ("-p", "--password"):
                db_password = arg

        return (db_host, db_port, db_database, db_username, db_password)
    except getopt.GetoptError:
        return (None, None, None, None, None)

def open_influx_connection(db_host, db_port, db_database, db_username, db_password):
    db_conn = InfluxDBClient(host = db_host, port = db_port, database = db_database)
    if not filter(lambda db : db['name'] == db_database, db_conn.get_list_database()):
        db_conn.create_database(db_database) 

def close_influx_connection(db_conn):
    if db_conn:
        db_conn.close()

if __name__ == '__main__':
    (db_host, db_port, db_database, db_username, db_password) = resolve_params()

    if not db_host or not db_port or not db_database:
        print_usage()
        exit(1)

    db_conn = None

    try:
        db_conn = open_influx_connection(db_host, db_port, db_database, db_username, db_password)
    except Exception, e:
        print e
    finally:
        close_influx_connection(db_conn)

    """
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

    def save_trade(this, measurement_name, market_name, symbol_name, trade):
        points = [ this.generate_point_by_trade(measurement_name, market_name, symbol_name, trade) ]

        this.client.write_points(points)

    def save_tick(this, measurement_name, market_name, symbol_name, tick):
        points = [ this.generate_point_by_tick(measurement_name, market_name, symbol_name, tick) ]

        this.client.write_points(points)

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        measurement_name = 'market_ticks'

        points = [ this.generate_point_by_tick(measurement_name, market_name, symbol_name, tick) for tick in ticks ]

        this.client.write_points(points)

    def query(this, sql, epoch = None):
        result_set = this.client.query(sql, epoch = epoch)
        return result_set.raw

    def save_k10_daily_rank(this, table_name, k10_rank):
        points = this.generate_points_by_k10_rank(table_name, k10_rank)
        this.client.write_points(points)

    def save_k10_index(this, k10_index):
        points = this.generate_points_by_k10_index("k10_index", k10_index)
        this.client.write_points(points)
    """
