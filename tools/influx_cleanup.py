import sys
import getopt
import time

from influxdb import InfluxDBClient

TOP_N = 10

class coin_symbol(object):
    pass

class coin_symbol_price(object):
    pass

def print_usage():
    print "%s --host <db ip> --port <db port> --database <db name> --username <username> --password <password>" % sys.argv[0]

def resolve_params():
    try:
        db_host = None
        db_port = None
        db_database = None
        db_username = None
        db_password = None

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
    return db_conn

def translate_to_symbol(row):
    symbol = coin_symbol()
    symbol.time = row[0]
    symbol.symbol = row[1]
    symbol.rank = row[2]
    symbol.market_cap_usd = row[3]
    symbol.price_btc = row[4]
    symbol.price_usd = row[5]
    symbol.volume_usd_24h = row[6]
    return symbol

def get_first_and_last_timestamp(db_conn):
    # get maximal timestamp
    sql = "select time, * from market_ticks order by time desc limit 1"
    rows = db_conn.query(sql, epoch = 's').raw
    last_timestamp = rows['series'][0]['values'][0][0]

    # get minimal timestamp
    sql = "select time, * from market_ticks order by time limit 1"
    rows = db_conn.query(sql, epoch = 's').raw
    first_timestamp = rows['series'][0]['values'][0][0]

    return (first_timestamp, last_timestamp)

def get_top10_symbols(db_conn, begin_timestamp):
    end_timestamp = begin_timestamp + 60
    sql = "select time, symbol, rank, market_cap_usd, price_btc, price_usd, volume_usd_24h from k10_daily_rank where time >= %d and time < %d group by symbol" % (begin_timestamp * 1e9, end_timestamp * 1e9)
    rows = db_conn.query(sql, epoch = 's').raw

    if not rows.has_key('series'):
        return None
    else:
        symbols = []
        for row in rows['series']:
            symbols.append(translate_to_symbol(row['values'][0]))
        symbols.sort(lambda x, y: cmp(x.rank, y.rank))
        return symbols[:TOP_N]

def get_top10_symbols_ratio(db_conn, begin_timestamp, symbols):
    total = reduce(lambda last, current: last + current.market_cap_usd, [0,] + symbols)
    for symbol in symbols:
        symbol.ratio = 1.0 * symbol.market_cap_usd / total
    return symbols

def get_top10_symbols_prices(db_conn, begin_timestamp, symbols):
    end_timestamp = begin_timestamp + 60

    for symbol in symbols:
        tick_symbol_filter = "(symbol = %susdt or symbol = %sbtc)" % (symbol.symbol.lower(), symbol.symbol.lower())
        sql = "select time, symbol, market, open, close, high, low, volume from market_ticks where time >= %d and time < %d and %s group by symbol, market" % (begin_timestamp * 1e9, end_timestamp * 1e9, tick_symbol_filter)
        print sql
        rows = db_conn.query(sql, epoch = 's').raw

def save_top10_symbols_index(db_conn, timestamp, prices, period, symbol):
    pass

def close_influx_connection(db_conn):
    if db_conn:
        db_conn.close()

if __name__ == '__main__':
    (db_host, db_port, db_database, db_username, db_password) = resolve_params()

    if not db_host or not db_port or not db_database:
        print_usage()
        exit(1)

    db_conn = None

    previous_top10_symbols = None

    try:
        db_conn = open_influx_connection(db_host, db_port, db_database, db_username, db_password)

        (first_timestamp, last_timestamp) = get_first_and_last_timestamp(db_conn)

        for first_timestamp in range(first_timestamp, last_timestamp, 60):
            total_high = 0
            total_low = 0
            total_open = 0
            total_close = 0

            symbols = get_top10_symbols(db_conn, first_timestamp)

            if symbols == None:
                continue

            symbols = get_top10_symbols_ratio(db_conn, first_timestamp, symbols)
            symbols = get_top10_symbols_prices(db_conn, first_timestamp, symbols)

            for symbol in symbols:
                total_high = total_high + symbol['ratio'] * sum(symbols['prices']['high']) / len(symbols['prices']['high'])
                total_low = total_low + symbol['ratio'] * sum(symbols['prices']['low']) / len(symbols['prices']['low'])
                total_open = total_open + symbol['ratio'] * sum(symbols['prices']['open']) / len(symbols['prices']['open'])
                total_close = total_close + symbol['ratio'] * sum(symbols['prices']['close']) / len(symbols['prices']['close'])

            save_top10_symbols_index(db_conn, first_timestamp, (total_high, total_low, total_open, total_close), period, symbol)

            print "%s\thigh: %f, low: %f, open: %f, close: %f" % (time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(first_timestamp)), total_high, total_low, total_open, total_close)
    except Exception, e:
        print e
    finally:
        close_influx_connection(db_conn)
