import re
import time

class kline_service(object):
    def __init__(this, client, settings):
        this.client = client
        this.settings = settings

    def get_kline_by_time_range(this, start, end):
        result_set = this.client.query('select * from market_ticks')
        return result_set.raw

    def get_influx_timegroup_by_resolution(this, resolution):
        match = re.search(r'^([0-9]+)$', resolution)

        if match:
            sql_time_group = '%sm' % match.group(1)
        elif resolution == 'D':
            sql_time_group = "1d"
        elif resolution == 'W':
            sql_time_group = "1w"
        elif resolution == 'M':
            sql_time_group = "30d"
        else:
            sql_time_group = "1d"

        return sql_time_group

    def get_tvkline_by_market_symbol(this, symbol, from_time, to_time, resolution, size):
        #sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high from k10_index where time >= %d and time <= %d group by time(%s) order by time desc limit %d" % (from_time * 1e9, to_time * 1e9, this.get_influx_timegroup_by_resolution(resolution), size)

        #NOTE: ignore limits as tradingview will alwasy send from_time and to_time in a reasonable range
        symbol = symbol.lower()
        if not symbol == 'index' and not symbol == 'innovation':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from market_ticks where symbol = '%s' and market = 'binance' and time >= %d and time <= %d group by time(%s) order by time desc" % (symbol, from_time * 1e9, to_time * 1e9, this.get_influx_timegroup_by_resolution(resolution))
        elif symbol == 'innovation':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from k30_index where time >= %d and time <= %d group by time(%s) order by time desc" % (from_time * 1e9, to_time * 1e9, this.get_influx_timegroup_by_resolution(resolution))
        else:
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from k10_index where time >= %d and time <= %d group by time(%s) order by time desc" % (from_time * 1e9, to_time * 1e9, this.get_influx_timegroup_by_resolution(resolution))
        rows = this.client.query(sql, epoch = 's')
        print "%d, %d rows: %d" % (from_time, to_time, len(rows))
        return rows 

    def get_kline_by_market_symbol(this, market, symbol, resolution, size):
        if symbol == 'index':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from k10_index group by time(%s) order by time desc limit %d" % (this.get_influx_timegroup_by_resolution(resolution), size)
        elif symbol == 'innovation':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from k30_index group by time(%s) order by time desc limit %d" % (this.get_influx_timegroup_by_resolution(resolution), size)
        else:
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from %s_ticks where market = '%s' and symbol = '%s' group by time(%s) order by time desc limit %d" % (market, market, symbol, this.get_influx_timegroup_by_resolution(resolution), size)
        rows = this.client.query(sql, epoch = 's')

        return rows 

    def get_trend_by_symbol(this, symbol, from_time, to_time, resolution):
        sql = "select (mean(low) + mean(high)) / 2 as price from market_ticks where symbol = '%s' and time >= %d and time <= %d group by time(%s) order by time desc" % (symbol, from_time * 1e9, to_time * 1e9, this.get_influx_timegroup_by_resolution(resolution))
        rows = this.client.query(sql, epoch = 's')

        return rows 

    def query_symbol_daily_rank(this, from_time, to_time, count):
        sql = "select time, symbol, price_usd, rank, /name/, market_cap_usd, percent_change_24h, volume_usd_24h from k10_daily_rank where time >= %d and time <= %d order by time desc limit %d" % (from_time * 1e9, to_time * 1e9, 29)
        result = this.client.query(sql, epoch = 's')
        if len(result) == 0 or not result.has_key('series'):
            return None

        ranks = result['series'][0]['values']
        # remove dupliated symbols (same name)
        ranks = reduce(lambda collection, item: filter(lambda x: x[4] != item[4], collection) + [item], [[],] + ranks)
        ranks.sort(lambda x, y: cmp(x[3], y[3]))

        return ranks[:count]

    def query_latest_index(this, index_type, size):
        if index_type == 'k30' or index_type == 'K30':
            sql = "select time, open, close, low, high, volume from k30_index order by time desc limit %d" % (size)
        else:
            sql = "select time, open, close, low, high, volume from k10_index order by time desc limit %d" % (size)
        rows = this.client.query(sql, epoch = 's')

        return rows

    def query_monitor(this):
        sql = "select time, market, symbol, update_time from monitor"
        print('Monitor SQL: %s', sql)
        result = this.client.query(sql, epoch = 's')
        if len(result) == 0 or not result.has_key('series'):
            return None

        monitors = result['series'][0]['values']
        return monitors

    def query_validation(this, start, end, market, symbol):
        sql = "select time, market, symbol, msg from validation where time >= %d and time <= %d" % (start * 1e9, end * 1e9)
        if market != '':
            sql = sql + " and market = '%s'" % (market)
        if symbol != '':
            sql = sql + " and symbol = '%s'" % (symbol)
        print('Validation SQL: %s', sql)
        result = this.client.query(sql, epoch = 's')
        if len(result) == 0 or not result.has_key('series'):
            return None

        validations = result['series'][0]['values']
        return validations

    def query_datasync(this, table, start, end):
        sql = ""
        if table == "market_ticks":
            cur_time = time.time()
            base_time1 = int(cur_time - cur_time % 86400) + 28800
            base_time2 = base_time1 - 60
            sql = "select time, amount, close, count, high, low, market, open, period, symbol, timezone_offset, volume from market_ticks where time >= %d and time <= %d" % (base_time2 * 1e9, base_time1 * 1e9)
        elif table == "k10_index":
            sql = "select time, close, high, low, open, period, symbol, volume from k10_index where time >= %d and time <= %d" % (start * 1e9, end * 1e9)
        elif table == "k30_index":
            sql = "select time, close, high, low, open, period, symbol, volume from k30_index where time >= %d and time <= %d" % (start * 1e9, end * 1e9)
        elif table == "k10_daily_rank":
            sql = "select time, id, market_cap_usd, max_supply, \"name\", percent_change_1h, percent_change_24h, percent_change_7d, period, price_usd, rank, symbol, total_supply, volume_usd_24h from k10_daily_rank where time >= %d and time <= %d" % (start * 1e9, end * 1e9)
        print('datasync SQL: %s', sql)
        result = this.client.query(sql, epoch = 's')
        if len(result) == 0 or not result.has_key('series'):
            return None

        datasyncs = result['series'][0]['values']
        return datasyncs