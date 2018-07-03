import re

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
        if market == 'market_index':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high, sum(volume) as volume from k10_index group by time(%s) order by time desc limit %d" % (this.get_influx_timegroup_by_resolution(resolution), size)
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
