class kline_service(object):
    def __init__(this, client, settings):
        this.client = client
        this.settings = settings

    def get_kline_by_time_range(this, start, end):
        result_set = this.client.query('select * from market_ticks')
        return result_set.raw

    def get_tvkline_by_market_symbol(this, symbol, from_time, to_time, size):
        sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high from k10_index where time >= %d and time <= %d group by time(1d) order by time desc limit %d" % (from_time * 1e9, to_time * 1e9, size)
        rows = this.client.query(sql, epoch = 's')
        print "%d, %d rows: %d" % (from_time, to_time, len(rows))
        return rows 

    def get_kline_by_market_symbol(this, market, symbol, size):
        if market == 'market_index':
            sql = "select time, first(open) as open, last(close) as close, min(low) as low, max(high) as high from k10_index group by time(1d) order by time desc limit %d" % size
        else:
            sql = "select time, open, close, low, high from %s_ticks where market = '%s' and symbol = '%s' order by time desc limit %d" % (market, market, symbol, size)
        rows = this.client.query(sql, epoch = 's')

        return rows 
