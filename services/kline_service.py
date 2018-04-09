class kline_service(object):
    def __init__(this, client):
        this.client = client

    def get_kline_by_time_range(this, start, end):
        result_set = this.client.query('select * from market_ticks')
        return result_set.raw

    def get_kline_by_market_symbol(this, market, symbol, size):
        sql = "select time, open, close, low, high from market_ticks where market = '%s' and symbol = '%s' order by time desc limit %d" % (market, symbol, size)
        rows = this.client.query(sql)

        return rows 
