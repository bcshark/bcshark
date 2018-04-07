class kline_service(object):
    def __init__(this, client):
        this.client = client

    def get_kline_by_time_range(this, start, end):
        result_set = this.client.query('select * from ticks_huobi')
        return result_set.raw

    def get_kline_by_market_symbol(this, market, symbol, size):
        sql = "select * from ticks_huobi where market = '%s' and symbol = '%s' limit %d" % (market, symbol, size)
        result_set = this.client.query(sql)
        return result_set.raw
