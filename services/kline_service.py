class kline_service(object):
    def __init__(this, client):
        this.client = client

    def get_kline_by_time_range(this, start, end):
        result_set = this.client.query('select * from market_ticks')
        return result_set.raw

    def get_kline_by_market_symbol(this, market, symbol, size):
        sql = "select timestamp as time, open, close, low, high from market_ticks where market = '%s' and symbol = '%s' order by timestamp desc limit %d" % (market, symbol, size)
        rows = this.client.query(sql)
        ticks = [
            list(row) for row in rows             
        ]
        ticks.sort(lambda x, y: int(x[0] - y[0]))
        result = {
            'series': [
                {
                    'columns': [ 'time', 'open', 'close', 'low', 'high' ],
                    'values': ticks
                }
            ]
        }

        return result 
