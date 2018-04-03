from influxdb import SeriesHelper

class series_helper(SeriesHelper):
    def __init__(this, influxdb_client):
        global client

        client = influxdb_client

    class Meta:
        global client
        series_name = 'market.{market_name}.ticks'
        fields = [ 'time', 'open', 'close', 'low', 'high', 'amount', 'volume', 'count' ]
        tags = [ 'market_name' ]
        bulk_size = 200
        auto_commit = True

