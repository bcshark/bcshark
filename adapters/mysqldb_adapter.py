import MySQLdb

from .database_adapter import database_adapter
from .utility import *

class mysqldb_adapter(database_adapter):
    def __init__(this, settings):
        super(mysqldb_adapter, this).__init__(settings)

        this.client = None

    def open(this):
        if not this.client:
            this.client = MySQLdb.connect(host = this.host, port = this.port, db = this.database, user = this.username, passwd = this.password)

        return this.client

    def close(this):
        if this.client:
            this.client.close()

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        cursor = this.client.cursor()
        values = [
            "(%s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s')" % (
                tick.time + tick.timezone_offset,
                tick.open,
                tick.close,
                tick.high,
                tick.low,
                tick.amount,
                tick.volume,
                tick.count,
                market_name,
                symbol_name
            )
            for tick in ticks         
        ]
        seperator = ","

        cursor.execute("insert into market_ticks(timestamp, open, close, high, low, amount, volume, count, market, symbol) values %s" % seperator.join(values))
        cursor.close()

    def query(this, sql):
        cursor = this.client.cursor()
        cursor.execute(sql)
        result_set = cursor.fetchall()
        print result_set
        cursor.close()
        return result_set.raw
