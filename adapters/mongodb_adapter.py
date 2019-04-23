from pymongo import MongoClient

from .database_adapter import database_adapter
from lib.utility import *

class mongodb_adapter(database_adapter):
    COLLECTION_MARKET_INFO = 'market_info'

    def __init__(this, settings):
        super(mongodb_adapter, this).__init__(settings)

        this.client = None

    def open(this):
        if not this.client:
            this.client = MongoClient(host = this.host, port = this.port, username = this.username, password = this.password)

        return this.client

    def close(this):
        if this.client:
            this.client.close()
            this.client = None

    def save_tick(this, measurement_name, market_name, symbol_name, tick):
        pass

    def save_check(this, measurement_name, market_name, checkpoint):
        pass

    def save_index(this, measurement_name, index):
        pass

    def save_trade(this, measurement_name, market_name, symbol_name, trade):
        pass

    def save_market_info(this, market_name, market_info):
        current_second = long(time.time())
        with this.client.start_session(causal_consistency = True) as session:
            db = this.client[this.database]
            collection = db[this.COLLECTION_MARKET_INFO]
            market_info['market_name'] = market_name
            market_info['updated_at'] = current_second
            collection.replace_one(
                filter = { 'market_name': market_name }, 
                replacement = market_info, 
                upsert = True,
                session = session
            )

    def query(this, sql):
        pass

    def query_market_info(this, market_name):
        with this.client.start_session(causal_consistency = True) as session:
            db = this.client[this.database]
            collection = db[this.COLLECTION_MARKET_INFO]
            result= collection.find_one(
                filter = { 'market_name': market_name }, 
                session = session
            )
            return result


