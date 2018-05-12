import requests
import datetime

from model.k10_rank import k10_rank
from .collector import collector
from .utility import *

class collector_k10_daily_rank(collector):
    DEFAULT_PERIOD = "1min"

    @property
    def market_name(this):
        return "k10_daily_rank"

    def __init__(this, settings, market_settings):
        super(collector_k10_daily_rank, this).__init__(settings, market_settings)

    def translate(this, objs):
        rank = k10_rank()
        obj = objs['data']
        rank.timezone_offset = this.timezone_offset
        update_time = long(obj["last_updated"])
        rank.time = update_time - update_time % 100
        rank.id = obj["id"]
        rank.name = obj["name"]
        rank.symbol = obj["symbol"]
        rank.rank = obj["rank"]
        rank.price_usd = obj['quotes']['USD']["price"]
        rank.volume_usd_24h = obj['quotes']['USD']["volume_24h"]
        rank.market_cap_usd = obj['quotes']['USD']["market_cap"]
        rank.total_supply = obj["total_supply"]
        if obj["max_supply"] == None:
            rank.max_supply = 0
        else:
            rank.max_supply = obj["max_supply"]
        rank.percent_change_1h = obj['quotes']['USD']["percent_change_1h"]
        rank.percent_change_24h = obj['quotes']['USD']["percent_change_24h"]
        rank.percent_change_7d = obj['quotes']['USD']["percent_change_7d"]
        rank.period = this.DEFAULT_PERIOD

        return rank

    def collect_rest(this):
        for symbol in this.symbols_market:
            if symbol == '':
                continue
            url = this.REST_URL + symbol
            data = this.http_request_json(url, None)
            if not data or not isinstance(data, dict):
                this.logger.error('cannot get response from k10 daily rank')
                return

            k10_ranks = this.translate(data)
            this.save_k10_daily_rank(k10_ranks)

            this.logger.info('get response from k10 daily rank')
