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
        k10_ranks = []

        for obj in objs:
            if obj["symbol"] == 'USDT':
                continue
            rank = k10_rank()
            rank.timezone_offset = this.timezone_offset
            rank.time = obj["last_updated"]
            rank.id = obj["id"]
            rank.name = obj["name"]
            rank.symbol = obj["symbol"]
            rank.rank = obj["rank"]
            rank.price_usd = obj["price_usd"]
            rank.price_btc = obj["price_btc"]
            rank.volume_usd_24h = obj["24h_volume_usd"]
            rank.market_cap_usd = obj["market_cap_usd"]
            rank.available_supply = obj["available_supply"]
            rank.total_supply = obj["total_supply"]
            if obj["max_supply"] == None:
                rank.max_supply = 0
            else:
                rank.max_supply = obj["max_supply"]
            rank.percent_change_1h = obj["percent_change_1h"]
            rank.percent_change_24h = obj["percent_change_24h"]
            rank.percent_change_7d = obj["percent_change_7d"]
            rank.period = this.DEFAULT_PERIOD

            k10_ranks.append(rank)

        return k10_ranks

    def collect_rest(this):

        data = this.http_request_json(this.REST_URL, None)

        if not data or not isinstance(data, list):
            this.logger.error('cannot get response from k10 daily rank')
            return

        k10_ranks = this.translate(data)
        this.bulk_save_k10_daily_rank(k10_ranks)

        this.logger.info('get response from k10 daily rank')

