import requests
import datetime

from model.k20_rank import k20_rank
from .collector import collector
from .utility import *

class collector_k20_daily_rank(collector):
    DEFAULT_PERIOD = "1day"

    def __init__(this, settings, market_settings):
        super(collector_k20_daily_rank, this).__init__(settings, market_settings)

    def translate(this, objs):
        k20_ranks = []

        for obj in objs:
            rank = k20_rank()
            rank.timezone_offset = this.timezone_offset
            rank.time = obj["last_updated"]
            rank.id = obj["id"]
            rank.name = obj["name"]
            rank.symbol = obj["symbol"]
            rank.rank = obj["rank"]
            rank.price_usd = obj["price_usd"]
            rank.price_btc = obj["price_btc"]
            rank.volume_usd_24h = obj["volume_usd_24h"]
            rank.market_cap_usd = obj["market_cap_usd"]
            rank.available_supply = obj["available_supply"]
            rank.total_supply = obj["total_supply"]
            rank.max_supply = obj["max_supply"]
            rank.percent_change_1h = obj["percent_change_1h"]
            rank.percent_change_24h = obj["percent_change_24h"]
            rank.percent_change_7d = obj["percent_change_7d"]
            rank.period = this.DEFAULT_PERIOD

            k20_ranks.append(rank)

        return k20_ranks

    def collect_rest(this):

        url = "https://api.coinmarketcap.com/v1/ticker/?limit=20"
        data = this.http_request_json(url, None)

        if not data or not isinstance(data, list):
            this.logger.error('cannot get response from k20 daily rank')
            return

        k20_ranks = this.translate(data)
        this.bulk_save_k20_daily_rank('k20', k20_ranks)

        this.logger.info('get response from k20 daily rank')

