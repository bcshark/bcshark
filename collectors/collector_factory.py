from .huobi import collector_huobi
from .binance import collector_binance
from .okex import collector_okex
from .poloniex import collector_poloniex
from .okcoin import collector_okcoin
from .gdax import collector_gdax

class collector_factory(object):
    def __init__(this, settings):
        this.settings = settings

    def get_collector(this, collector_name):
        collector = None

        if collector_name == 'huobi':
            collector = collector_huobi(this.settings)
        elif collector_name == 'binance':
            collector = collector_binance(this.settings)
        elif collector_name == 'okex':
            collector = collector_okex(this.settings)
        elif collector_name == 'poloniex':
            collector = collector_poloniex(this.settings)
        elif collector_name == 'okcoin':
            collector = collector_okcoin(this.settings)
        elif collector_name == 'gdax':
            collector = collector_gdax(this.settings)

        return collector

    def get_all_collectors(this):
        markets = this.settings['markets']

        collectors = []

        for market in markets:
            collectors.append(market)

        return collectors

