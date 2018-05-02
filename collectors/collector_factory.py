from .huobi import collector_huobi
from .binance import collector_binance
from .okex import collector_okex
from .poloniex import collector_poloniex
from .okcoin import collector_okcoin
from .gdax import collector_gdax
from .bitfinex import collector_bitfinex
from .bitstamp import collector_bitstamp
from .bittrex import collector_bittrex
from .gateio import collector_gateio
from .k10_daily_rank import collector_k10_daily_rank
from .k10_index_calc import collector_k10_index_calc

class collector_factory(object):
    def __init__(this, settings):
        this.settings = settings

    def get_market_settings(this, market_name):
        markets = this.settings['markets']

        if markets.has_key(market_name):
            return markets[market_name]

        return None

    def get_collector(this, market_name):
        collector = None
        market_settings = this.get_market_settings(market_name)

        if market_name == 'huobi':
            collector = collector_huobi(this.settings, market_settings)
        elif market_name == 'binance':
            collector = collector_binance(this.settings, market_settings)
        elif market_name == 'okex':
            collector = collector_okex(this.settings, market_settings)
        elif market_name == 'poloniex':
            collector = collector_poloniex(this.settings, market_settings)
        elif market_name == 'okcoin':
            collector = collector_okcoin(this.settings, market_settings)
        elif market_name == 'gdax':
            collector = collector_gdax(this.settings, market_settings)
        elif market_name == 'bitfinex':
            collector = collector_bitfinex(this.settings, market_settings)
        elif market_name == 'bitstamp':
            collector = collector_bitstamp(this.settings, market_settings)
        elif market_name == 'bittrex':
            collector = collector_bittrex(this.settings, market_settings)
        elif market_name == 'gateio':
            collector = collector_gateio(this.settings, market_settings)
        elif market_name == 'k10_daily_rank':
            collector = collector_k10_daily_rank(this.settings, market_settings)
        elif market_name == 'k10_index_calc':
            collector = collector_k10_index_calc(this.settings, market_settings)

        return collector

    def get_all_rest_collectors(this):
        markets = this.settings['markets']
        collectors = []

        for market, settings in markets.items():
            if settings.has_key('api') and settings['api'].has_key('rest'):
                collectors.append(this.get_collector(market))

        return collectors

    def get_all_ws_collectors(this):
        markets = this.settings['markets']

        collectors = []

        for market, settings in markets.items():
            if settings.has_key('api') and settings['api'].has_key('ws'):
                collectors.append(this.get_collector(market))

        return collectors

