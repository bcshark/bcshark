from .huobi import collector_huobi
from .binance import collector_binance

class collector_factory(object):
    def __init__(this, settings):
        this.settings = settings

    def get_collector(this, collector_name):
        collector = None

        if collector_name == 'huobi':
            collector = collector_huobi(this.settings)
        elif collector_name == 'binance':
            collector = collector_binance(this.settings)

        return collector

    def get_all_collectors(this):
        collectors = [ this.get_collector('huobi'), this.get_collector('binance') ]

        return collectors

