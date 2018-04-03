from .huobi import collector_huobi

class collector_factory(object):
    def __init__(this, settings):
        this.settings = settings

    def get_collector(this, collector_name):
        collector = None

        if collector_name == 'huobi':
            collector = collector_huobi(this.settings)

        return collector

    def get_all_collectors(this):
        collectors = [ this.get_collector('huobi') ]

        return collectors

