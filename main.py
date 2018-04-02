import logging

from collectors.collector_factory import collector_factory

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    global factory
    global settings

    settings = { 'logger': logger }
    factory = collector_factory(settings)

    collectors = factory.get_all_collectors()
    for collector in collectors:
        collector.collect()

