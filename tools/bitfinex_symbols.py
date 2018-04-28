import requests
import json
import time

DEFAULT_TIMEOUT_IN_SECONDS = 60
DEFAULT_OUTPUT_FILE = 'bitfinex.matches.txt'
DEFAULT_WATCH_SYMBOLS = ['BTC', 'USDT', 'USD']

check_symbol_list = ['EOS', 'FIL', 'ADA', 'ZIL', 'ONT', 'ELF', 'AE', 'KNC', 'AGI', 'MANA', 'POWR', 'ENG', 'THETA', 'IOST', 'BLZ', 'ELA', 'BRD', 'ICX', 'DDD', 'ZRX', 'CVC', 'WAN', 'AION', 'DRGN', 'RDN', 'MOBI', 'RUFF', 'GNX', 'LRC', 'SNT', 'VEN', 'NANO', 'WTC', 'GNT', 'LOOM']

def http_request_json(url, headers = None, cookies = None):
    try:
        res = requests.get(url, headers = headers, cookies = cookies, timeout = DEFAULT_TIMEOUT_IN_SECONDS, allow_redirects = True)

        return res.json()
    except Exception, e:
        print e
        return None

if __name__ == '__main__':
    support_symbols = http_request_json('https://api.bitfinex.com/v2/tickers?symbols=ALL&_=%d' % (time.time() * 1000))

    with open(DEFAULT_OUTPUT_FILE, 'wr') as out_file:
        for index in range(0, len(check_symbol_list)):
            symbol = check_symbol_list[index]
            ret = []

            matches = filter(lambda x: x[0] == 't%sUSD' % symbol, support_symbols)
            if matches:
                ret.append('USD')

            matches = filter(lambda x: x[0] == 't%sBTC' % symbol, support_symbols)
            if matches:
                ret.append('BTC')

            if ret:
                out_file.write('%s    %s\n' % (symbol, '/'.join(ret)))
            else:
                out_file.write('%s    %s\n' % (symbol, 'NA'))


