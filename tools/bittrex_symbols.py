import requests
import json

DEFAULT_TIMEOUT_IN_SECONDS = 60
DEFAULT_OUTPUT_FILE = 'bittrex.matches.txt'
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
    json_obj = http_request_json('https://www.bittrex.com/api/v2.0/pub/Markets/GetMarketSummaries')
    support_symbols = json_obj['result']

    with open(DEFAULT_OUTPUT_FILE, 'wr') as out_file:
        for index in range(0, len(check_symbol_list)):
            symbol = check_symbol_list[index]
            matches = filter(lambda x: x['Market']['MarketCurrency'] == symbol and x['Market']['BaseCurrency'] in DEFAULT_WATCH_SYMBOLS, support_symbols)
            ret = '/'.join([match['Market']['BaseCurrency'] for match in matches])
            if not ret:
                ret = 'NA'

            out_file.write('%s    %s\n' % (symbol, ret))


