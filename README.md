Coin Market
----------------------------------------

* Author: Yun (igouzy@live.com)

* Author: Lix.Yan

### Backup & Restore Influxdb ###

backup: 

    influxd backup ./influxdb
    influxd backup -database market_index ./market_index

restore:

    influxd restore -metadir /var/lib/influxdb/meta ./influxdb
    influxd restore -database market_index -datadir /var/lib/influxdb/data ./market_index

### Run with Docker ###

docker run --name CoinMarket.Web -d -p 80:80 -v ~/Projects/CoinMarket/web/test/:/usr/share/nginx/html:ro nginx:stable

### Scripts included ###

* Gentelella: https://github.com/puikinsh/gentelella
