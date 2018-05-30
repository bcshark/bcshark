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

    docker run --name CoinMarket.Web -d -p 80:80 -v `pwd`/CoinMarket/web/latest/:/usr/share/nginx/html:ro nginx:stable
    docker run --name CoinMarket.Influx -d -p 8086:8086 -v `pwd`/influxdb:/var/lib/influxdb influxdb:latest
    docker run --name CoinMarket.Collector -it --link=CoinMarket.Influx:influxdb -p 5000:5000 -v `pwd`/CoinMarket:/home/ python:2 /bin/bash

### Scripts included ###

* Gentelella: https://github.com/puikinsh/gentelella
