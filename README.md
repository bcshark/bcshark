# Coin Market #

* Author: Yun (igouzy@live.com)
* Author: Lix.Yan

## Backup & Restore Influxdb ##

backup:

```bash
influxd backup ./influxdb
influxd backup -database market_index ./market_index
```

restore:

```bash
influxd restore -metadir /var/lib/influxdb/meta ./influxdb
influxd restore -database market_index -datadir /var/lib/influxdb/data ./market_index
```

## Run with Docker ##

```bash
docker run --name CoinMarket.Influx -d -p 8086:8086 -v `pwd`/influxdb:/var/lib/influxdb influxdb:latest
docker run --name CoinMarket.Collector -it --link=CoinMarket.Influx:influxdb -p 5000:5000 -v `pwd`/CoinMarket:/home/ python:2 /bin/bash
docker run --name CoinMarket.Web -v `pwd`/CoinMarket/web/latest/:/home -p 80:80 -it python:2 /bin/bash
```

If you plan to use nginx as reverse proxy, create a nginx container with following command:

    docker run --name CoinMarket.Web -d -p 80:80 -v `pwd`/CoinMarket/web/latest/:/usr/share/nginx/html:ro -v `pwd`/config/nginx.conf:/etc/nginx/conf.d/default.conf --link CoinMarket.BBS:bbs nginx:stable

## Misc ##

If you get error when using SignalR fetch data from bittrex `[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed`, change source code of SignalR.

    sudo vim /usr/lib/python2.7/site-packages/signalr/transports/_ws_transport.py

    -- line 38 --
    self.ws = create_connection(ws_url,
                                header=self.__get_headers(),
                                cookie=self.__get_cookie_str(),
                                enable_multithread=True,
                                sslopt = { "cert_reqs" : ssl.CERT_NONE })

If you install Python from source, use following configure command:

    ./configure --enable-optimizations --enable-loadable-sqlite-extensions --enable-shared --enable-unicode=ucs4

## Scripts included ##

* Gentelella: https://github.com/puikinsh/gentelella
