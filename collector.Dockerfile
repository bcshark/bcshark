FROM bityun.azurecr.io/coinmarket-collector-base:1.0
LABEL author="igouzy@live.com"

RUN mkdir -p /apps/logs /apps/data-files/cache /apps/config-files

ADD adapters /apps/adapters/
ADD collectors /apps/collectors/
ADD config /apps/config/
ADD lib /apps/lib/
ADD services /apps/services/
ADD tools /apps/tools/
ADD config-files /apps/config-files/

ADD *.py /apps/

WORKDIR /apps
