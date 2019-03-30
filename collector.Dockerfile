FROM python:2
LABEL author="igouzy@live.com"

RUN pip install flask flask_cors influxdb requests signalr-client

RUN mkdir -p /apps/logs /apps/data-files /apps/config-files
COPY adapters collectors config lib services tools *.py /apps

WORKDIR /apps
