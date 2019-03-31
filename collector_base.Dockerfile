FROM python:2
LABEL author="igouzy@live.com"

RUN apt-get update && apt-get -y install cron vim
RUN pip install flask flask_cors influxdb requests signalr-client

RUN mkdir -p /apps

WORKDIR /apps
