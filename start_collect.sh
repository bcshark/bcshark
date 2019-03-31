#!/bin/sh

MARKET_NAME=$1

echo "start collecting market index at `date '+%Y-%m-%d %H:%M:%S'`"

echo "replace crontab with MARKET_NAME=${MARKET_NAME}..."
sed -i "s/#MARKET_NAME#/${MARKET_NAME}/g" /etc/crontab

echo "run cron as foreground process..."
/usr/sbin/cron -f
