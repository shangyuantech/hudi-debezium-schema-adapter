#! /bin/sh

set -e

if [ -z $APP_LOG_LEVEL ]; then
  APP_LOG_LEVEL='DEBUG'
fi

exec java -jar -Dapp.log.level=${APP_LOG_LEVEL} ${SERVICE_HOME}/adapter-server.jar