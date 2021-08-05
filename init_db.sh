#!/bin/bash

# Needs clear database. Simply execute
# psql -c "drop database ccdb ; create database ccdb;"

cd `dirname $0`

./package/package.sh

nohup java -jar -Dmultimaster=true package/sql.jar &

sleep 5 && pkill java
