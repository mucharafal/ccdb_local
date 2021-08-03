#!/bin/bash

cd `dirname $0`

psql -h $HOST -p $PORT -c "drop database ccdb" $DEFAULT_DATABASE $DEFAULT_USER
psql -h $HOST -p $PORT -c "create database ccdb" $DEFAULT_DATABASE $DEFAULT_USER

java -jar -Dmultimaster=true sql.jar &

sleep 5
echo "Started db"