#!/bin/bash

export HOST=localhost
export PORT=5433
export DEFAULT_USER=rmucha
export DEFAULT_DATABASE=postgres
export PGPASSWORD=password

cd `dirname $0`

bash init.sh
bash insert.sh
bash update.sh
bash insert.sh

pkill java