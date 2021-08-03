#!/bin/bash

START_DIR=`pwd`

cd $START_DIR/../../package

./package.sh

echo "Copy to $START_DIR"

cp sql.jar $START_DIR