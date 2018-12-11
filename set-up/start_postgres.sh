#!/usr/bin/env bash

export PGDATA='/usr/local/var/postgres'
export PGPASSWORD=admin

pg_ctl start

createuser admin -s
createdb dbtsandbox
psql dbtsandbox -c 'CREATE SCHEMA source;'