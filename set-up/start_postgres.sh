#!/usr/bin/env bash

export PGDATA='/usr/local/var/postgres'
pg_ctl start

createuser admin -s
createdb dbtsandbox -U admin -W admin
