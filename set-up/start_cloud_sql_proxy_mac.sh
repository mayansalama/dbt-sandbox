#!/usr/bin/env bash
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.amd64
chmod +x cloud_sql_proxy

./cloud_sql_proxy -instances=$1:dbt-sandbox=tcp:5433