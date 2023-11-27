#!/bin/sh

# retrieve min/max by executing dateskRange.hql
min_sk=2450815
max_sk=2453002

for ((sk=min_sk; sk<=max_sk; sk++))
do 
    echo ${sk}
    ~/Trino-Cli/target/trino-cli-1.0-executable.jar --server=localhost:9090 --catalog=hive -f ddl/createAllORCTables.hql -d DATESK=${sk} -d ORCDBNAME=hive_tpcds100g_partition_orc -d SOURCE=hive_tpcds100g_parquet
done 