#!/bin/sh

# set -v

# retrieve min/max by executing dateskRange.hql
min_sk=2450815
max_sk=2453002
stride=50

min() {
    if [ $1 -le $2 ]; then
        echo $1
    else
        echo $2
    fi
}

for ((sk=min_sk; sk<=max_sk; sk=sk+stride))
do 
    sk_lower=$sk
    sk_upper=`expr $sk + $stride - 1`
    sk_upper=$(min $sk_upper $max_sk)
    echo LB=${sk_lower} UB=${sk_upper}
    ~/Trino-Cli/target/trino-cli-1.0-executable.jar --server=localhost:9090 --catalog=hive -f ddl/createAllORCTables.hql -d DATELB=${sk} -d DATEUB=${sk} -d ORCDBNAME=hive_tpcds100g_partition_orc -d SOURCE=hive_tpcds100g_parquet
done 