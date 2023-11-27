USE ${hiveconf:SOURCE};

select min(sk), max(sk)
from 
(
    select ws_sold_date_sk as sk from web_sales
    union
    select wr_returned_date_sk as sk from web_returns
    union 
    select cs_sold_date_sk as sk from catalog_sales
    union 
    select cr_returned_date_sk as sk from catalog_returns
    union
    select ss_sold_date_sk as sk from store_sales
    union
    select sr_returned_date_sk as sk from store_returns
    union
    select inv_date_sk as sk from inventory
) t;