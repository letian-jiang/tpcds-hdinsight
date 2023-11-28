-- do not drop, as this script will be executed repeatively
-- DROP DATABASE IF EXISTS ${ORCDBNAME} CASCADE;
CREATE SCHEMA IF NOT EXISTS ${ORCDBNAME};

USE ${ORCDBNAME};

create table if not exists inventory
(
    inv_item_sk          bigint,
    inv_warehouse_sk        bigint,
    inv_quantity_on_hand    int,
    inv_date_sk          bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['inv_date_sk']
);

insert into inventory
select
    i.inv_item_sk,
    i.inv_warehouse_sk,
    i.inv_quantity_on_hand,
    i.inv_date_sk
from ${SOURCE}.inventory i
where i.inv_date_sk >= ${DATELB} and i.inv_date_sk <= ${DATEUB};

create table if not exists catalog_returns
(
    cr_returned_time_sk       bigint,
    cr_item_sk                bigint,
    cr_refunded_customer_sk   bigint,
    cr_refunded_cdemo_sk      bigint,
    cr_refunded_hdemo_sk      bigint,
    cr_refunded_addr_sk       bigint,
    cr_returning_customer_sk  bigint,
    cr_returning_cdemo_sk     bigint,
    cr_returning_hdemo_sk     bigint,
    cr_returning_addr_sk      bigint,
    cr_call_center_sk         bigint,
    cr_catalog_page_sk        bigint,
    cr_ship_mode_sk           bigint,
    cr_warehouse_sk           bigint,
    cr_reason_sk              bigint,
    cr_order_number           bigint,
    cr_return_quantity        int,
    cr_return_amount          decimal(7, 2),
    cr_return_tax             decimal(7, 2),
    cr_return_amt_inc_tax     decimal(7, 2),
    cr_fee                    decimal(7, 2),
    cr_return_ship_cost       decimal(7, 2),
    cr_refunded_cash          decimal(7, 2),
    cr_reversed_charge        decimal(7, 2),
    cr_store_credit           decimal(7, 2),
    cr_net_loss               decimal(7, 2),
    cr_returned_date_sk       bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['cr_returned_date_sk']
);

insert into catalog_returns
select
        cr.cr_returned_time_sk,
        cr.cr_item_sk,
        cr.cr_refunded_customer_sk,
        cr.cr_refunded_cdemo_sk,
        cr.cr_refunded_hdemo_sk,
        cr.cr_refunded_addr_sk,
        cr.cr_returning_customer_sk,
        cr.cr_returning_cdemo_sk,
        cr.cr_returning_hdemo_sk,
        cr.cr_returning_addr_sk,
        cr.cr_call_center_sk,
        cr.cr_catalog_page_sk,
        cr.cr_ship_mode_sk,
        cr.cr_warehouse_sk,
        cr.cr_reason_sk,
        cr.cr_order_number,
        cr.cr_return_quantity,
        cr.cr_return_amount,
        cr.cr_return_tax,
        cr.cr_return_amt_inc_tax,
        cr.cr_fee,
        cr.cr_return_ship_cost,
        cr.cr_refunded_cash,
        cr.cr_reversed_charge,
        cr.cr_store_credit,
        cr.cr_net_loss,
        cr.cr_returned_date_sk 
from ${SOURCE}.catalog_returns cr
where cr.cr_returned_date_sk >= ${DATELB} and cr.cr_returned_date_sk <= ${DATEUB};

create table if not exists catalog_sales
(
    cs_sold_time_sk           bigint,
    cs_ship_date_sk           bigint,
    cs_bill_customer_sk       bigint,
    cs_bill_cdemo_sk          bigint,
    cs_bill_hdemo_sk          bigint,
    cs_bill_addr_sk           bigint,
    cs_ship_customer_sk       bigint,
    cs_ship_cdemo_sk          bigint,
    cs_ship_hdemo_sk          bigint,
    cs_ship_addr_sk           bigint,
    cs_call_center_sk         bigint,
    cs_catalog_page_sk        bigint,
    cs_ship_mode_sk           bigint,
    cs_warehouse_sk           bigint,
    cs_item_sk                bigint,
    cs_promo_sk               bigint,
    cs_order_number           bigint,
    cs_quantity               int,
    cs_wholesale_cost         decimal(7, 2),
    cs_list_price             decimal(7, 2),
    cs_sales_price            decimal(7, 2),
    cs_ext_discount_amt       decimal(7, 2),
    cs_ext_sales_price        decimal(7, 2),
    cs_ext_wholesale_cost     decimal(7, 2),
    cs_ext_list_price         decimal(7, 2),
    cs_ext_tax                decimal(7, 2),
    cs_coupon_amt             decimal(7, 2),
    cs_ext_ship_cost          decimal(7, 2),
    cs_net_paid               decimal(7, 2),
    cs_net_paid_inc_tax       decimal(7, 2),
    cs_net_paid_inc_ship      decimal(7, 2),
    cs_net_paid_inc_ship_tax  decimal(7, 2),
    cs_net_profit             decimal(7, 2),
    cs_sold_date_sk           bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['cs_sold_date_sk']
);

insert into catalog_sales
select
        cs.cs_sold_time_sk,
        cs.cs_ship_date_sk,
        cs.cs_bill_customer_sk,
        cs.cs_bill_cdemo_sk,
        cs.cs_bill_hdemo_sk,
        cs.cs_bill_addr_sk,
        cs.cs_ship_customer_sk,
        cs.cs_ship_cdemo_sk,
        cs.cs_ship_hdemo_sk,
        cs.cs_ship_addr_sk,
        cs.cs_call_center_sk,
        cs.cs_catalog_page_sk,
        cs.cs_ship_mode_sk,
        cs.cs_warehouse_sk,
        cs.cs_item_sk,
        cs.cs_promo_sk,
        cs.cs_order_number,
        cs.cs_quantity,
        cs.cs_wholesale_cost,
        cs.cs_list_price,
        cs.cs_sales_price,
        cs.cs_ext_discount_amt,
        cs.cs_ext_sales_price,
        cs.cs_ext_wholesale_cost,
        cs.cs_ext_list_price,
        cs.cs_ext_tax,
        cs.cs_coupon_amt,
        cs.cs_ext_ship_cost,
        cs.cs_net_paid,
        cs.cs_net_paid_inc_tax,
        cs.cs_net_paid_inc_ship,
        cs.cs_net_paid_inc_ship_tax,
        cs.cs_net_profit,
        cs.cs_sold_date_sk
from ${SOURCE}.catalog_sales cs
where cs.cs_sold_date_sk >= ${DATELB} and cs.cs_sold_date_sk <= ${DATEUB};

create table if not exists store_returns
(
    sr_return_time_sk         bigint,
    sr_item_sk                bigint,
    sr_customer_sk            bigint,
    sr_cdemo_sk               bigint,
    sr_hdemo_sk               bigint,
    sr_addr_sk                bigint,
    sr_store_sk               bigint,
    sr_reason_sk              bigint,
    sr_ticket_number          bigint,
    sr_return_quantity        int,
    sr_return_amt             decimal(7, 2),
    sr_return_tax             decimal(7, 2),
    sr_return_amt_inc_tax     decimal(7, 2),
    sr_fee                    decimal(7, 2),
    sr_return_ship_cost       decimal(7, 2),
    sr_refunded_cash          decimal(7, 2),
    sr_reversed_charge        decimal(7, 2),
    sr_store_credit           decimal(7, 2),
    sr_net_loss               decimal(7, 2),
    sr_returned_date_sk       bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['sr_returned_date_sk']
);


insert into store_returns
select
        sr.sr_return_time_sk,
        sr.sr_item_sk,
        sr.sr_customer_sk,
        sr.sr_cdemo_sk,
        sr.sr_hdemo_sk,
        sr.sr_addr_sk,
        sr.sr_store_sk,
        sr.sr_reason_sk,
        sr.sr_ticket_number,
        sr.sr_return_quantity,
        sr.sr_return_amt,
        sr.sr_return_tax,
        sr.sr_return_amt_inc_tax,
        sr.sr_fee,
        sr.sr_return_ship_cost,
        sr.sr_refunded_cash,
        sr.sr_reversed_charge,
        sr.sr_store_credit,
        sr.sr_net_loss,
        sr_returned_date_sk
from ${SOURCE}.store_returns sr
where sr.sr_returned_date_sk >= ${DATELB} and sr.sr_returned_date_sk <= ${DATEUB};

create table if not exists store_sales
(
    ss_sold_time_sk           bigint,
    ss_item_sk                bigint,
    ss_customer_sk            bigint,
    ss_cdemo_sk               bigint,
    ss_hdemo_sk               bigint,
    ss_addr_sk                bigint,
    ss_store_sk               bigint,
    ss_promo_sk               bigint,
    ss_ticket_number          bigint,
    ss_quantity               int,
    ss_wholesale_cost         decimal(7, 2),
    ss_list_price             decimal(7, 2),
    ss_sales_price            decimal(7, 2),
    ss_ext_discount_amt       decimal(7, 2),
    ss_ext_sales_price        decimal(7, 2),
    ss_ext_wholesale_cost     decimal(7, 2),
    ss_ext_list_price         decimal(7, 2),
    ss_ext_tax                decimal(7, 2),
    ss_coupon_amt             decimal(7, 2),
    ss_net_paid               decimal(7, 2),
    ss_net_paid_inc_tax       decimal(7, 2),
    ss_net_profit             decimal(7, 2),
    ss_sold_date_sk           bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['ss_sold_date_sk']
);

insert into store_sales
select
        ss.ss_sold_time_sk,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        ss.ss_cdemo_sk,
        ss.ss_hdemo_sk,
        ss.ss_addr_sk,
        ss.ss_store_sk,
        ss.ss_promo_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_wholesale_cost,
        ss.ss_list_price,
        ss.ss_sales_price,
        ss.ss_ext_discount_amt,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_ext_list_price,
        ss.ss_ext_tax,
        ss.ss_coupon_amt,
        ss.ss_net_paid,
        ss.ss_net_paid_inc_tax,
        ss.ss_net_profit,
        ss.ss_sold_date_sk
from ${SOURCE}.store_sales ss
where ss.ss_sold_date_sk >= ${DATELB} and ss.ss_sold_date_sk <= ${DATEUB};

create table if not exists web_returns
(
    wr_returned_time_sk       bigint,
    wr_item_sk                bigint,
    wr_refunded_customer_sk   bigint,
    wr_refunded_cdemo_sk      bigint,
    wr_refunded_hdemo_sk      bigint,
    wr_refunded_addr_sk       bigint,
    wr_returning_customer_sk  bigint,
    wr_returning_cdemo_sk     bigint,
    wr_returning_hdemo_sk     bigint,
    wr_returning_addr_sk      bigint,
    wr_web_page_sk            bigint,
    wr_reason_sk              bigint,
    wr_order_number           bigint,
    wr_return_quantity        int,
    wr_return_amt             decimal(7, 2),
    wr_return_tax             decimal(7, 2),
    wr_return_amt_inc_tax     decimal(7, 2),
    wr_fee                    decimal(7, 2),
    wr_return_ship_cost       decimal(7, 2),
    wr_refunded_cash          decimal(7, 2),
    wr_reversed_charge        decimal(7, 2),
    wr_account_credit         decimal(7, 2),
    wr_net_loss               decimal(7, 2),
    wr_returned_date_sk       bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['wr_returned_date_sk']
);


insert into web_returns
select
        wr.wr_returned_time_sk,
        wr.wr_item_sk,
        wr.wr_refunded_customer_sk,
        wr.wr_refunded_cdemo_sk,
        wr.wr_refunded_hdemo_sk,
        wr.wr_refunded_addr_sk,
        wr.wr_returning_customer_sk,
        wr.wr_returning_cdemo_sk,
        wr.wr_returning_hdemo_sk,
        wr.wr_returning_addr_sk,
        wr.wr_web_page_sk,
        wr.wr_reason_sk,
        wr.wr_order_number,
        wr.wr_return_quantity,
        wr.wr_return_amt,
        wr.wr_return_tax,
        wr.wr_return_amt_inc_tax,
        wr.wr_fee,
        wr.wr_return_ship_cost,
        wr.wr_refunded_cash,
        wr.wr_reversed_charge,
        wr.wr_account_credit,
        wr.wr_net_loss,
        wr.wr_returned_date_sk
from ${SOURCE}.web_returns wr
where wr.wr_returned_date_sk >= ${DATELB} and wr.wr_returned_date_sk <= ${DATEUB};

create table if not exists web_sales
(
    ws_sold_time_sk           bigint,
    ws_ship_date_sk           bigint,
    ws_item_sk                bigint,
    ws_bill_customer_sk       bigint,
    ws_bill_cdemo_sk          bigint,
    ws_bill_hdemo_sk          bigint,
    ws_bill_addr_sk           bigint,
    ws_ship_customer_sk       bigint,
    ws_ship_cdemo_sk          bigint,
    ws_ship_hdemo_sk          bigint,
    ws_ship_addr_sk           bigint,
    ws_web_page_sk            bigint,
    ws_web_site_sk            bigint,
    ws_ship_mode_sk           bigint,
    ws_warehouse_sk           bigint,
    ws_promo_sk               bigint,
    ws_order_number           bigint,
    ws_quantity               int,
    ws_wholesale_cost         decimal(7, 2),
    ws_list_price             decimal(7, 2),
    ws_sales_price            decimal(7, 2),
    ws_ext_discount_amt       decimal(7, 2),
    ws_ext_sales_price        decimal(7, 2),
    ws_ext_wholesale_cost     decimal(7, 2),
    ws_ext_list_price         decimal(7, 2),
    ws_ext_tax                decimal(7, 2),
    ws_coupon_amt             decimal(7, 2),
    ws_ext_ship_cost          decimal(7, 2),
    ws_net_paid               decimal(7, 2),
    ws_net_paid_inc_tax       decimal(7, 2),
    ws_net_paid_inc_ship      decimal(7, 2),
    ws_net_paid_inc_ship_tax  decimal(7, 2),
    ws_net_profit             decimal(7, 2),
    ws_sold_date_sk           bigint
)
WITH (
    format = 'ORC',
    partitioned_by = ARRAY['ws_sold_date_sk']
);

insert into web_sales
select
        ws.ws_sold_time_sk,
        ws.ws_ship_date_sk,
        ws.ws_item_sk,
        ws.ws_bill_customer_sk,
        ws.ws_bill_cdemo_sk,
        ws.ws_bill_hdemo_sk,
        ws.ws_bill_addr_sk,
        ws.ws_ship_customer_sk,
        ws.ws_ship_cdemo_sk,
        ws.ws_ship_hdemo_sk,
        ws.ws_ship_addr_sk,
        ws.ws_web_page_sk,
        ws.ws_web_site_sk,
        ws.ws_ship_mode_sk,
        ws.ws_warehouse_sk,
        ws.ws_promo_sk,
        ws.ws_order_number,
        ws.ws_quantity,
        ws.ws_wholesale_cost,
        ws.ws_list_price,
        ws.ws_sales_price,
        ws.ws_ext_discount_amt,
        ws.ws_ext_sales_price,
        ws.ws_ext_wholesale_cost,
        ws.ws_ext_list_price,
        ws.ws_ext_tax,
        ws.ws_coupon_amt,
        ws.ws_ext_ship_cost,
        ws.ws_net_paid,
        ws.ws_net_paid_inc_tax,
        ws.ws_net_paid_inc_ship,
        ws.ws_net_paid_inc_ship_tax,
        ws.ws_net_profit,
        ws.ws_sold_date_sk
from ${SOURCE}.web_sales ws
where ws.ws_sold_date_sk >= ${DATELB} and ws.ws_sold_date_sk <= ${DATEUB};

create table if not exists call_center WITH ( format = 'ORC' )
as select * from ${SOURCE}.call_center;

create table if not exists catalog_page WITH ( format = 'ORC' )
as select * from ${SOURCE}.catalog_page;

create table if not exists customer WITH ( format = 'ORC' )
as select * from ${SOURCE}.customer;

create table if not exists customer_address WITH ( format = 'ORC' )
as select * from ${SOURCE}.customer_address;

create table if not exists customer_demographics WITH ( format = 'ORC' )
as select * from ${SOURCE}.customer_demographics;

create table if not exists date_dim WITH ( format = 'ORC' )
as select * from ${SOURCE}.date_dim;

create table if not exists household_demographics WITH ( format = 'ORC' )
as select * from ${SOURCE}.household_demographics;

create table if not exists income_band WITH ( format = 'ORC' )
as select * from ${SOURCE}.income_band;

create table if not exists item WITH ( format = 'ORC' )
as select * from ${SOURCE}.item;

create table if not exists promotion WITH ( format = 'ORC' )
as select * from ${SOURCE}.promotion;

create table if not exists reason WITH ( format = 'ORC' )
as select * from ${SOURCE}.reason;

create table if not exists ship_mode WITH ( format = 'ORC' )
as select * from ${SOURCE}.ship_mode;

create table if not exists store WITH ( format = 'ORC' )
as select * from ${SOURCE}.store;

create table if not exists time_dim WITH ( format = 'ORC' )
as select * from ${SOURCE}.time_dim;

create table if not exists warehouse WITH ( format = 'ORC' )
as select * from ${SOURCE}.warehouse;

create table if not exists web_page WITH ( format = 'ORC' )
as select * from ${SOURCE}.web_page;

create table if not exists web_site WITH ( format = 'ORC' )
as select * from ${SOURCE}.web_site;
