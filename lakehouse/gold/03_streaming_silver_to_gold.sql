-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Streaming - Silver to Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inventory Transactions

-- COMMAND ----------

create
or replace view training.test_pos_gold.view_inventory_transactions as with trxs as (
  select
    s.name as store_name,
    t.trans_id,
    i.name as item_name,
    t.date_time,
    t.quantity,
    y.change_type
  from
    training.test_pos_silver.inventory_transactions4 t
    inner join training.test_pos_silver.stores s on s.store_id = t.store_id
    inner join training.test_pos_silver.items i on i.item_id = t.item_id
    inner join training.test_pos_silver.inventory_types y on y.change_type_id = t.change_type_id
  where
    not (
      s.name = 'online'
      and y.change_type = 'bopis'
    )
    and t.date_time > '2021-02-01' -- current_timestamp() - interval '1 hour'
  order by
    t.date_time desc
),
proc as (
  select
    *,
    row_number() over w_last as last_trx_rank
  from
    trxs window w_last as (
      partition by store_name
      order by
        date_time desc
    )
)
select
  *
except
  (last_trx_rank)
from
  proc
where
  last_trx_rank = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inventory Transactions History

-- COMMAND ----------

create
or replace view training.test_pos_gold.view_inventory_transactions_history as
select
  t.date_time,
  s.name store_name,
  i.name item_name,
  y.change_type,
  sum(t.quantity) quantity
from
  training.test_pos_silver.inventory_transactions4 t
  inner join training.test_pos_silver.stores s on s.store_id = t.store_id
  inner join training.test_pos_silver.items i on i.item_id = t.item_id
  inner join training.test_pos_silver.inventory_types y on y.change_type_id = t.change_type_id
where t.date_time > '2021-01-24' -- current_timestamp() - interval '1 week'
group by
  1,
  2,
  3,
  4;
