-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Batch - Silver to Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inventory Snapshots

-- COMMAND ----------

create
or replace view training.pos_gold.view_last_snapshots as with snapshots as (
  select
    *,
    row_number() over w_last_ss as last_ss
  from
    training.pos_silver.inventory_snapshots window w_last_ss as (
      partition by store_id,
      item_id
      order by
        date_time
    )
)
select
  *
except
  (last_ss)
from
  snapshots
where
  last_ss = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Current State of Inventory

-- COMMAND ----------

create
or replace view training.pos_gold.view_inventory_current as
select
  t.name as store_name,
  m.name item_name,
  first(h.quantity) as snapshot_quantity,
  coalesce(sum(t.quantity), 0) as transactions_quantity,
  first(h.quantity) + coalesce(sum(t.quantity), 0) as current_inventory,
  greatest(first(h.date_time), max(t.date_time)) as date_time
from
  training.pos_gold.view_last_snapshots h
  inner join (
    select
      x.store_id,
      x.item_id,
      x.date_time,
      x.quantity,
      s.name
    from
      training.pos_silver.inventory_transactions x
      inner join training.pos_silver.stores s on s.store_id = x.store_id
      inner join training.pos_silver.inventory_types i on i.change_type_id = x.change_type_id
    where
      not (
        s.name = 'online'
        AND i.change_type = 'bopis'
      )
  ) t on t.store_id = h.store_id
  and t.item_id = h.item_id
  and t.date_time >= h.date_time
  inner join training.pos_silver.items m on m.item_id = h.item_id
group by
  1,
  2
order by
  date_time desc;
