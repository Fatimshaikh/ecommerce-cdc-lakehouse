
  
  create view "lakehouse"."main"."stg_order_items__dbt_tmp" as (
    SELECT *
FROM delta_scan('s3://lakehouse/silver/order_items')
  );
