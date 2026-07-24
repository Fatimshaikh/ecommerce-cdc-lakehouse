
  
    
    

    create  table
      "lakehouse"."main"."fct_orders__dbt_tmp"
  
    as (
      WITH order_totals AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * CAST(oi.unit_price AS DOUBLE)) AS order_revenue,
        SUM(oi.quantity) AS total_items
    FROM "lakehouse"."main"."stg_order_items" oi
    GROUP BY oi.order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_date,
    ot.order_revenue,
    ot.total_items,
    p.payment_status,
    p.payment_method
FROM "lakehouse"."main"."stg_orders" o
LEFT JOIN order_totals ot ON o.order_id = ot.order_id
LEFT JOIN "lakehouse"."main"."stg_payments" p ON o.order_id = p.order_id
    );
  
  