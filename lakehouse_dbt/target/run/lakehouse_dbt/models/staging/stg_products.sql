
  
  create view "lakehouse"."main"."stg_products__dbt_tmp" as (
    SELECT *
FROM delta_scan('s3://lakehouse/silver/products')
  );
