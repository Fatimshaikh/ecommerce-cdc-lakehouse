
  
  create view "lakehouse"."main"."stg_payments__dbt_tmp" as (
    SELECT *
FROM delta_scan('s3://lakehouse/silver/payments')
  );
