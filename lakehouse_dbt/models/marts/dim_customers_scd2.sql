
{{
    config(
        materialized='table'
    )
}}

WITH customer_changes AS (
    SELECT
        customer_id,
        full_name,
        email,
        country,
        _cdc_ts_ms,
        _ingested_at
    FROM {{ ref('stg_customers') }}
),

with_validity AS (
    SELECT
        customer_id,
        full_name,
        email,
        country,
        _cdc_ts_ms AS valid_from_ms,
        LEAD(_cdc_ts_ms) OVER (
            PARTITION BY customer_id ORDER BY _cdc_ts_ms
        ) AS valid_to_ms
    FROM customer_changes
)

SELECT
    customer_id,
    full_name,
    email,
    country,
    valid_from_ms,
    valid_to_ms,
    CASE WHEN valid_to_ms IS NULL THEN true ELSE false END AS is_current
FROM with_validity
