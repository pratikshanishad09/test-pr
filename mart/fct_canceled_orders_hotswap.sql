{{ config(
    materialized='table',
    tags=['hot_swap']
) }}

SELECT * FROM {{ ref('fct_canceled_orders_stg') }}