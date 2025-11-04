{{ config(
    tags=["intermediate"],
    materialized='incremental',
    unique_key='order_number'
) }}

-- Intermediate model: Selects the first non-ERP occurrence of each order
-- Purpose:
--   - Retain only one record per order_number
--   - Based on earliest partitioned timestamp
--   - Supports incremental loading (adds only new order_numbers)

-- CTE to get the first non-ERP occurrence of each order from feed data
WITH cte_first_occurrence AS (
    SELECT
        order_number,
        customer_id,
        channel_order_number,
        placed_ts,
        updated_ts,
		order_status,
		cancel_reason,
		order_substatus,
		buyer_comments,
        product_hash,
        type,
        parent_order_number,
        order_currency,
        shipping_address_id,
        shipping_amount,
        shipping_tax_amount,
        shipping_discount_amount,
        admin_discount_amount,
        handling_amount,
        handling_tax_amount,
        subtotal_amount,
        total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY order_number 
            ORDER BY CAST(partition_0 || partition_1 || partition_2 AS BIGINT), updated_ts
        ) AS rn
    FROM {{ source('staging', 'stg_rp_feed__orders') }}
    WHERE placed_ts IS NOT NULL
      AND updated_ts IS NOT NULL
	  AND order_status = 'Canceled'
      AND (order_initial_source != 'ERP' OR order_final_source != 'ERP')
),

-- CTE to get the first non-ERP occurrence of each order from JSON extended data
cte_first_json_occurrence AS (
    SELECT DISTINCT
        order_number,
        retail_delivery_fee,
        shipping_insurance_amount,
        ROW_NUMBER() OVER (
            PARTITION BY order_number 
            ORDER BY CAST(partition_0 || partition_1 || partition_2 AS BIGINT), updated_ts
        ) AS rn
    FROM {{ source('staging', 'stg_rp_json_extended_order_data__orders') }}
    WHERE placed_ts IS NOT NULL
      AND (order_initial_source != 'ERP' OR order_final_source != 'ERP')
),

-- Select only the first occurrence of each order_number from feed data
unique_orders AS (
    SELECT
        order_number,                  -- Unique order identifier
        customer_id,                   -- Customer placing the order
        channel_order_number,          -- Order number from the sales channel
        placed_ts,                     -- Timestamp when the order was placed
        updated_ts,                    -- Timestamp when order was last updated(cancel timestamp)
		order_status,                  -- Order status (will always be 'Canceled')
		cancel_reason,                 -- Reason for cancellation
		order_substatus,               -- Order substatus (can contain sub-reason)
		buyer_comments,                -- Buyer comments/notes
        product_hash,                  -- Hash representing the product(s) in the order
        type,                          -- Type of order
        parent_order_number,           -- Parent order reference
        order_currency,                -- Currency code (e.g., USD, CAD)
        shipping_address_id,           -- Foreign key to shipping address
        shipping_amount,               -- Shipping cost
        shipping_tax_amount,           -- Tax applied to shipping
        shipping_discount_amount,      -- Discount applied to shipping
        admin_discount_amount,         -- Order level admin discount amount
        handling_amount,               -- Handling fee
        handling_tax_amount,           -- Tax applied to handling
        subtotal_amount,               -- Subtotal before taxes and discounts
        total_amount                   -- Final total amount
    FROM cte_first_occurrence
    WHERE rn = 1
),

-- Select only the first occurrence of each order_number from JSON data
unique_order_json AS (
    SELECT
        order_number,                       -- Associated order number
        retail_delivery_fee,               -- Delivery fee from JSON data
        shipping_insurance_amount          -- Insurance amount from JSON data
    FROM cte_first_json_occurrence
    WHERE rn = 1                            -- Keep only the first occurrence per item
),

-- Final combined dataset with enriched fields
cte_final AS (
    SELECT
        uo.order_number,                   -- Unique order identifier
        uo.customer_id,                    -- Customer placing the order
        uo.channel_order_number,           -- Order number from the sales channel
        uo.placed_ts,                      -- Timestamp when the order was placed
        uo.updated_ts,                     -- Timestamp when order was last updated(cancel timestamp)
		uo.order_status,                   -- Order status (will always be 'Canceled')
		uo.cancel_reason,                  -- Reason for cancellation
		uo.order_substatus,                -- Order substatus (can contain sub-reason)
		uo.buyer_comments,                 -- Buyer comments/notes
        uo.product_hash,                   -- Hash representing the product(s) in the order
        uo.type,                           -- Type of order
        uo.parent_order_number,            -- Parent order reference
        uo.order_currency,                 -- Currency code (e.g., USD, CAD)
        uo.shipping_address_id,            -- Foreign key to shipping address
        uo.shipping_amount,                -- Shipping cost
        uo.shipping_tax_amount,            -- Tax applied to shipping
        uo.shipping_discount_amount,       -- Discount applied to shipping
        uo.admin_discount_amount,          -- Order level admin discount amount
        uo.handling_amount,                -- Handling fee
        uo.handling_tax_amount,            -- Tax applied to handling
        uo.subtotal_amount,                -- Subtotal before taxes and discounts
        uo.total_amount,                   -- Final total amount
        uoj.retail_delivery_fee,           -- Delivery fee from JSON data
        uoj.shipping_insurance_amount      -- Insurance amount from JSON data
    FROM unique_orders uo
    LEFT JOIN unique_order_json uoj
        ON uo.order_number = uoj.order_number
)

-- Final selection with incremental logic
SELECT * FROM cte_final AS first_occurrences

{% if is_incremental() %}
-- Exclude orders already present in the target table
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing.order_number = first_occurrences.order_number)
{% endif %}