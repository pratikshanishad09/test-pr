{{ config(
    materialized='table',
    tags=['intermediate']
) }}

-- Intermediate model: Enriches transformed sales orders with channel and customer metadata
-- Purpose:
--   - Classify orders as Wholesale or Retail
--   - Identify channel owner and type
--   - Generate channel label and tier

SELECT DISTINCT
    -- Classify order type based on product and parent order logic
    CASE
        WHEN cp.product = 'Wholesale' THEN 'Wholesale'
        WHEN cp.product = 'SupplierStore' AND ccp.product = 'Wholesale' THEN 'Wholesale'
        WHEN o.type = 'PURCHASE_ORDER' AND o.parent_order_number IS NOT NULL THEN 'Wholesale'
        ELSE 'Retail'
    END AS sales_order_type,

    -- Core order item identifiers and pricing
    oi.id                                               AS sales_order_item_identifier,
    o.parent_order_number                               AS sales_order_parent_order_identifier,
    oi.core_charge                                      AS sales_order_item_core_charge_amount,
    oi.price                                            AS sales_order_item_amount,
    COALESCE(fpsm_pi.cost, CAST(0.00 AS DECIMAL(19,2))) AS sales_order_item_est_dealer_net,
    oi.tax_amount                                       AS sales_order_item_sales_tax,

    -- Identifies the entity funding the shipping & discount amounts (Manufacturer or Retailer)
    COALESCE(discount_amount.manufacturer_shipping_discount_amount, CAST(0.00 AS DECIMAL(19,2)))  AS sales_order_manufacturer_shipping_discount_amount,
    COALESCE(discount_amount.retailer_shipping_discount_amount, CAST(0.00 AS DECIMAL(19,2)))      AS sales_order_retailer_shipping_discount_amount,
    COALESCE(item_discount_amount.manufacturer_item_discount_amount, CAST(0.00 AS DECIMAL(19,2))) AS sales_order_item_manufacturer_item_discount_amount,
    COALESCE(item_discount_amount.retailer_item_discount_amount, CAST(0.00 AS DECIMAL(19,2)))     AS sales_order_item_retailer_item_discount_amount,

    -- Order-level identifiers and customer info
    o.order_number         AS sales_order_identifier,
    o.channel_order_number AS sales_order_channel_order_identifier,
    o.customer_id          AS sales_order_seller_identifier,
    cc.name                AS sales_order_seller_label,

    -- Product metadata
    oi.product_id_stripped AS sales_order_item_part_number,
    oi.product_type        AS saler_order_item_rp_product_type,
    rc.category_name       AS sales_order_item_part_category,
    rc.sub_category_name   AS sales_order_item_part_subcategory,
    rc.component_name      AS sales_order_item_part_type,

    -- Vehicle metadata selected during order
    oi.car_year  AS sales_order_item_vehicle_year_selected,
    oi.car_make  AS sales_order_item_vehicle_make_selected,
    oi.car_model AS sales_order_item_vehicle_model_selected,

    -- Brand and account metadata
    oi.brand        AS sales_order_item_brand_label,
    sat.parentid    AS sales_order_parent_account_identifier,
    sat.parent_name AS sales_order_parent_account_label,

    -- Identify channel owner based on product type
    CASE 
        WHEN cp.product = 'SupplierStore' THEN ccp.customer_id
        WHEN cp.product = 'VirtualPartsCounter' AND lg.source_hash != lg.fulfillment_hash THEN ccp.customer_id
        ELSE cp.customer_id 
    END AS sales_order_sales_channel_owner_identifier,

    -- Identify channel owner label
    CASE 
        WHEN cp.product = 'SupplierStore' THEN sat_owner.account_name
        WHEN cp.product = 'VirtualPartsCounter' AND lg.source_hash != lg.fulfillment_hash THEN sat_owner.account_name
        ELSE sat.account_name 
    END AS sales_order_sales_channel_owner_label,

    -- Identify brand owner for the item; only if the brand-account pair exists in base_brands
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.oem_cid
        ELSE NULL
    END AS sales_order_item_brand_owner_identifier,

    -- Label for the brand owner; derived from dealer account if brand-account pair is valid
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.oem_account
        ELSE NULL
    END AS sales_order_item_brand_owner_label,

    -- OEM Account ID

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.oem_account_id
        ELSE NULL
    END AS sales_order_item_franchiser_account_identifier,

    -- OEM Account Name

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.oem_account
        ELSE NULL
    END AS sales_order_item_franchiser_account_label,

    -- dealer and franchise metadata

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.account_id
        ELSE NULL
    END AS sales_order_item_franchisee_id,

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.account_name
        ELSE NULL
    END AS sales_order_item_franchisee_label,

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.dealer_code
        ELSE NULL
    END AS sales_order_item_franchisee_code,

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.dealer_name
        ELSE NULL
    END AS sales_order_item_franchisee_name,

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.region
        ELSE NULL
    END AS sales_order_item_franchiser_geographic_level_1,

    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.area
        ELSE NULL
    END AS sales_order_item_franchiser_geographic_level_2,
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('staging', 'base_brands') }} bb
            WHERE bb.account_id = dealer_accounts.oem_account_id
              AND bb.brand = oi.brand
            ) THEN dealer_accounts.market
        ELSE NULL
    END AS sales_order_item_franchiser_geographic_level_3,

    -- Tiering logic based on RP Superstore and dealer validity
    CASE
        WHEN trs.hash = lg.source_hash AND trs.category = 'Tier 1' THEN 'Tier 1'
        WHEN dealer_accounts.dealer_code IS NOT NULL THEN 'Tier 3' 
        ELSE 'Other'
    END AS sales_order_channel_tier,

    -- Determine channel type from product metadata
    CASE 
        WHEN cp.product = 'SupplierStore' THEN ccp.product
        WHEN cp.product = 'VirtualPartsCounter' AND lg.source_hash != lg.fulfillment_hash THEN ccp.product
        ELSE cp.product 
    END AS sales_order_sales_channel_type,

    -- Channel identifier hash; for SupplierStore, use the superstore's hash instead of the supplier store's
    CASE 
        WHEN cp.product = 'SupplierStore' THEN ccp.hash
        WHEN cp.product = 'VirtualPartsCounter' AND lg.source_hash != lg.fulfillment_hash THEN ccp.hash
        ELSE cp.hash 
    END AS sales_order_sales_channel_identifier,

    -- Generate a label for the sales channel
    COALESCE(
    CASE
        WHEN cp.product = 'SupplierStore' THEN ccs.domain
        WHEN cp.product = 'VirtualPartsCounter'
             AND lg.source_hash != lg.fulfillment_hash
             THEN ccs.domain
        WHEN cp.product = 'VirtualPartsCounter' THEN 'VPC ' || CAST(sat.cid AS VARCHAR) || ' ' || sat.account_name
        WHEN cp.product = 'Wholesale' THEN cs.domain
        WHEN cp.product IN ('eBay', 'Amazon', 'Walmart') AND cp.name IS NOT NULL THEN cp.name
        WHEN cp.product = 'Webstore' THEN cs.domain
        WHEN cp.product = 'PluginStore' THEN iut.install_url
    END, ccp.hash, cp.hash) AS sales_order_sales_channel_label,

    -- Order financials and logistics
    o.placed_ts                 AS sales_order_placed_time_stamp,
	o.updated_ts                AS sales_order_updated_time_stamp,
	o.order_status              AS sales_order_status,
	o.cancel_reason             AS sales_order_cancel_reason,
	o.order_substatus           AS sales_order_cancel_subreason,
	o.buyer_comments            AS sales_order_cancel_note,
    o.order_currency            AS sales_order_currency,
    o.shipping_amount           AS sales_order_shipping_amount,
    o.subtotal_amount           AS sales_order_subtotal_amount,
    o.total_amount              AS sales_order_total_amount,
    o.shipping_tax_amount       AS sales_order_shipping_tax_amount,
    o.shipping_discount_amount  AS sales_order_shipping_discount_amount,
    o.admin_discount_amount     AS sales_order_admin_discount_amount,
    o.handling_amount           AS sales_order_handling_amount,
    o.handling_tax_amount       AS sales_order_handling_tax_amount,
    o.retail_delivery_fee       AS sales_order_retail_delivery_fee,
    o.shipping_insurance_amount AS sales_order_shipping_insurance,

    -- Shipping and seller location metadata
    sc.country_code  AS sales_order_shipping_country,
    sc.state_code    AS sales_order_shipping_state_province,
    sat.country_code AS sales_order_seller_country,
    sat.state        AS sales_order_seller_state_province,

    -- Payment method and quantity
    pm.payment_method AS sales_order_initial_successful_payment_method,
    oi.quantity       AS sales_order_item_quantity

FROM {{ ref('int_canceled_orders_transform') }} o

-- Join with order item details
LEFT JOIN {{ ref('int_sales_order_items__transform') }} oi 
    ON oi.order_number = o.order_number

-- Join to identify and exclude test accounts
LEFT JOIN {{ ref('int_core_clients__transform') }} cc 
    ON cc.id = o.customer_id

-- Join with lead generation data to enrich channel metadata
LEFT JOIN {{ ref('int_core_lead_gen__transform') }} lg 
    ON lg.order_number = o.order_number

-- Join with customer product metadata (supplier store or wholesale)
LEFT JOIN {{ ref('int_core_customer_products__transform') }} cp 
    ON cp.hash = o.product_hash

-- Join with superstore product metadata (used when product is SupplierStore OR VirtualPartsCounter)
LEFT JOIN {{ ref('int_core_customer_products__transform') }} ccp 
    ON ccp.hash = lg.source_hash

-- Join with store metadata for supplier store
LEFT JOIN {{ ref('int_core_store__transform') }} cs 
    ON cs.hash = cp.hash

-- Join with store metadata for superstore
LEFT JOIN {{ ref('int_core_store__transform') }} ccs 
    ON ccs.hash = lg.source_hash

-- Join with cdata setting for install url
LEFT JOIN {{ ref('int_core_install_url__transform') }} iut 
    ON iut.hash = cp.hash
   AND iut.customer_id = cp.customer_id

-- Join with Salesforce account data for seller and parent account info
LEFT JOIN {{ ref('int_salesforce_account__transform') }} sat 
    ON sat.cid = cp.customer_id

-- Join with Salesforce account data for account owner label
LEFT JOIN {{ ref('int_salesforce_account__transform') }} sat_owner
    ON sat_owner.cid = ccp.customer_id

-- Join with RP Superstore tiering data to classify channel tier
LEFT JOIN {{ ref('seed_sales_tier1_rp_superstore') }} trs
    ON trs.hash = lg.source_hash

-- Join with category metadata for product classification
LEFT JOIN {{ ref('int_core_catagory_codes__transform') }} rc 
    ON rc.rp_category = oi.rp_category

-- Join with state code metadata for shipping location
LEFT JOIN {{ ref('int_core_state_code__transform') }} sc 
    ON sc.id = o.shipping_address_id

-- Join with payment method metadata
LEFT JOIN {{ ref('int_core_payment_method__transform') }} pm 
    ON pm.order_number = o.order_number

-- Join with dealer accounts data for brand/franchise metadata
LEFT JOIN {{ source('staging', 'stg_salesforce__dealer_accounts') }} dealer_accounts
    ON dealer_accounts.cid = o.customer_id

-- Join with parts info to get estimated dealer net cost
LEFT JOIN {{ source('staging', 'stg_rp_catalog_fpsm__parts_info') }} fpsm_pi 
    ON fpsm_pi.part_number_stripped = oi.product_id_stripped
   AND fpsm_pi.brand = oi.brand
   AND fpsm_pi.country = sat.country_code

-- Join with promotion discount amount data to get discount amounts (Manufacturer or Retailer)
LEFT JOIN {{ ref('int_sales_orders_promotions_discount_amount__transform') }} discount_amount
    ON discount_amount.order_number = o.order_number

-- Join with promotion discount amount data to get shipping amounts (Manufacturer or Retailer)
LEFT JOIN {{ ref('int_sales_order_items_item_discount_amount__transform') }} item_discount_amount
    ON item_discount_amount.id = oi.id
   AND item_discount_amount.order_number = oi.order_number
   AND item_discount_amount.product_id_stripped = oi.product_id_stripped

-- Filter out test accounts
WHERE cc.is_test_account = 0

-- Corresponding Order Items details check
  AND oi.id IS NOT NULL