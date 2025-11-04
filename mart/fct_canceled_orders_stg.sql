{{ config(
    materialized='table',
    tags=['mart']
) }}

-- mart model: Enriches transformed sales orders with channel and customer metadata

SELECT

    -- Order Metadata
    sales_order_updated_time_stamp                     AS updated_ts,
    sales_order_status                                 AS order_status,
    sales_order_cancel_reason                          AS cancel_reason,
    sales_order_cancel_subreason                       AS order_substatus,
    sales_order_cancel_note                            AS buyer_comments,
    sales_order_placed_time_stamp                      AS placed_at,
    sales_order_identifier                             AS order_id,
    sales_order_type                                   AS order_type,
    sales_order_channel_order_identifier               AS channel_order_id,
    sales_order_parent_order_identifier                AS parent_order_id,
    sales_order_parent_account_identifier              AS parent_company_id,
    sales_order_parent_account_label                   AS parent_company_label,

    -- Channel Info
    sales_order_sales_channel_identifier               AS channel_id,
    sales_order_sales_channel_label                    AS channel_label,
    sales_order_sales_channel_type                     AS channel_type,
    sales_order_sales_channel_owner_identifier         AS channel_owner_id,
    sales_order_sales_channel_owner_label              AS channel_owner_label,

    -- Financials
    sales_order_currency                               AS currency,
    sales_order_subtotal_amount                        AS subtotal_amount,
    sales_order_shipping_amount                        AS shipping_amount,
    sales_order_shipping_discount_amount               AS shipping_discount_amount,
    sales_order_admin_discount_amount                  AS admin_discount_amount,
    sales_order_manufacturer_shipping_discount_amount  AS manufacturer_shipping_discount_amount,
    sales_order_retailer_shipping_discount_amount      AS retailer_shipping_discount_amount,
    sales_order_shipping_tax_amount                    AS shipping_tax_amount,
    sales_order_shipping_insurance                     AS shipping_insurance,
    sales_order_handling_amount                        AS handling_amount,
    sales_order_initial_successful_payment_method      AS payment_method_type,
    sales_order_total_amount                           AS total_amount,
    sales_order_retail_delivery_fee                    AS retail_delivery_fee,
    sales_order_handling_tax_amount                    AS handling_tax_amount,

    -- OEM & Seller Info
    sales_order_item_franchiser_account_identifier     AS franchiser_id,
    sales_order_item_franchiser_account_label          AS franchiser_label,
    sales_order_channel_tier                           AS franchiser_channel_tier,
    sales_order_item_franchisee_id                     AS franchisee_id,
    sales_order_item_franchisee_label                  AS franchisee_label,
    sales_order_item_franchisee_name                   AS franchisee_dealer_name,
    sales_order_item_franchisee_code                   AS franchisee_dealer_code,
    sales_order_item_franchiser_geographic_level_1     AS franchiser_sales_region_primary,
    sales_order_item_franchiser_geographic_level_2     AS franchiser_sales_region_secondary,
    sales_order_item_franchiser_geographic_level_3     AS franchiser_sales_region_tertiary,

    -- Seller & Shipping Details
    sales_order_seller_identifier                      AS seller_id,
    sales_order_seller_label                           AS seller_label,
    sales_order_seller_country                         AS seller_country,
    sales_order_seller_state_province                  AS seller_state_province,

    sales_order_shipping_country                       AS shipping_country,
    sales_order_shipping_state_province                AS shipping_state_province,

    -- Item Details
    sales_order_item_identifier                        AS item_id,
    sales_order_item_part_number                       AS item_part_number,
    sales_order_item_brand_label                       AS item_brand_label,
    sales_order_item_core_charge_amount                AS item_core_charge_amount,
    sales_order_item_amount                            AS item_price,
    sales_order_item_quantity                          AS item_quantity,
    sales_order_item_manufacturer_item_discount_amount AS manufacturer_item_discount_amount,
    sales_order_item_retailer_item_discount_amount     AS retailer_item_discount_amount,
    sales_order_item_sales_tax                         AS item_sales_tax_amount,
    sales_order_item_est_dealer_net                    AS item_est_dealer_net,
    saler_order_item_rp_product_type                   AS item_rp_product_type,
    sales_order_item_part_category                     AS item_rp_category,
    sales_order_item_part_subcategory                  AS item_rp_subcategory,
    sales_order_item_part_type                         AS item_rp_type,
    sales_order_item_vehicle_year_selected             AS item_vehicle_year,
    sales_order_item_vehicle_make_selected             AS item_vehicle_make,
    sales_order_item_vehicle_model_selected            AS item_vehicle_model

FROM {{ source('intermediate', 'int_canceled_orders_stg_report_transform') }}