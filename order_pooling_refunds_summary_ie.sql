/* Final SQL Query No. 1 - About 50% of the visualizations */

SELECT
    dd_lvl1.tenant_id                                                                                              AS tenant_id,
    dd_lvl1.sys_courier_delivery_zone_id                                                                           AS sys_courier_delivery_zone_id,
    dd_lvl1.delivery_zone_name                                                                                     AS delivery_zone_name,
    dd_lvl1.resto_type                                                                                             AS resto_type,
    dd_lvl1.is_multi_collect                                                                                       AS is_multi_collect,
    dd_lvl1.multi_collect_sequence                                                                                 AS multi_collect_sequence,
    dd_lvl1.is_mcdonalds                                                                                           AS is_mcdonalds,
    dd_lvl1.delivery_status                                                                                        AS delivery_status,
    dd_lvl1.is_on_time_order                                                                                       AS is_on_time_order,
    dd_lvl1.delivery_completion_slack_bin                                                                          AS delivery_completion_slack_bin,
    dd_lvl1.is_asap_order                                                                                          AS is_asap_order,
    CASE WHEN dd_lvl1.voucher_refunds_issued_total_amount < 0
         THEN 1 ELSE 0 END                                                                                         AS is_negative_company_net_adjustment,
    CASE WHEN dd_lvl1.delivery_begin_parking_time_local IS NOT NULL
         THEN 1
         ELSE 0
         END                                                                                                       AS has_begin_parking_time,
    dd_lvl1.payment_sequence_dense_rank                                                                            AS payment_sequence_dense_rank,
    dd_lvl1.deliver_original_total_driving_time_min                                                                AS deliver_original_total_driving_time_min,
    CASE WHEN FLOOR(dd_lvl1.ctod_sec/60) >= 30
         THEN "+30"
		 WHEN FLOOR(dd_lvl1.ctod_sec/60) < 0
         THEN "0"
         ELSE CAST(FLOOR(dd_lvl1.ctod_sec/60) AS STRING) END                                                       AS ctod_min,
    COUNT(DISTINCT(dd_lvl1.sys_order_id))                                                                          AS delco_orders,
    COUNT(DISTINCT(CASE WHEN dd_lvl1.is_multi_collect = 1 THEN dd_lvl1.sys_order_id ELSE NULL END))                AS md_orders,
    COUNT(DISTINCT(CASE WHEN CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'FOOD_QUALITY')
                        THEN dd_lvl1.sys_order_id
                        ELSE NULL END))                                                                            AS orders_w_food_quality_refund,
    COUNT(DISTINCT(CASE WHEN CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'DELIVERY_SERVICE')
                        THEN dd_lvl1.sys_order_id
                        ELSE NULL END))                                                                            AS orders_w_delivery_service_refund,
    COUNT(DISTINCT(CASE WHEN CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'COMPLAINT_ON_ORDER_ITEM')   = TRUE
                        OR   CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'ENTIRE_ORDER_INCORRECT')    = TRUE
                        OR   CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'INCORRECT_OR_MISSED_ITEM')  = TRUE
                        OR   CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'COMPLAINT_ON_ENTIRE_ORDER') = TRUE
                        OR   CONTAINS_SUBSTR(dd_lvl1.voucher_refund_reason, 'MISSING_INCORRECT_ITEMS')   = TRUE
                        THEN dd_lvl1.sys_order_id
                        ELSE NULL END))                                                                            AS orders_w_missing_item_refund

FROM
    `just-data-warehouse.international_reporting.order_pooling_refunds_time_periods_order_counts_ie` AS dd_lvl1

WHERE
     dd_lvl1.tenant_id  = 'ie'

GROUP BY
    dd_lvl1.tenant_id,
    dd_lvl1.sys_courier_delivery_zone_id,
    dd_lvl1.delivery_zone_name,
    dd_lvl1.resto_type,
    dd_lvl1.is_multi_collect,
    dd_lvl1.multi_collect_sequence,
    dd_lvl1.is_mcdonalds,
    dd_lvl1.delivery_status,
    dd_lvl1.is_on_time_order,
    dd_lvl1.delivery_completion_slack_bin,
    dd_lvl1.is_asap_order,
    is_negative_company_net_adjustment,
    has_begin_parking_time,
    dd_lvl1.payment_sequence_dense_rank,
    dd_lvl1.deliver_original_total_driving_time_min,
    ctod_min
