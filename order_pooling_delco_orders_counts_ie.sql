
SELECT
    dd_lvl1.tenant_id                                                                                              AS tenant_id,
    dd_lvl1.order_date                                                                                             AS order_date,
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
    dd_lvl1.voucher_refund_reason                                                                                  AS voucher_refund_reason,
    dd_lvl1.payment_sequence                                                                                       AS payment_sequence,
    COUNT(DISTINCT(dd_lvl1.sys_order_id))                                                                          AS delco_orders


FROM `just-data-warehouse.international_reporting.order_pooling_refunds_time_periods_order_counts_ie` AS dd_lvl1
WHERE tenant_id = 'ie'

GROUP BY
    dd_lvl1.tenant_id,
    dd_lvl1.order_date,
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
    dd_lvl1.voucher_refund_reason,
    dd_lvl1.payment_sequence
