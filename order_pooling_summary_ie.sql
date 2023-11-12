WITH rejections AS (

    SELECT
        ds.sys_delivery_id                AS sys_delivery_id,
        COUNT(DISTINCT bc.sys_courier_id) AS rejections_count

    FROM
        `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie`                             AS ds
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.bridge_delivery_details_blacklisted_couriers`   AS bc ON bc.sys_delivery_id = ds.sys_delivery_id

    GROUP BY
        sys_delivery_id

),
pooling_kpi AS (

    WITH first_delivery_in_transit_time AS (

        SELECT
            ccdd_id                             AS ccdd_id,
            MIN(delivery_in_transit_time_local) AS first_delivery_in_transit_time_local

        FROM
            `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie`      AS ds

        WHERE
            is_multi_collect           = 1
            AND multi_collect_sequence = 1

        GROUP BY
            ccdd_id

    ),
    assignment_acceptance_diff AS (

        SELECT
            ds.sys_delivery_id                                                                                           AS sys_delivery_id,
            ds.ccdd_id                                                                                                   AS ccdd_id,
            ds.multi_collect_sequence                                                                                    AS multi_collect_sequence,
            ds.collect_assigned_time_local                                                                               AS collect_assigned_time_local,
            ds.collect_accepted_time_local                                                                               AS collect_accepted_time_local,
            ABS(DATETIME_DIFF(ds.collect_assigned_time_local, LAG(ds.collect_assigned_time_local)
                                                                    OVER (PARTITION BY ds.ccdd_id
                                                                          ORDER BY ds.multi_collect_sequence), second))  AS assignment_time_gap_sec,
            ABS(DATETIME_DIFF(ds.collect_accepted_time_local, LAG(ds.collect_accepted_time_local)
                                                                    OVER (PARTITION BY ds.ccdd_id
                                                                          ORDER BY ds.multi_collect_sequence), second))  AS accepted_time_gap_sec
        FROM
            just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie       AS ds

    )

    SELECT
        ds.sys_delivery_id                                                                                                 AS sys_delivery_id,
        aad.assignment_time_gap_sec                                                                                        AS assignment_time_gap_sec,
        aad.accepted_time_gap_sec                                                                                          AS accepted_time_gap_sec,
        DATETIME_DIFF(DATETIME(TIMESTAMP(ds.delivery_begin_parking_time_local, ds.timezone)),
                      DATETIME(TIMESTAMP(tt.first_delivery_in_transit_time_local, ds.timezone)), SECOND)                   AS pooling_kpi_sec

    FROM
        `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie`                    AS ds
        LEFT JOIN first_delivery_in_transit_time                                                                           AS tt  ON  tt.ccdd_id = ds.ccdd_id
        LEFT JOIN assignment_acceptance_diff                                                                               AS aad ON  aad.sys_delivery_id = ds.sys_delivery_id
                                                                                                                                  AND aad.ccdd_id = ds.ccdd_id

),
--leaving the below in so as to not rock the boat too much
exchange_rates AS (

    SELECT
        'ca'  AS tenant_id,
        1.47  AS local_currency_to_euros

    UNION ALL

    SELECT
        'uk'  AS tenant_id,
        0.851 AS local_currency_to_euros

    UNION ALL

    SELECT
        'ie'  AS tenant_id,
        1     AS local_currency_to_euros

    UNION ALL

    SELECT
        'au'  AS tenant_id,
        1.579 AS local_currency_to_euros

    UNION ALL

    SELECT
        'nz'  AS tenant_id,
        1.6   AS local_currency_to_euros

)

SELECT
    ds.order_date                                                    AS order_date,
    ds.time_of_day_group                                             AS time_of_day_group,
    ds.day_of_week_local                                             AS day_of_week_local,
    ds.tenant_id                                                     AS tenant_id,
    ds.sys_courier_delivery_zone_id                                  AS sys_courier_delivery_zone_id,
    ds.delivery_zone_name                                            AS delivery_zone_name,
    ds.sys_zone_group_id                                             AS sys_zone_group_id,
    ds.zone_group_name                                               AS zone_group_name,
    ds.resto_type                                                    AS resto_type,
    ds.is_multi_collect                                              AS is_multi_collect,
    ds.is_assigned_by_holding_container                              AS is_assigned_by_holding_container,
    ds.pooling_in_transit_tag                                        AS pooling_in_transit_tag,
    ds.multi_collect_sequence                                        AS multi_collect_sequence,
    ds.is_mcdonalds                                                  AS is_mcdonalds,
    ds.delivery_status                                               AS delivery_status,
    ds.is_on_time_order                                              AS is_on_time_order,
    ds.delivery_completion_slack_bin                                 AS delivery_completion_slack_bin,
    ds.ctod_min_bin                                                  AS ctod_min_bin,
    ds.ptov_min_bin                                                  AS ptov_min_bin,
    ds.delivery_driving_distance_original_kms_bin                    AS delivery_driving_distance_original_kms_bin,
    ds.deliver_original_total_driving_time_min_bin                   AS deliver_original_total_driving_time_min_bin,
    ds.is_asap_order                                                 AS is_asap_order,
    ds.is_rejected_as_multi_collect                                  AS is_rejected_as_multi_collect,
    ds.is_assigned_as_multi_collect                                  AS is_assigned_as_multi_collect,
    ds.is_manual_unassigned                                          AS is_manual_unassigned,
    ds.is_cnmp_unassigned                                            AS is_cnmp_unassigned,
    ds.is_timer_expired                                              AS is_timer_expired,
    ds.is_offer_declined                                             AS is_offer_declined,
    CASE WHEN ds.company_net_adjustment_sum < 0
         THEN 1 ELSE 0 END                                           AS is_negative_company_net_adjustment,
    CASE WHEN ds.delivery_begin_parking_time_local IS NOT NULL
         THEN 1
         ELSE 0
          END                                                        AS has_begin_parking_time,
    CASE WHEN ti.has_ticket = 1
         THEN 1
         ELSE 0
          END                                                        AS has_ticket,
    COUNT(DISTINCT ds.sys_delivery_id)                               AS delivery_count,
    SUM(CASE WHEN ds.is_multi_collect = 1
             THEN IFNULL(tt.assignment_time_gap_sec,0)
             ELSE 0 END)                                     / 60.0  AS assignment_time_gap_mins_sum,
    SUM(CASE WHEN ds.is_multi_collect = 1
             THEN IFNULL(tt.accepted_time_gap_sec,0)
             ELSE 0 END)                                     / 60.0  AS acceptance_time_gap_mins_sum,
    SUM(IFNULL(ds.handle_time_sec,0))                        / 60.0  AS handle_time_mins_sum,
    SUM(IFNULL(ds.ccdd_courier_analysis_handle_time_sec,0))  / 60.0  AS pooling_second_order_handle_time_min_sum,
    SUM(IFNULL(ds.collect_total_driving_time_sec_max_ccdd,0))/ 60.0  AS collect_total_driving_time_saved_min_sum,
    SUM(IFNULL(ds.route_time_sec,0))                         / 60.0  AS route_time_min_sum,
    SUM(IFNULL(ds.pooling_capacity_gains_sec, 0))            / 60.0  AS pooling_capacity_gains_mins_sum,
    SUM(IFNULL(ds.ptod_sec,0))                               / 60.0  AS ptod_mins_sum,
    SUM(IFNULL(ds.ptoc_sec,0))                               / 60.0  AS ptoc_mins_sum,
    SUM(IFNULL(ds.ctod_sec,0))                               / 60.0  AS ctod_mins_sum,
    SUM(IFNULL(ds.ptov_sec,0))                               / 60.0  AS ptov_mins_sum,
    SUM(IFNULL(tt.pooling_kpi_sec,0))                        / 60.0  AS pooling_kpi_mins_sum,
    SUM(IFNULL(ds.resto_hold_sec,0))                         / 60.0  AS resto_hold_mins_sum,
    SUM(IFNULL(ds.in_resto_time_sec,0))                      / 60.0  AS in_resto_time_mins_sum,
    SUM(IFNULL(ds.accept_to_collect_sec,0))                  / 60.0  AS accept_to_collect_mins_sum,
    SUM(IFNULL(ds.accept_to_drive_to_resto_sec,0))            / 60.0 AS accept_to_drive_to_resto_mins_sum,
    SUM(IFNULL(ds.accept_to_arrive_at_resto_sec,0))           / 60.0 AS accept_to_arrive_at_resto_mins_sum,
    SUM(IFNULL(ds.collect_delay,0))                                  AS collect_delay_mins_sum,
    SUM(IFNULL(ds.courier_pay_without_tips,0))                       AS courier_pay_without_tips_sum,
    SUM(IFNULL(ds.courier_pay_without_tips_or_courier_adjustments,0)) AS courier_pay_without_tips_or_courier_adjustments_sum,
    SUM(IFNULL(rj.rejections_count,0))                               AS rejections_sum,
    COUNT(DISTINCT CASE WHEN rj.rejections_count > 0
                        THEN ds.sys_delivery_id END)                 AS rejected_offer_count,
   SUM(IFNULL(ds.food_subtotal,0))                                AS food_subtotal,
    SUM(IFNULL(ds.company_net_adjustment_sum,0))                     AS company_net_adjustment_sum,
    SUM(IFNULL(ds.courier_net_adjustment_sum,0))                     AS courier_net_adjustment_sum,
    SUM(IFNULL(ds.custo_net_adjustment_sum,0))                       AS custo_net_adjustment_sum,
    SUM(IFNULL(ds.resto_net_adjustment_sum,0))                       AS resto_net_adjustment_sum,
    SUM((IFNULL(ds.courier_pay_without_tips_or_courier_adjustments,0) -
         IFNULL(ds.company_net_adjustment_sum,0)))                   AS total_order_cost_sum /* want courier pay + paid out money - taken in money so using a -sign here*/,
    SUM((IFNULL(ds.courier_pay_without_tips,0)) /
               et.local_currency_to_euros)                           AS courier_pay_without_tips_euros_sum,
    SUM((IFNULL(ds.courier_pay_without_tips_or_courier_adjustments,0)) /
               et.local_currency_to_euros)                           AS courier_pay_without_tips_or_courier_adjustments_euros_sum,
    SUM(IFNULL(ds.company_net_adjustment_sum,0) /
               et.local_currency_to_euros)                           AS company_net_adjustment_euros_sum,
    SUM(IFNULL(ds.courier_net_adjustment_sum,0) /
               et.local_currency_to_euros)                           AS courier_net_adjustment_euros_sum,
    SUM(IFNULL(ds.custo_net_adjustment_sum,0) /
               et.local_currency_to_euros)                           AS custo_net_adjustment_euros_sum,
    SUM(IFNULL(ds.resto_net_adjustment_sum,0) /
               et.local_currency_to_euros)                           AS resto_net_adjustment_euros_sum,
    SUM((IFNULL(ds.courier_pay_without_tips_or_courier_adjustments,0) -
         IFNULL(ds.company_net_adjustment_sum,0))/
                et.local_currency_to_euros)                          AS total_order_cost_euros_sum /* want courier pay + paid out money - taken in money so using a -sign here*/,
    IFNULL(COUNT(DISTINCT rc.sys_delivery_id),0)                     AS orders_with_min_1_reorder_count,
    IFNULL(COUNT(DISTINCT rc.first_reorder_sys_delivery_id),0)       AS first_reorders_count,
    IFNULL(SUM(rc.days_between_current_order_and_first_reorder),0)   AS first_reorder_days_sum,
    IFNULL(SUM(rc.first_reorder_food_subtotal),0)   AS first_reorder_food_subtotal,

    IFNULL(COUNT(DISTINCT rc.second_reorder_sys_delivery_id),0)      AS second_reorders_count,
    IFNULL(SUM(rc.days_between_first_reorder_and_second_reorder),0)  AS second_reorder_days_sum,
    IFNULL(SUM(rc.second_reorder_food_subtotal),0)  AS second_reorder_food_subtotal,

    IFNULL(COUNT(DISTINCT rc.third_reorder_sys_delivery_id),0)       AS third_reorders_count,
    IFNULL(SUM(rc.days_between_second_reorder_and_third_reorder),0)  AS third_reorder_days_sum,
    IFNULL(SUM(rc.third_reorder_food_subtotal),0)  AS third_reorder_food_subtotal,

    IFNULL(COUNT(DISTINCT rc.fourth_reorder_sys_delivery_id),0)      AS fourth_reorders_count,
    IFNULL(SUM(rc.days_between_third_reorder_and_fourth_reorder),0)  AS fourth_reorder_days_sum,
   IFNULL(SUM(rc.fourth_reorder_food_subtotal),0)  AS fourth_reorder_food_subtotal,

	SUM(IFNULL(ds.collect_accessibility_minutes,0))                  AS restaurant_accessibility_time_mins,
	SUM(IFNULL(ds.courier_pay_without_tips,0) -
        IFNULL(ds.company_net_adjustment_sum,0) -
		IFNULL(ds.custo_net_adjustment_sum,0))                       AS total_order_cost_less_customer_refunds_sum,
	SUM(CASE WHEN ds.is_score_positive = 1
	         THEN 1 ELSE 0 END)                                      AS positive_scores_count,
	SUM(CASE WHEN ds.is_score_positive = 0
	         THEN 1 ELSE 0 END)                                      AS non_positive_scores_count

FROM
    `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie`             AS ds
    LEFT JOIN rejections                                                                    AS rj ON  rj.sys_delivery_id = ds.sys_delivery_id
    LEFT JOIN pooling_kpi                                                                   AS tt ON  tt.sys_delivery_id = ds.sys_delivery_id
    LEFT JOIN exchange_rates                                                                AS et ON  et.tenant_id       = ds.tenant_id
    LEFT JOIN `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_0_reorder_info_ie`       AS rc ON  rc.sys_delivery_id = ds.sys_delivery_id
    LEFT JOIN `just-data-warehouse.international_reporting.datamarts_order_pooling_wk_0_zendesk_ticket_info_ie` AS ti ON ti.sys_delivery_id  = ds.sys_delivery_id

GROUP BY
    ds.order_date,
    ds.time_of_day_group,
    ds.day_of_week_local,
    ds.tenant_id,
    ds.sys_courier_delivery_zone_id,
    ds.delivery_zone_name,
    ds.sys_zone_group_id,
    ds.zone_group_name,
    ds.resto_type,
    ds.is_multi_collect,
    ds.is_assigned_by_holding_container,
    ds.pooling_in_transit_tag,
    ds.multi_collect_sequence,
    ds.is_mcdonalds,
    ds.delivery_status,
    ds.is_on_time_order,
    ds.is_asap_order,
    ds.is_rejected_as_multi_collect,
    ds.is_assigned_as_multi_collect,
    ds.is_manual_unassigned,
    ds.is_cnmp_unassigned,
    ds.is_timer_expired,
    ds.is_offer_declined,
    ds.delivery_completion_slack_bin,
    ds.ctod_min_bin,
    ds.ptov_min_bin,
    ds.delivery_driving_distance_original_kms_bin,
    ds.deliver_original_total_driving_time_min_bin,
    is_negative_company_net_adjustment,
    has_begin_parking_time,
    has_ticket
