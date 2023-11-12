WITH target_dates AS (

    SELECT
		CASE
		WHEN DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), WEEK) <= '2022-03-21'
		THEN DATE('2022-03-21')
		ELSE DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) END                              AS start_date, /* Three months old date. Example, 24th March 2022 if today is 24th June 2022. */ 
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)                                         AS end_date /* Yesterday. Example, 23rd June 2022 if today is 24th June 2022. */

),

delivery_staging AS (

    SELECT
        fo.order_date                                                                            AS order_date,
        fd.tenant_id                                                                             AS tenant_id,
        fd.timezone                                                                              AS timezone,
        fd.sys_courier_delivery_zone_id                                                          AS sys_courier_delivery_zone_id,
        dz.name                                                                                  AS delivery_zone_name,
        dz.sys_zone_group_id                                                                     AS sys_zone_group_id,
        dz.zone_group_name                                                                       AS zone_group_name,
        ROUND(ST_AREA(dz.polygon)/1000000,1)                                                     AS zone_area_square_km,
        cz.sys_courier_delivery_zone_id                                                          AS sys_courier_delviery_zone_id_resto,
        cz.name                                                                                  AS delivery_zone_name_resto,
        ROUND(ST_AREA(cz.polygon)/1000000,1)                                                     AS zone_area_square_km_resto,
        fd.sys_courier_id                                                                        AS sys_courier_id,
        fd.sys_delivery_id                                                                       AS sys_delivery_id,
        fo.sys_order_id                                                                          AS sys_order_id,
        fo.order_number                                                                          AS order_number,
        fo.just_eat_order_number                                                                 AS just_eat_order_number,
        fd.delivery_created_time_local                                                           AS delivery_created_time_local,
        fd.delivery_in_transit_time_local                                                        AS delivery_in_transit_time_local,
        fd.collect_assigned_time_local                                                           AS collect_assigned_time_local,
        fd.collect_accepted_time_local                                                           AS collect_accepted_time_local,
        fd.collect_arrived_time_local                                                            AS collect_arrived_time_local,
        fd.collect_completed_time_local                                                          AS collect_completed_time_local,
        fd.delivery_begin_parking_time_local                                                     AS delivery_begin_parking_time_local,
        cd.ccdd_id                                                                               AS ccdd_id,
        cd.multi_collect_sequence                                                                AS multi_collect_sequence,
        fo.sys_resto_id                                                                          AS sys_resto_id,
        dr.resto_type                                                                            AS resto_type,
        fo.sys_custo_id                                                                          AS sys_custo_id,
        fd.delivery_status                                                                       AS delivery_status,
        IFNULL(cd.is_multi_collect,0)                                                            AS is_multi_collect,
		cd.is_assigned_by_holding_container                                                      AS is_assigned_by_holding_container,
        CASE WHEN cd.multi_collect_in_transit = 1
             THEN 'Assigned In-Transit'
             ELSE 'Not Assigned In-Transit' END                                                  AS pooling_in_transit_tag,
        fd.handle_time_sec                                                                       AS handle_time_sec,
        IFNULL(fm.courier_pay_without_tips,0) -
        IFNULL(fm.courier_net_adjustment,0)                                                      AS courier_pay_without_tips_or_courier_adjustments, /* excludes courier adjustments */
        IFNULL(fm.courier_pay_without_tips,0)                                                    AS courier_pay_without_tips, /* includes courier adjustments */
        IFNULL(fo.food_subtotal, 0)                                                              AS food_subtotal,
        fd.collect_total_driving_time_min                                                        AS collect_total_driving_time_min,
        fd.delivery_total_driving_time_min                                                       AS delivery_total_driving_time_min,
        CASE WHEN cd.multi_collect_sequence = 2
             THEN MAX(fd.collect_total_driving_time_min) OVER (PARTITION BY cd.ccdd_id) * 60
             ELSE 0 END                                                                          AS collect_total_driving_time_sec_max_ccdd,
        cd.ccdd_courier_analysis_handle_time_sec                                                 AS ccdd_courier_analysis_handle_time_sec,
        CASE WHEN cd.multi_collect_sequence = 2
             THEN LAG(fd.delivery_total_driving_time_min, 1) OVER
                        (PARTITION BY cd.ccdd_id
                         ORDER BY     cd.multi_collect_sequence)
                  * 60
             ELSE 0 END                                                                          AS delivery_total_driving_time_min_previous_leg,
        CASE WHEN cd.multi_collect_sequence = 2
             THEN
                MAX(fd.collect_total_driving_time_min) OVER (PARTITION BY cd.ccdd_id)
                 * 60
              + cd.ccdd_courier_analysis_handle_time_sec
              - LAG(fd.delivery_total_driving_time_min, 1)
                  OVER (PARTITION BY cd.ccdd_id
                        ORDER BY     cd.multi_collect_sequence)
                  * 60
             ELSE 0 END                                                                          AS pooling_capacity_gains_sec, -- Avijeet: calculate for second dropoff as 99%+ of pooled orders are 2 deliveries
        fd.ptod_sec                                                                              AS ptod_sec,
        fd.ptoc_sec                                                                              AS ptoc_sec,
        fd.ctod_sec                                                                              AS ctod_sec,
        ROUND(fd.collect_driving_distance_original_meters / 1000.0,2)                            AS collect_driving_distance_original_kms,
        ROUND(fd.delivery_driving_distance_original_meters / 1000.0,2)                           AS delivery_driving_distance_original_kms,
        fd.collect_original_total_driving_time_min                                               AS collect_original_total_driving_time_min,
        fd.deliver_original_total_driving_time_min                                               AS deliver_original_total_driving_time_min,
        IFNULL(fo.resto_food_prep_min,0)                                                         AS resto_food_prep_min,
        IFNULL(fo.resto_delay_requested_min,0)                                                   AS resto_delay_requested_min,
        CASE WHEN fo.is_asap_order = 1
             THEN IFNULL(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_accepted_time_local, fd.timezone)),
                         DATETIME(TIMESTAMP(fd.delivery_created_time_local, fd.timezone)), SECOND),0)
              END                                                                                AS ptov_sec,
        CASE WHEN fo.is_asap_order = 1
             THEN IFNULL(DATETIME_DIFF(
                         DATETIME_ADD(DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, fd.timezone)),
                                               INTERVAL (IFNULL(fd.collect_accessibility_minutes,0) +
                                                         IFNULL(fd.collect_original_total_driving_time_min,0)) MINUTE),
                         DATETIME_ADD(DATETIME(TIMESTAMP(fd.delivery_created_time_local, fd.timezone)),
                                               INTERVAL (IFNULL(fo.resto_food_prep_min,0) +
                                                         IFNULL(fo.resto_delay_requested_min,0)) MINUTE)
              , SECOND),0)
              END                                                                                  AS collect_leg_delay_sec,
        IFNULL(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, fd.timezone)),
                          DATETIME(TIMESTAMP(fd.collect_accepted_time_local, fd.timezone)), SECOND),0)   AS vtoa_sec,
        IFNULL(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_arrived_time_local, fd.timezone)),
                          DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, fd.timezone)), SECOND),0) AS atoar_sec,
        fd.resto_hold_sec                                                                        AS resto_hold_sec,

        IFNULL(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.delivery_arrived_time_local, fd.timezone)),
                             DATETIME(TIMESTAMP(fd.delivery_in_transit_time_local, fd.timezone))
                             , SECOND),0)                                                        AS drive_to_custo_sec,
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, fd.timezone)),
                            DATETIME(TIMESTAMP(fd.collect_accepted_time_local, fd.timezone)),
                            MILLISECOND)/1000, 0)                                                AS accept_to_drive_to_resto_sec,
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_arrived_time_local, fd.timezone)),
                            DATETIME(TIMESTAMP(fd.collect_accepted_time_local, fd.timezone)),
                            MILLISECOND)/1000, 0)                                                AS accept_to_arrive_at_resto_sec,
        fd.custo_hold_sec                                                                        AS custo_hold_sec,
        DATETIME_DIFF(DATETIME(TIMESTAMP(fd.delivery_begin_parking_time_local, fd.timezone)),
                      DATETIME(TIMESTAMP(fd.delivery_created_time_local, fd.timezone)), SECOND)  AS end_to_end_time_sec,
        DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_completed_time_local, fd.timezone)),
                      DATETIME(TIMESTAMP(fd.collect_arrived_time_local, fd.timezone)), SECOND) -
        (2 * (fd.collect_accessibility_minutes * 60))                                            AS in_resto_time_sec,

        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_completed_time_local, "UTC")),
                    DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                    MILLISECOND)/1000, 0)                                                        AS accept_to_collect_sec,
        
        DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_arrived_time_local, fd.timezone)),
                      DATETIME(TIMESTAMP(fd.delivery_created_time_local, fd.timezone)), SECOND)  AS route_time_sec,

        ROUND((IFNULL(fd.collect_total_driving_time_min,0) -
               IFNULL(fd.collect_original_total_driving_time_min,0)),0)                          AS collect_delay,
        ROUND((IFNULL(fd.delivery_total_driving_time_min,0) -
        IFNULL(fd.deliver_original_total_driving_time_min,0)),0)                                 AS deliver_delay,
        CASE WHEN crp.grouping = 'McDonald%s'
               OR dr.resto_name_short LIKE '%McDonald%s%' THEN 1 ELSE 0 END                      AS is_mcdonalds,
        DATETIME(ij.injection_time)                                                                      AS pos_injection_time_utc,
        ij.fire_into_pos_time                                                                            AS pos_fire_into_pos_time_min,
        DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, fd.timezone))                       AS collect_in_transit_time_utc,
        CASE WHEN fd.delivery_completion_slack_min BETWEEN 0 AND 10
             THEN 1
             ELSE 0
             END                                                                                 AS is_on_time_order,
        CASE WHEN fd.delivery_completion_slack_min < 0
             THEN 'Early'
             WHEN fd.delivery_completion_slack_min BETWEEN 0  AND 10
             THEN '0 to 10 Mins'
             WHEN fd.delivery_completion_slack_min BETWEEN 11 AND 15
             THEN '11 to 15 Mins'
             WHEN fd.delivery_completion_slack_min BETWEEN 16 AND 20
             THEN '16 to 20 Mins'
             WHEN fd.delivery_completion_slack_min BETWEEN 21 AND 25
             THEN '21 to 25 Mins'
             WHEN fd.delivery_completion_slack_min BETWEEN 26 AND 30
             THEN '26 to 30 Mins'
             WHEN fd.delivery_completion_slack_min BETWEEN 30 AND 40
             THEN '30 to 40 Mins'
             WHEN fd.delivery_completion_slack_min > 40
             THEN '>40 Mins'
             ELSE ' N/A'
              END                                                                                AS delivery_completion_slack_bin,
       CASE  WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 0 AND 5
             THEN '0 to 5 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 6 AND 8
             THEN '6 to 8 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 9 AND 11
             THEN '9 to 11 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 12 AND 15
             THEN '12 to 15 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 16 AND 20
             THEN '16 to 20 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) BETWEEN 21 AND 25
             THEN '21 to 25 Mins'
             WHEN FLOOR(fd.ctod_sec/60.0) > 26
             THEN '> 25 Mins'
             ELSE ' N/A'
              END                                                                                AS ctod_min_bin,
        CASE WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 0 AND 2
             THEN '0 to 2 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 3 AND 5
             THEN '3 to 5 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 6 AND 8
             THEN '6 to 8 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 9 AND 11
             THEN '9 to 11 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 12 AND 14
             THEN '12 to 14 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 15 AND 17
             THEN '15 to 17 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) BETWEEN 18 AND 20
             THEN '18 to 20 KMs'
             WHEN FLOOR(fd.delivery_driving_distance_original_meters / 1000.0) > 20
             THEN '21+ KMs'
              END                                                                                AS delivery_driving_distance_original_kms_bin,
        CASE WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 0 AND 2
             THEN '0 to 2 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 3 AND 5
             THEN '3 to 5 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 6 AND 8
             THEN '6 to 8 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 9 AND 11
             THEN '9 to 11 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 12 AND 14
             THEN '12 to 14 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 15 AND 17
             THEN '15 to 17 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) BETWEEN 18 AND 20
             THEN '18 to 20 Mins'
             WHEN FLOOR(fd.deliver_original_total_driving_time_min) > 20
             THEN '21+ Mins'
              END                                                                                 AS deliver_original_total_driving_time_min_bin,
		IFNULL(fo.is_asap_order,0)                                                               AS is_asap_order,
		fd.collect_accessibility_minutes                                                         AS collect_accessibility_minutes,
        fd.deliver_accessibility_minutes                                                         AS deliver_accessibility_minutes,
		IFNULL(rr.is_score_positive,0)                                                           AS is_score_positive,
        CASE WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 7 AND 10
             THEN 'Breakfast'
             WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 11 AND 15
             THEN 'Lunch'
             WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 16 AND 19
             THEN 'Dinner'
             WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 20 AND 23
             THEN 'After-Dinner'
             WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 0  AND 6
             THEN 'Overnight'
             END                                                                                         AS time_of_day_group,
       EXTRACT(DAYOFWEEK FROM DATETIME(fd.delivery_created_time_local))                                  AS day_of_week_local,
       CONCAT(fd.delivery_created_time_local,
       CASE WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 7 AND 10
            THEN 'Breakfast'
            WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 11 AND 15
            THEN 'Lunch'
            WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 16 AND 19
            THEN 'Dinner'
            WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 20 AND 23
            THEN 'After-Dinner'
            WHEN EXTRACT(HOUR FROM fd.delivery_created_time_local) BETWEEN 0  AND 6
            THEN 'Overnight'
            END )                                                                                        AS day_of_week_and_time_of_day_group,
            ga.company_net_adjustment_sum                                                                AS company_net_adjustment_sum,
            ga.na_company_net_adjustment_sum                                                             AS na_company_net_adjustment_sum,
            ga.nc_total_net_compensation_sum                                                             AS nc_total_net_compensation_sum,
            ga.courier_net_adjustment_sum                                                                AS courier_net_adjustment_sum,
            ga.custo_net_adjustment_sum                                                                  AS custo_net_adjustment_sum,
            ga.na_custo_net_adjustment_sum                                                               AS na_custo_net_adjustment_sum,
            ga.nc_custo_net_compensation_sum                                                             AS nc_custo_net_compensation_sum,
            ga.resto_net_adjustment_sum                                                                  AS resto_net_adjustment_sum,
            ga.na_resto_net_adjustment_sum                                                               AS na_resto_net_adjustment_sum,
            ga.nc_resto_net_compensation_sum                                                             AS nc_resto_net_compensation_sum


    FROM
        `just-data-warehouse.delco_analytics_team_dwh.fact_deliveries`                                 AS fd
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_orders_resto`                     AS fo  ON  fo.sys_order_id                 = fd.sys_order_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto`                             AS dr  ON  dr.sys_resto_id                 = fo.sys_resto_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.bridge_courier_multi_collect_delivery` AS cd  ON  cd.sys_delivery_id              = fd.sys_delivery_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_order_financial_metrics`          AS fm  ON  fm.sys_order_id                 = fo.sys_order_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_courier_delivery_zones`            AS dz  ON  dz.sys_courier_delivery_zone_id = fd.sys_courier_delivery_zone_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto_grouping_crp`                AS crp ON  crp.sys_resto_id                = dr.sys_resto_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_resto_reviews`                    AS rr  ON  rr.sys_order_id                 = fo.sys_order_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_courier_delivery_zones`            AS cz  ON  cz.sys_courier_delivery_zone_id = dr.sys_delivery_zone_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_datamarts.global_adjustments`              AS ga  ON  ga.sys_delivery_id              = fd.sys_delivery_id
        LEFT JOIN `just-data-warehouse.clean_skip_data_lake.restaurant_order_flmt_data_mart_pos_injection_information_*` AS ij ON ij.order_id     = fo.sys_order_id
   
    WHERE
            fo.order_date BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND fd.delivery_status = 'DELIVERED'
        AND fd.tenant_id =  'ie'

),assignments AS (

    SELECT
        ds.sys_delivery_id,
        MAX(CAST(cs.assigned_as_multi_collect AS INT64)) AS is_assigned_as_multi_collect


    FROM
        delivery_staging AS ds
        LEFT JOIN
            (
            SELECT
                courierId                                                                    AS sys_courier_id,
                sys_delivery_id                                                              AS sys_delivery_id,
                ARRAY_LENGTH(REGEXP_EXTRACT_ALL(ul.jobChanges.assignedJobs, 'COLLECT')) > 1  AS assigned_as_multi_collect

            FROM
                `just-data-warehouse.clean_skip_data_lake.je_assignment_change_log_courier_assignment_received_log_*` AS ul
            , UNNEST(REGEXP_EXTRACT_ALL(ul.jobChanges.assignedJobs, '([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})')) AS sys_delivery_id

            QUALIFY ROW_NUMBER() OVER (PARTITION BY courierId, sys_delivery_id ORDER BY timestamp DESC) = 1
            ) AS cs ON cs.sys_delivery_id = ds.sys_delivery_id

    GROUP BY
        ds.sys_delivery_id

),
rejections AS (

    SELECT
        ds.sys_delivery_id                                     AS sys_delivery_id,
        SUM(CAST(bc.is_multi_collect AS INT64))                AS is_rejected_as_multi_collect_count,
        IFNULL(MAX(CAST(bc.is_multi_collect AS INT64)),0)      AS is_rejected_as_multi_collect,
        SUM(CAST(bc.is_manual_unassigned AS INT64))            AS is_manual_unassigned_count,
        IFNULL(MAX(CAST(bc.is_manual_unassigned AS INT64)),0)  AS is_manual_unassigned,
        SUM(CAST(bc.is_cnmp_unassigned AS INT64))              AS is_cnmp_unassigned_count,
        IFNULL(MAX(CAST(bc.is_cnmp_unassigned AS INT64)),0)    AS is_cnmp_unassigned,
        SUM(CAST(bc.is_timer_expired AS INT64))                AS is_timer_expired_count,
        IFNULL(MAX(CAST(bc.is_timer_expired AS INT64)),0)      AS is_timer_expired,
        SUM(CAST(bc.is_offer_declined AS INT64))               AS is_offer_declined_count,
        IFNULL(MAX(CAST(bc.is_offer_declined AS INT64)),0)     AS is_offer_declined

    FROM
        delivery_staging                                                                                      AS ds
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.bridge_delivery_details_blacklisted_couriers` AS bc ON bc.sys_delivery_id = ds.sys_delivery_id

    GROUP BY
        ds.sys_delivery_id
),
pooling_predicted_drive_time AS (

    SELECT
        ccdd_id                                 AS ccdd_id,
        sys_delivery_id                         AS sys_delivery_id,
        deliver_original_total_driving_time_min AS first_order_deliver_original_total_driving_time_min

    FROM
        delivery_staging

    WHERE
        ccdd_id IS NOT NULL
        AND multi_collect_sequence = 1
),
delivery_hold_container AS (

    WITH hold_container_arch_live AS (
        SELECT
            delivery_id                                                        AS sys_delivery_id,
            is_in_hold_container                                               AS is_in_hold_container,
            time_window_start                                                  AS time_window_start_utc,        /*time the delivery entered the holding container*/
            time_window_end                                                    AS time_window_end_utc,          /*planned time the delivery exited the holding container*/
            time_window_end_original                                           AS time_window_end_original_utc, /*actual time the delivery exited the holding container*/
            DATETIME_DIFF(time_window_end, time_window_start, SECOND)          AS planned_holding_window_sec,
            DATETIME_DIFF(time_window_end_original, time_window_start, SECOND) AS actual_holding_window_sec

        FROM
            `just-data.production_je_ecom_sub_skip.deliveries_delivery_hold_container_v1_*`

		UNION ALL

		SELECT
            delivery_id                                                        AS sys_delivery_id,
            is_in_hold_container                                               AS is_in_hold_container,
            time_window_start                                                  AS time_window_start_utc,        /*time the delivery entered the holding container*/
            time_window_end                                                    AS time_window_end_utc,          /*planned time the delivery exited the holding container*/
            time_window_end_original                                           AS time_window_end_original_utc, /*actual time the delivery exited the holding container*/
            DATETIME_DIFF(time_window_end, time_window_start, SECOND)          AS planned_holding_window_sec,
            DATETIME_DIFF(time_window_end_original, time_window_start, SECOND) AS actual_holding_window_sec

        FROM
            `just-data.production_je_ecom_sub_skip.deliveries_archive_delivery_hold_container_v1_*`
    )

	SELECT
		*
	FROM
		hold_container_arch_live

    QUALIFY
        ROW_NUMBER()OVER(PARTITION BY sys_delivery_id ORDER BY time_window_start_utc DESC) = 1 /*Removing duplicates and taking the latest record in case of re-assignment*/
)

SELECT
    ds.*,
    cs.is_assigned_as_multi_collect,
    rj.is_rejected_as_multi_collect_count,
    rj.is_rejected_as_multi_collect,
    rj.is_manual_unassigned_count,
    rj.is_manual_unassigned,
    rj.is_cnmp_unassigned_count,
    rj.is_cnmp_unassigned,
    rj.is_timer_expired_count,
    rj.is_timer_expired,
    rj.is_offer_declined_count,
    rj.is_offer_declined,
    pd.first_order_deliver_original_total_driving_time_min,
    CASE  WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 0 AND 2
           AND ds.is_asap_order = 1
          THEN '0 to 2 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 3 AND 5
           AND ds.is_asap_order = 1
          THEN '3 to 5 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 6 AND 8
           AND ds.is_asap_order = 1
          THEN '6 to 8 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 9 AND 11
           AND ds.is_asap_order = 1
          THEN '9 to 11 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 12 AND 14
           AND ds.is_asap_order = 1
          THEN '12 to 14 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 15 AND 17
           AND ds.is_asap_order = 1
          THEN '15 to 17 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) BETWEEN 18 AND 21
           AND ds.is_asap_order = 1
          THEN '18 to 21 Mins'
          WHEN FLOOR(ds.ptov_sec/60.0) > 22
           AND ds.is_asap_order = 1
          THEN '> 21 Mins'
          ELSE ' N/A'
           END                                                          AS ptov_min_bin,
    DATETIME(TIMESTAMP(dhc.time_window_start_utc),ds.timezone)          AS time_window_start_local,
    DATETIME(TIMESTAMP(dhc.time_window_end_utc),ds.timezone)            AS time_window_end_local,
    DATETIME(TIMESTAMP(dhc.time_window_end_original_utc),ds.timezone)   AS time_window_end_original_local,
    dhc.planned_holding_window_sec                                      AS planned_holding_window_sec,
    dhc.actual_holding_window_sec                                       AS actual_holding_window_sec,
    (dhc.actual_holding_window_sec - dhc.planned_holding_window_sec)    AS actual_vs_planned_diff

FROM
    delivery_staging                       AS ds
    LEFT JOIN pooling_predicted_drive_time AS pd  ON pd.ccdd_id          = ds.ccdd_id
    LEFT JOIN delivery_hold_container      AS dhc ON dhc.sys_delivery_id = ds.sys_delivery_id
    LEFT JOIN rejections                   AS rj  ON rj.sys_delivery_id  = ds.sys_delivery_id
    LEFT JOIN assignments                  AS cs  ON cs.sys_delivery_id  = ds.sys_delivery_id
