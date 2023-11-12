WITH target_dates AS (
    SELECT
		CASE
        WHEN DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), WEEK) <= '2022-03-21'
		THEN DATE('2022-03-21')
		ELSE DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) END                              AS start_date, /* Three months old date. Example, 24th March 2022 if today is 24th June 2022. */
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)                                         AS end_date /* Yesterday. Example, 23rd June 2022 if today is 24th June 2022. */

), deliveries AS (
    SELECT DISTINCT
        fo.order_date                                               AS order_date,
        dz.zone_name                                                AS zone_name,
        fo.tenant_id                                                AS tenant_id,
        ROUND(SUM(fd.handle_time_sec) /60.0,2)                      AS handle_time_min_sum,
        ROUND(SUM(CASE WHEN COALESCE(ccdd.is_multi_collect,0) = 1
                  THEN ccdd.ccdd_courier_analysis_handle_time_sec
                  ELSE fd.handle_time_sec END)/60.0,2)              AS ccdd_handle_time_min_sum,


    FROM
        `just-data-warehouse.delco_analytics_team_dwh.fact_orders_resto`                                 AS fo
        INNER JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_deliveries`                        AS fd ON fd.sys_order_id  = fo.sys_order_id
        INNER JOIN  `just-data-warehouse.delco_analytics_team_dwh.bridge_courier_shift_delivery`         AS cs ON fd.sys_delivery_id = cs.sys_delivery_id
        INNER JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_courier_delivery_zones`             AS dz ON fd.sys_courier_delivery_zone_id = dz.sys_delivery_zone_id
        LEFT JOIN   `just-data-warehouse.delco_analytics_team_dwh.bridge_courier_multi_collect_delivery` AS ccdd ON ccdd.sys_delivery_id = fd.sys_delivery_id

    WHERE
        fd.delivery_status = 'DELIVERED'
        AND fo.order_date BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)

     GROUP BY
        fo.order_date,
        dz.zone_name,
        fo.tenant_id

),worked_hours AS (
    SELECT
        courier_shifts.tenant_id                                                                       AS tenant_id,
        courier_shifts.start_date                                                                      AS courier_shift_start_date,
        dz.zone_name                                                                                   AS zone_name,
        ROUND(SUM(courier_shifts.shift_time_sec - COALESCE(courier_shifts.dnu_time_sec, 0)) / 60.0,2)  AS time_on_shift_min_sum

    FROM
        `just-data-warehouse.delco_analytics_team_dwh.dim_courier_shifts`                   AS courier_shifts
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_couriers`               AS courier ON courier.sys_courier_id = courier_shifts.sys_courier_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_courier_delivery_zones` AS dz      ON courier_shifts.delivery_zone_id = dz.sys_delivery_zone_id

	WHERE
	    courier_shifts.start_date BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)

    GROUP BY
        courier_shifts.tenant_id,
        courier_shifts.start_date,
        dz.zone_name

)
SELECT DISTINCT
    del.order_date                          AS order_date,
    del.tenant_id                           AS tenant_id,
    del.zone_name                           AS zone_name,
    worked.time_on_shift_min_sum            AS time_on_shift_min_sum,
    del.handle_time_min_sum                 AS handle_time_min_sum,
    del.ccdd_handle_time_min_sum            AS ccdd_handle_time_min_sum

FROM
    deliveries                  AS del
    LEFT JOIN worked_hours      AS worked ON del.tenant_id = del.tenant_id
                                          AND del.zone_name = worked.zone_name
                                          AND del.order_date  = worked.courier_shift_start_date
WHERE
    del.zone_name NOT LIKE 'SAT - %'