### Amendments made  ###
 #Late order contact added in

#changed the target dates here
WITH target_dates AS (

    SELECT
		CASE
		WHEN DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), WEEK) <= '2022-08-21'
		THEN DATE('2022-03-21')
		ELSE DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) END                              AS start_date, /* One months old date. Example, 24th March 2022 if today is 24th June 2022. */
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)                                         AS end_date /* Yesterday. Example, 23rd June 2022 if today is 24th June 2022. */

),

#Chagne around these rejections
rejection_counts AS (

    SELECT
        bc.sys_delivery_id                        AS sys_delivery_id,
        COUNT(DISTINCT bc.sys_courier_id)         AS rejections

    FROM
        `just-data-warehouse.delco_analytics_team_dwh.bridge_delivery_details_blacklisted_couriers` AS bc
        WHERE tenant_id = 'ie'

    GROUP BY
        bc.sys_delivery_id

),

# will have to look back at this but might be able to keep it as is
net_adjustments AS (

    SELECT
        fa.sys_order_id                                               AS sys_order_id,
		STRING_AGG(DISTINCT(CAST(fa.scenario AS STRING)), ', ')       AS all_scenarios,
		ROUND(SUM(fa.company_net_adjustment), 3)                      AS total_company_net_adjustment

    FROM
        `just-data-warehouse.delco_analytics_team_dwh.fact_order_net_adjustments` AS fa
        WHERE tenant_id = 'ie'

    GROUP BY
        fa.sys_order_id

),

### Added in the below CTE
late_order_contacts AS (   
    
    SELECT DISTINCT 
        orders.order_id_local                                          AS order_id_local,
        CASE WHEN (COUNT(zendesk.ticket_id_local) >=1) then 1 ELSE 0 END  AS late_order_contact

    FROM `just-data-warehouse.opensource_ecommerce.tickets`
        WHERE order_date.order_date_local BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND company.country_code = 'IE'
        AND (zendesk.query_type LIKE '%rds_late%' OR zendesk.query_type LIKE '%late_order%')
    GROUP BY 1

    
),

### Added in the below CTE as well
food_quality_contacts AS (   
    
    SELECT DISTINCT 
        orders.order_id_local                                          AS order_id_local,
        CASE WHEN (COUNT(zendesk.ticket_id_local) >=1) then 1 ELSE 0 END  AS food_quality_contact

    FROM `just-data-warehouse.opensource_ecommerce.tickets`
        WHERE order_date.order_date_local BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND company.country_code = 'IE'
        AND (zendesk.query_type LIKE '%food_quality%' OR zendesk.query_type LIKE '%quality_of_food%')
    GROUP BY 1

    
),


deliveries_data_lvl1 AS (
#definately change the rejections
    SELECT
        fd.tenant_id                                                                                      AS tenant_id,
        fd.sys_courier_delivery_zone_id                                                                   AS sys_courier_delivery_zone_id,
        fo.sys_order_id                                                                                   AS sys_order_id,
        fo.order_date                                                                                    AS order_date,
        /* ROUND(ST_AREA(dz.polygon)/1000000, 3)                                                          AS delivery_polygon_size, */
        /* CASE WHEN fo.is_asap_order = 1 THEN fd.ptod_sec ELSE NULL END                                  AS asap_ptod_sec, */
        /* CASE WHEN fo.is_asap_order = 1
		     THEN TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                        DATETIME(TIMESTAMP(fd.delivery_created_time_local, "UTC")),
                        MILLISECOND)/1000, 0)
             ELSE NULL END                                                                                AS asap_ptocourieraccept_sec, */
        /* CASE WHEN fo.is_asap_order = 1
		     THEN TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_assigned_time_local, "UTC")),
                        DATETIME(TIMESTAMP(fd.delivery_created_time_local, "UTC")),
                        MILLISECOND)/1000, 0)
             ELSE NULL END                                                                                AS asap_ptoassigned_sec, */
        fd.ctod_sec                                                                                       AS ctod_sec,
        /* CASE WHEN fo.is_asap_order = 1 THEN fd.ptoc_sec ELSE NULL END                                  AS asap_ptoc_sec, */
        /* DATETIME(TIMESTAMP(fd.collect_completed_time_local, "UTC"))                                    AS collected_utc, */
        /* DATETIME(TIMESTAMP(fd.delivery_completed_time_local, "UTC"))                                   AS delivered_utc, */
        DATETIME(TIMESTAMP(fd.delivery_created_time_local, "UTC"))                                        AS payment_utc,
        /* CASE WHEN cd.ccdd_id IS NULL
             THEN NULL
             ELSE FIRST_VALUE(DATETIME(TIMESTAMP(fd.delivery_completed_time_local, "UTC")))
             OVER (PARTITION BY cd.ccdd_id ORDER BY cd.multi_collect_sequence) END                        AS first_del_utc, */
        /* LEAD(DATETIME(TIMESTAMP(fd.delivery_created_time_local, "UTC")))
             OVER (PARTITION BY fd.sys_resto_id
             ORDER BY DATETIME(TIMESTAMP(fd.delivery_created_time_local, "UTC")))                         AS next_payment_utc, */
        cd.is_multi_collect                                                                               AS is_multi_collect,
        /* CASE WHEN cd.is_multi_collect = 1 THEN TRUE ELSE FALSE END                                     AS multi_collect_flag, */
        /* fd.collect_original_total_driving_time_min                                                     AS collect_original_total_driving_time_min, */
        fd.deliver_original_total_driving_time_min                                                        AS deliver_original_total_driving_time_min,
        COALESCE(rj.rejections,0)                                                                         AS rejections,
        COALESCE(loc.late_order_contact,0)                                                                AS late_order_contacts,
        COALESCE(fqc.food_quality_contact,0)                                                              AS food_quality_contacts,
        /* rg.resto_name_short                                                                            AS parent_group_name, */
        /* rc.cuisine                                                                                     AS primary_cuisine, */
        na.total_company_net_adjustment                                                                   AS voucher_refunds_issued_total_amount,
        na.all_scenarios                                                                                  AS voucher_refund_reason,
        /* cd.sys_courier_id	                                                                          AS sys_courier_id, */
        /* cd.ccdd_id	                                                                                  AS ccdd_id, */
        cd.multi_collect_sequence	                                                                      AS multi_collect_sequence,
        /* TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_assigned_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS assigned_to_accept_sec, */
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_completed_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS accept_to_collect_sec,
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS accept_to_drive_to_rest_sec,
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_arrived_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS accept_to_arrive_at_rest_sec,
        /* TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_arrived_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS drive_to_arrive_at_rest_sec, */
        TRUNC(DATETIME_DIFF(DATETIME(TIMESTAMP(fd.collect_completed_time_local, "UTC")),
                            DATETIME(TIMESTAMP(fd.collect_arrived_time_local, "UTC")),
                            MILLISECOND)/1000, 0)                                                         AS arrive_at_rest_to_collect_sec,
        /* DATETIME(fd.collect_accepted_time_local)                                                       AS accepted_local, */
        /* DATETIME(fd.collect_assigned_time_local)                                                       AS assigned_local, */
        /* DATETIME(TIMESTAMP(fd.collect_assigned_time_local, "UTC"))                                     AS assigned_utc, */
        /* DATETIME(TIMESTAMP(fd.collect_in_transit_time_local, "UTC"))                                   AS driving_to_restaurant_utc, */
        /* DATETIME(TIMESTAMP(fd.collect_arrived_time_local, "UTC"))                                      AS arrived_at_rest_utc, */
        /* fo.order_key                                                                                   AS order_key, */
        fd.resto_hold_sec                                                                                 AS resto_hold_sec,
        /* fd.collect_hold_accuracy                                                                       AS collect_hold_accuracy, */
        /* dz.zone_name                                                                                   AS courier_zone_name, */
        dz.name                                                                                           AS delivery_zone_name,
        /* fd.sys_courier_id                                                                              AS courier_id, */
        /* fd.sys_delivery_id                                                                             AS delivery_key, */
        fd.delivery_status                                                                                AS delivery_status,
        CASE WHEN fd.delivery_completion_slack_min BETWEEN 0 AND 10
             THEN 1
             ELSE 0
             END                                                                                          AS is_on_time_order,
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
             END                                                                                          AS delivery_completion_slack_bin,
        IFNULL(fo.is_asap_order,0)                                                                        AS is_asap_order,
        /* DATETIME(TIMESTAMP(fd.collect_accepted_time_local, "UTC"))                                     AS collect_accepted_date_utc, */
		fd.delivery_begin_parking_time_local                                                              AS delivery_begin_parking_time_local,
        CASE WHEN cd.sys_delivery_id IS NOT NULL THEN cd.ccdd_id ELSE fd.sys_delivery_id END              AS del_key,
        /* fm.courier_pay_with_tips                                                                       AS courier_pay_with_tips, */
        /* dr.resto_name                                                                                  AS restaurant_name, */
        dr.resto_type                                                                                     AS resto_type,
        /* dr.location_address                                                                            AS geog_pt_restaurant, */
        /* dr.location_latitude                                                                           AS rest_lat, */
        /* dr.location_longitude                                                                          AS rest_lon, */
        CASE WHEN crp.grouping = 'McDonald%s'
               OR dr.resto_name_short LIKE '%McDonald%s%' THEN 1 ELSE 0 END                               AS is_mcdonalds,
        ROW_NUMBER () OVER (PARTITION BY fo.sys_order_id)                                                 AS record_rank

    FROM
        `just-data-warehouse.delco_analytics_team_dwh.fact_deliveries`                                 AS fd
        INNER JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_orders_resto`                    AS fo  ON fo.sys_order_id                 = fd.sys_order_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto`                             AS dr  ON dr.sys_resto_id                 = fd.sys_resto_id
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.bridge_courier_multi_collect_delivery` AS cd  ON cd.sys_delivery_id              = fd.sys_delivery_id
        /* LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto_grouping_crp`             AS rg  ON rg.sys_resto_id                 = fd.sys_resto_id */
        /* LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto_cuisines`                 AS rc  ON rc.sys_resto_id                 = fd.sys_resto_id */
        /* LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_order_financial_metrics`       AS fm  ON fm.sys_delivery_id              = fd.sys_delivery_id */
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_courier_delivery_zones`            AS dz  ON dz.sys_courier_delivery_zone_id = fd.sys_courier_delivery_zone_id
        LEFT JOIN net_adjustments                                                                      AS na  ON na.sys_order_id                 = fd.sys_order_id
		LEFT JOIN rejection_counts                                                                     AS rj  ON rj.sys_delivery_id              = fd.sys_delivery_id
		LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.dim_resto_grouping_crp`                AS crp ON crp.sys_resto_id                = dr.sys_resto_id
        LEFT JOIN late_order_contacts                                                                  AS loc ON loc.order_id_local              = fo.just_eat_order_number
        LEFT JOIN food_quality_contacts                                                                AS fqc ON fqc.order_id_local              = fo.just_eat_order_number

    WHERE
        fo.order_date BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND fd.delivery_created_time_local BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND fd.delivery_status = "DELIVERED"
        AND fd.tenant_id = 'ie'
        AND fo.tenant_id = 'ie'
        #AND dr.tenant_id = 'ie'
        #AND cd.tenant_id = 'ie'

)

SELECT
	ROW_NUMBER() OVER (PARTITION BY dd_lvl1.del_key, dd_lvl1.record_rank ORDER BY dd_lvl1.payment_utc ASC)       AS payment_sequence,
	DENSE_RANK() OVER (PARTITION BY dd_lvl1.del_key ORDER BY dd_lvl1.payment_utc ASC)                            AS payment_sequence_dense_rank,
	CASE WHEN dd_lvl1.record_rank = 1 THEN TRUE ELSE FALSE END                                                   AS non_dup_record_flag,
	/* TIMESTAMP_DIFF(dd_lvl1.delivered_utc, dd_lvl1.first_del_utc, SECOND)                                      AS secs_to_2nd_del_complete, */
	/* TIMESTAMP_DIFF(dd_lvl1.next_payment_utc, dd_lvl1.payment_utc, MINUTE)                                     AS min_to_next_payment, */
	dd_lvl1.*

FROM
    deliveries_data_lvl1 AS dd_lvl1


#   WHERE late_order_contact >=1
