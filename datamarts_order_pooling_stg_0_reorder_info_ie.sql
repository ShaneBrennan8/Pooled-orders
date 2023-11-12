WITH target_dates AS (

    SELECT
		CASE
		WHEN DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), WEEK) <= '2022-03-21'
		THEN DATE('2022-03-21')
		ELSE DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) END                              AS start_date, 
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)                                         AS end_date /* Yesterday */

),

#order level
delivery_staging AS (

    SELECT
        fd.tenant_id                                                                             AS tenant_id,
        fd.sys_delivery_id                                                                       AS sys_delivery_id,
        fo.order_date                                                                            AS order_date,
        food_subtotal,
        LEAD(fd.sys_delivery_id) OVER(PARTITION BY fd.tenant_id,
                                                   fo.sys_custo_id
                                          ORDER BY fo.order_datetime)                            AS first_reorder_sys_delivery_id,
        LEAD(fo.order_date)      OVER(PARTITION BY fd.tenant_id,
                                                   fo.sys_custo_id
                                          ORDER BY fo.order_datetime)                            AS first_reorder_date,
        LEAD(fo.food_subtotal)      OVER(PARTITION BY fd.tenant_id,
                                                   fo.sys_custo_id
                                          ORDER BY fo.order_datetime)                            AS first_reorder_food_subtotal,  
        LEAD(fd.sys_delivery_id,2) OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS second_reorder_sys_delivery_id,
        LEAD(fo.order_date,2)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS second_reorder_date,
        LEAD(fo.food_subtotal,2)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS second_reorder_food_subtotal,
        LEAD(fd.sys_delivery_id,3) OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS third_reorder_sys_delivery_id,
        LEAD(fo.order_date,3)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS third_reorder_date,
        LEAD(fo.food_subtotal,3)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS third_reorder_food_subtotal,
        LEAD(fd.sys_delivery_id,4) OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS fourth_reorder_sys_delivery_id,
        LEAD(fo.order_date,4)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS fourth_reorder_date,
        LEAD(fo.food_subtotal,4)      OVER(PARTITION BY fd.tenant_id,
                                                     fo.sys_custo_id
                                            ORDER BY fo.order_datetime)                          AS fourth_reorder_food_subtotal

    FROM
        `just-data-warehouse.delco_analytics_team_dwh.fact_deliveries`             AS fd
        LEFT JOIN `just-data-warehouse.delco_analytics_team_dwh.fact_orders_resto` AS fo  ON  fo.sys_order_id = fd.sys_order_id

    WHERE
            fo.order_date BETWEEN (SELECT start_date FROM target_dates) AND (SELECT end_date FROM target_dates)
        AND fd.delivery_status = 'DELIVERED'
        AND fd.tenant_id ='ie'
       
),
sixty_day_reorder_info AS (

    WITH reorders AS (

        SELECT
            sys_delivery_id                                         AS sys_delivery_id,
            first_reorder_sys_delivery_id                           AS first_reorder_sys_delivery_id,
            DATE_DIFF(first_reorder_date, order_date, DAY)          AS days_between_current_order_and_first_reorder,
            first_reorder_food_subtotal,  
            second_reorder_sys_delivery_id                          AS second_reorder_sys_delivery_id,
            DATE_DIFF(second_reorder_date, first_reorder_date, DAY) AS days_between_first_reorder_and_second_reorder,
            second_reorder_food_subtotal,  
            third_reorder_sys_delivery_id                           AS third_reorder_sys_delivery_id,
            DATE_DIFF(third_reorder_date, second_reorder_date, DAY) AS days_between_second_reorder_and_third_reorder,
            third_reorder_food_subtotal,  
            fourth_reorder_sys_delivery_id                          AS fourth_reorder_sys_delivery_id,
            DATE_DIFF(fourth_reorder_date, third_reorder_date, DAY) AS days_between_third_reorder_and_fourth_reorder,
            fourth_reorder_food_subtotal,  

        FROM
           delivery_staging
   )

   SELECT
       sys_delivery_id,
       first_reorder_sys_delivery_id,
       days_between_current_order_and_first_reorder,
       first_reorder_food_subtotal,
       second_reorder_sys_delivery_id,
       days_between_first_reorder_and_second_reorder,
       second_reorder_food_subtotal,
       third_reorder_sys_delivery_id,
       days_between_second_reorder_and_third_reorder,
       third_reorder_food_subtotal, 
       fourth_reorder_sys_delivery_id,
       days_between_third_reorder_and_fourth_reorder,
       fourth_reorder_food_subtotal, 

   FROM
       reorders

   WHERE
       days_between_current_order_and_first_reorder     <= 60
       AND (days_between_first_reorder_and_second_reorder <= 60
       OR   days_between_first_reorder_and_second_reorder IS NULL)
       AND (days_between_second_reorder_and_third_reorder <= 60
       OR   days_between_second_reorder_and_third_reorder IS NULL)
       AND (days_between_third_reorder_and_fourth_reorder <= 60
        OR  days_between_third_reorder_and_fourth_reorder IS NULL)
)

SELECT
    *

FROM
    sixty_day_reorder_info
