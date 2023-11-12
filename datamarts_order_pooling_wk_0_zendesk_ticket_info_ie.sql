WITH ticket_data AS (

    SELECT
        ds.tenant_id                      AS tenant_id,
        ds.sys_delivery_id                AS sys_delivery_id,
        1                                 AS has_ticket

    FROM
        `just-data-warehouse.international_reporting.datamarts_order_pooling_stg_1_delivery_staging_ie`   AS ds
        LEFT JOIN `just-data-warehouse.opensource_ecommerce.tickets`                 AS tt ON CAST(tt.zendesk.zendesk_order_id_local AS INT64) = ds.just_eat_order_number
        LEFT JOIN UNNEST(zendesk.chat_ids_list)                                      AS chat
        LEFT JOIN UNNEST(zendesk.call_ids_list)                                      AS calls

    WHERE
       ds.tenant_id ='ie'
       AND tt.zendesk.zendesk_order_id_local IS NOT NULL
       AND tt.zendesk.contact_origin = 'originator_customer'
       AND (tt.zendesk.zendesk_ticket_origin IN ('origin_offline','origin_self_serve_offline') OR calls.call_segment_id IS NOT NULL OR chat is not null)
)

SELECT
    *

FROM
    ticket_data