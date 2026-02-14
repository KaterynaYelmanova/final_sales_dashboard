WITH base_events AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    user_pseudo_id,
    event_name,

    -- session id
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS session_id,

    -- user_session_id (унікальна сесія)
    CONCAT(
      user_pseudo_id,
      CAST(
        (SELECT value.int_value
         FROM UNNEST(event_params)
         WHERE key = 'ga_session_id') AS STRING
      )
    ) AS user_session_id,

    -- landing page clean (без домену та параметрів)
    REGEXP_EXTRACT(
      (SELECT value.string_value
       FROM UNNEST(event_params)
       WHERE key = 'page_location'),
      r'https?://[^/]+(/[^?]*)'
    ) AS page_path,

    -- traffic source
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,

    -- device info
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS operating_system,

    -- geo info
    geo.country AS country,
    geo.region  AS region,
    geo.city    AS city,

    --кроки воронки
     IF(event_name = 'session_start', 1, 0) AS step_session_start,
     IF(event_name = 'view_item', 1, 0) AS step_view_item,
     IF(event_name = 'add_to_cart', 1, 0) AS step_add_to_cart,
     IF(event_name = 'begin_checkout', 1, 0) AS step_begin_checkout,
     IF(event_name = 'add_shipping_info', 1, 0) AS step_add_shipping_info,
     IF(event_name = 'add_payment_info', 1, 0) AS step_add_payment_info,
     IF(event_name = 'purchase', 1, 0) AS step_purchase
     
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

  -- беремо тільки події funnel (але можна взяти ВСІ)
  WHERE event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'
  )
)

SELECT *
FROM base_events;