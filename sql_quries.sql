-- 1. Создание представления для заказов из crm, которое буду использоваться для сопоставления с данными из ga4
CREATE OR REPLACE VIEW `ga4-vitberry.raw_keycrm.view_ab_rozetka_orders` AS
SELECT
  -- Идентификаторы. id в crm и id заказа на сайте (для связи с ga4)
  JSON_VALUE(raw_content, '$.id') AS crm_order_id,
  JSON_VALUE(raw_content, '$.source_uuid') AS site_order_id,
  
  -- Статус и время. конвертирую время создания из utc по Украине, так как АПИ СРМ отдает время в utc.
  JSON_VALUE(raw_content, '$.status.name') AS order_status,
  DATETIME(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(raw_content, '$.created_at')), 'Europe/Kyiv') AS created_at,
  
  -- Финансы. привожу строки из json к числам (float64)
  SAFE_CAST(JSON_VALUE(raw_content, '$.grand_total') AS FLOAT64) AS revenue,
  SAFE_CAST(JSON_VALUE(raw_content, '$.margin_sum') AS FLOAT64) AS profit_margin,
  SAFE_CAST(JSON_VALUE(raw_content, '$.expenses_sum') AS FLOAT64) AS shipping_costs,
 
  -- Доставка и оплата. Достаю название службы доставки и метод оплаты.
  JSON_VALUE(raw_content, '$.payments[0].fiscal_result.payment_name') AS payment_name,
  JSON_VALUE(raw_content, '$.shipping.delivery_service.source_name') AS delivery_service,

  -- Данные покупателя
  SAFE_CAST(JSON_VALUE(raw_content, '$.buyer.orders_count') AS INT64) AS buyer_lifetime_orders,
  JSON_VALUE(raw_content, '$.buyer.full_name') AS customer_name,
  JSON_VALUE(raw_content, '$.buyer.phone') AS phone,
  
  -- Системное поле. Время последнего обновления строки данных в BigQuery (может отличаться от created_at, так как данные данные обновляются каждый день лбновляя информацию за последние 21 день, что бы учесть обновление всех статусов заказа)
  updated_at,

  -- Расчетная метрика. Доля логистики в чеке (безопасное деление)
  ROUND(
    SAFE_DIVIDE(
      SAFE_CAST(JSON_VALUE(raw_content, '$.expenses_sum') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(raw_content, '$.grand_total') AS FLOAT64)
    ) * 100, 2
  ) AS logistics_ratio
FROM 
  `ga4-vitberry.raw_keycrm.orders`
WHERE 
  -- Фильтр по дате создание заказа в Киевском часовом поясе
  DATE(DATETIME(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(raw_content, '$.created_at')), 'Europe/Kyiv')) BETWEEN '2026-04-01' AND '2026-04-07'

-- Дедупликация
-- row_number нумерует строки для каждого site_order_id, сортируя их по дате обновления.
-- qualify оставляет только строки под номером 1 (самые новіе).
QUALIFY ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(raw_content, '$.source_uuid') ORDER BY updated_at DESC) = 1


-- 2. запрос для получения данных по транзакциям из ga4 и их сопоставления с данными из crm для теста Rozetka
WITH test_users AS (
  -- Получаю данные по транзакциям из ga4 за период теста
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group,
    ecommerce.transaction_id,
    device.category,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
    geo.city,
    CONCAT(IFNULL(session_traffic_source_last_click.manual_campaign.source, 'direct'), '/', IFNULL(session_traffic_source_last_click.manual_campaign.medium, 'none')) AS source_medium 
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'purchase'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
)
-- Объеденияю данные из ga4 с данными из crm по id заказа (ga4 transaction_id = crm site_order_id) для анализа результатов теста Rozetka
select c.*, t.*
from test_users t
inner join `ga4-vitberry.raw_keycrm.view_ab_rozetka_orders` c on t.transaction_id = c.site_order_id

-- 2.1 Доработанный запрос для получения данных по транзакциям из ga4 и их сопоставления с данными из crm для теста Rozetka, 
-- с добавлением информации о первом взаимодействии и вермени покупки пользователя в рамках теста
-- Список всех уникальных участников теста и их групп
WITH test_users AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
),

-- Время самого первого взаиможействия только для участников теста
user_first_sessions AS (
  SELECT 
    user_pseudo_id,
    MIN(event_timestamp) as session_start_raw
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'session_start'
    AND user_pseudo_id IN (SELECT user_pseudo_id FROM test_users) -- Фильтр по тестовім юзерам
  GROUP BY user_pseudo_id
),

-- Данные по покупкам только для участников теста
user_purchases AS (
  SELECT DISTINCT
    user_pseudo_id,
    ecommerce.transaction_id,
    device.category,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
    geo.city,
    CONCAT(IFNULL(session_traffic_source_last_click.manual_campaign.source, 'direct'), '/', IFNULL(session_traffic_source_last_click.manual_campaign.medium, 'none')) AS source_medium,
    event_timestamp as purchase_time_raw
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'purchase'
    AND user_pseudo_id IN (SELECT user_pseudo_id FROM test_users) -- Фильтр по тестовім юзерам
)

-- Склеиваю всё в одну таблицу
SELECT 
  u.user_group,
  TIMESTAMP_MICROS(s.session_start_raw) as session_start,
  TIMESTAMP_MICROS(p.purchase_time_raw) as purchase_time,
  p.* EXCEPT(user_pseudo_id, purchase_time_raw), -- за исключением ID и времени, так как уже есть в селекте
  c.* EXCEPT(site_order_id), -- исключаю так как есть значение transaction_id из таблицы покупок
  u.user_pseudo_id
FROM test_users u
LEFT JOIN user_first_sessions s ON u.user_pseudo_id = s.user_pseudo_id
LEFT JOIN user_purchases p ON u.user_pseudo_id = p.user_pseudo_id
LEFT JOIN `ga4-vitberry.raw_keycrm.view_ab_rozetka_orders` c ON p.transaction_id = c.site_order_id




-- 3. Запрос для анализа ключевых метрик по группам в рамках теста Rozetka, используя данные из ga4 и crm
WITH test_users AS (
  -- Берем данные по транзакциям из ga4 за период теста
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group,
    ecommerce.transaction_id
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'purchase'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
)

SELECT
  t.user_group,
  v.delivery_service,
  COUNT(DISTINCT t.user_pseudo_id) AS users,
  COUNT(DISTINCT t.transaction_id) AS orders,
  -- Беру данные из crm View
  ROUND(SUM(v.revenue), 2) AS real_revenue,
  ROUND(SUM(v.profit_margin), 2) AS gross_margin,
  ROUND(SUM(v.shipping_costs), 2) AS total_shipping_costs,
  -- Чистая прибыль (Маржа минус расходы на доставку и другие расходы)
  ROUND(SUM(v.profit_margin - v.shipping_costs), 2) AS net_profit,
  -- Средняя маржа на юзера (ARPU по прибыли)
  ROUND(SAFE_DIVIDE(SUM(v.profit_margin), COUNT(DISTINCT t.user_pseudo_id)), 2) AS profit_per_user
FROM test_users t
-- Соединяю по id заказа (ga4 transaction_id = crm site_order_id)
INNER JOIN `ga4-vitberry.raw_keycrm.view_ab_rozetka_orders` v 
  ON t.transaction_id = v.site_order_id
GROUP BY 1, 2
ORDER BY 1, 6 DESC

-- 4. Дополнительный запрос к ga4 по свойствам пользователя, которые добавдяд при срабатывании тега в GTM, для анализа распределения заказов по ценовым диапазонам в рамках теста
WITH sales AS (
  SELECT
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group,
    ecommerce.purchase_revenue AS revenue
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'purchase'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
)

SELECT
  user_group,
  -- Создаю ценовые диапазоны
  CASE 
    WHEN revenue < 500 THEN '< 500 грн'
    WHEN revenue BETWEEN 500 AND 800 THEN '500-800 грн'
    WHEN revenue BETWEEN 800 AND 1200 THEN '800-1200 грн'
    WHEN revenue BETWEEN 1200 AND 2000 THEN '1200-2000 грн'
    ELSE '> 2000 грн'
  END AS price_bucket,
  COUNT(*) AS total_orders
FROM sales
GROUP BY 1, 2
ORDER BY 2, 1

-- 5. Запрос для анализа поведения пользователей: просмотры товара, добавления в корзину, заказы и доход по группам
WITH user_list AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
),

user_behavior AS (
  SELECT
    user_pseudo_id,
    COUNTIF(event_name = 'view_item') AS user_views,
    COUNTIF(event_name = 'add_to_cart') AS user_carts,
-- Считаю транзакции только если это событие покупки
    COUNT(DISTINCT IF(event_name = 'purchase', ecommerce.transaction_id, NULL)) AS user_orders,
    -- Суммируем доход только из событий покупки
    SUM(IF(event_name = 'purchase', ecommerce.purchase_revenue, 0)) AS user_revenue,
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
  GROUP BY 1
)

SELECT
  u.user_group,
  b.*
FROM user_list u
LEFT JOIN user_behavior b ON u.user_pseudo_id = b.user_pseudo_id


-- 6. Расширенный запрос с агригацией и построением воронки. Просмотры страниці товара, добавления в корзину, заказы, доход и конверсии по группам
WITH user_list AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
),
user_behavior AS (
  SELECT
    user_pseudo_id,
    COUNTIF(event_name = 'view_item') AS user_views,
    COUNTIF(event_name = 'add_to_cart') AS user_carts,
    COUNT(DISTINCT IF(event_name = 'purchase', ecommerce.transaction_id, NULL)) AS user_orders,
    SUM(IF(event_name = 'purchase', ecommerce.purchase_revenue, 0)) AS user_revenue,
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
  GROUP BY 1
)

-- агрегирую данные по группам
  u.user_group,
  COUNT(DISTINCT u.user_pseudo_id) AS total_users,
  -- показатели всех пользователей внутри группы
  SUM(b.user_views) AS total_views,
  SUM(b.user_carts) AS total_carts,
  SUM(b.user_orders) AS total_orders,
  ROUND(SUM(b.user_revenue), 2) AS total_revenue,
  -- Расчет конверсий и средних чеков
  ROUND(SAFE_DIVIDE(SUM(b.user_carts), SUM(b.user_views)) * 100, 2) AS view_to_cart_cr,
  ROUND(SAFE_DIVIDE(SUM(b.user_orders), SUM(b.user_carts)) * 100, 2) AS cart_to_purchase_cr,
  ROUND(SAFE_DIVIDE(SUM(b.user_orders), COUNT(DISTINCT u.user_pseudo_id)) * 100, 2) AS cr,
  ROUND(SAFE_DIVIDE(SUM(b.user_revenue), SUM(b.user_orders)), 2) AS aov,
  ROUND(SAFE_DIVIDE(SUM(b.user_revenue), COUNT(DISTINCT u.user_pseudo_id)), 2) AS arpu
FROM user_list u
LEFT JOIN user_behavior b ON u.user_pseudo_id = b.user_pseudo_id
GROUP BY 1;


-- 7. Запрос для анализа конверсии, среднего чека и ARPU по группам
WITH user_list AS (
  -- Беру только список юзер и группа (уникальные пары)
  SELECT DISTINCT user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') AS user_group
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'ab_test_rozetka_variant') IS NOT NULL
),
sales_data AS (
  -- Достаю только покупки
  SELECT
    user_pseudo_id,
    ecommerce.transaction_id,
    ecommerce.purchase_revenue as revenue
  FROM `ga4-vitberry.analytics_450794542.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260401' AND '20260407'
    AND event_name = 'purchase'
)

SELECT
  u.user_group,
  round(cast(COUNT(DISTINCT s.transaction_id) as float64)/COUNT(DISTINCT u.user_pseudo_id)*100, 2) as cr,
  round(cast(ROUND(SUM(s.revenue), 2) as float64)/COUNT(DISTINCT u.user_pseudo_id), 2) as arpu,
  COUNT(DISTINCT u.user_pseudo_id) AS total_users,
  COUNT(DISTINCT s.transaction_id) AS total_orders,
  ROUND(SUM(s.revenue), 2) AS total_revenue,
  ROUND(SUM(s.revenue) / COUNT(DISTINCT s.transaction_id), 2) AS aov
FROM user_list u
LEFT JOIN sales_data s ON u.user_pseudo_id = s.user_pseudo_id
GROUP BY 1


-- 8. Запрос для получения данных по заказам из crm за период теста Rozetka, который будет использоваться для сопоставления с данными из ga4
SELECT
  -- Идентификаторы
  JSON_VALUE(raw_content, '$.id') AS crm_order_id,
  JSON_VALUE(raw_content, '$.source_uuid') AS site_order_id,
  -- Статус и время
  JSON_VALUE(raw_content, '$.status.name') AS order_status,
  -- Врмея
  DATETIME(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(raw_content, '$.created_at')), 'Europe/Kyiv') AS created_at,
  -- Финансы
  SAFE_CAST(JSON_VALUE(raw_content, '$.grand_total') AS FLOAT64) AS revenue,
  SAFE_CAST(JSON_VALUE(raw_content, '$.margin_sum') AS FLOAT64) AS profit_margin,
  SAFE_CAST(JSON_VALUE(raw_content, '$.expenses_sum') AS FLOAT64) AS shipping_costs,
  -- Доставка
  JSON_VALUE(raw_content, '$.shipping.delivery_service.source_name') AS delivery_servise,
  -- Покупатель
  JSON_VALUE(raw_content, '$.buyer.full_name') AS customer_name,
  JSON_VALUE(raw_content, '$.buyer.phone') AS phone,
  updated_at
FROM 
  `ga4-vitberry.raw_keycrm.orders`
WHERE 
DATE(DATETIME(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(raw_content, '$.created_at')), 'Europe/Kyiv')) between '2026-04-01' and '2026-04-07'
--  (DATE(updated_at) between '2026-04-01' and '2026-04-07') AND (DATE(DATETIME(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(raw_content, '$.created_at')), 'Europe/Kyiv')) between '2026-04-01' and '2026-04-07')
order by created_at asc;