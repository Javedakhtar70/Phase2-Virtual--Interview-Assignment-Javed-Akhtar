
CREATE TABLE [dbo].[staging_events](
	[client_id] [nvarchar](255) NULL,
	[page_url] [nvarchar](max) NULL,
	[referrer] [nvarchar](max) NULL,
	[timestamp] [nvarchar](255) NULL,
	[event_name] [nvarchar](255) NULL,
	[event_data] [nvarchar](max) NULL,
	[user_agent] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

-- Use SQL Server Import Wizard to upload events data in staging table and store the process and  schedule it 

-- 1. Extracted fields view

CREATE VIEW vw_events_enriched AS
SELECT
  client_id,
  page_url,
  referrer,
  -- Convert timestamp to ISO 8601 format
  CONVERT(DATETIMEOFFSET, timestamp) AS event_ts,
  event_name,
  -- JSON extraction in SQL Server with NULL check and ISJSON validation
  CASE 
    WHEN event_data IS NOT NULL AND ISJSON(event_data) = 1 
    THEN JSON_VALUE(event_data, '$.transaction_id') 
    ELSE NULL 
  END AS transaction_id,
  CASE 
    WHEN event_data IS NOT NULL AND ISJSON(event_data) = 1 
    THEN TRY_CAST(JSON_VALUE(event_data, '$.revenue') AS FLOAT) 
    ELSE NULL 
  END AS revenue,
  CASE 
    WHEN event_data IS NOT NULL AND ISJSON(event_data) = 1 
    THEN JSON_VALUE(event_data, '$.product_id') 
    ELSE NULL 
  END AS product_id,
  user_agent,
  -- Extract utm_source parameter
  (SELECT TOP 1 value 
   FROM STRING_SPLIT(
     SUBSTRING(page_url, CHARINDEX('?', page_url) + 1, LEN(page_url)), '&'
   )
   WHERE value LIKE 'utm_source=%'
   AND CHARINDEX('?', page_url) > 0
  ) AS utm_source_param,
  -- Extract just the value from utm_source=value
  (SELECT TOP 1 
     SUBSTRING(value, CHARINDEX('=', value) + 1, LEN(value))
   FROM STRING_SPLIT(
     SUBSTRING(page_url, CHARINDEX('?', page_url) + 1, LEN(page_url)), '&'
   )
   WHERE value LIKE 'utm_source=%'
   AND CHARINDEX('?', page_url) > 0
  ) AS utm_source,
  -- Derived channel
  CASE
    WHEN EXISTS (
      SELECT 1 
      FROM STRING_SPLIT(
        SUBSTRING(page_url, CHARINDEX('?', page_url) + 1, LEN(page_url)), '&'
      )
      WHERE value LIKE 'utm_source=%'
      AND CHARINDEX('?', page_url) > 0
    ) THEN LOWER(
      (SELECT TOP 1 
         SUBSTRING(value, CHARINDEX('=', value) + 1, LEN(value))
       FROM STRING_SPLIT(
         SUBSTRING(page_url, CHARINDEX('?', page_url) + 1, LEN(page_url)), '&'
       )
       WHERE value LIKE 'utm_source=%'
       AND CHARINDEX('?', page_url) > 0
      )
    )
    WHEN referrer IS NULL OR referrer = '' THEN 'direct'
    WHEN referrer LIKE '%google%' 
      OR referrer LIKE '%bing%' 
      OR referrer LIKE '%yahoo%' 
      OR referrer LIKE '%duckduckgo%' THEN 'organic_search'
    WHEN referrer LIKE '%facebook%' 
      OR referrer LIKE '%instagram%' 
      OR referrer LIKE '%t.co%' 
      OR referrer LIKE '%twitter%' 
      OR referrer LIKE '%linkedin%' 
      OR referrer LIKE '%pinterest%' THEN 'social'
    ELSE 'referral'
  END AS channel,
  -- Device category
  CASE
    WHEN LOWER(user_agent) LIKE '%mobile%' 
      OR LOWER(user_agent) LIKE '%iphone%' 
      OR LOWER(user_agent) LIKE '%android%' THEN 'mobile'
    WHEN LOWER(user_agent) LIKE '%ipad%' 
      OR LOWER(user_agent) LIKE '%tablet%' THEN 'tablet'
    ELSE 'desktop'
  END AS device_category
FROM staging_events;

select * from vw_events_enriched


-- 2. Sessionized View

CREATE OR ALTER VIEW vw_sessions AS
WITH enriched AS (
  SELECT *,
    DATEADD(MILLISECOND, -DATEPART(MILLISECOND, event_ts), event_ts) AS event_ts_trunc
  FROM vw_events_enriched
),
flagged AS (
  SELECT
    *,
    LAG(event_ts_trunc) OVER (PARTITION BY client_id ORDER BY event_ts_trunc) AS prev_ts
  FROM enriched
),
gaps AS (
  SELECT
    *,
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN DATEDIFF(MINUTE, prev_ts, event_ts_trunc) > 30 THEN 1
      ELSE 0
    END AS new_session_flag
  FROM flagged
),
sequenced AS (
  SELECT
    *,
    SUM(new_session_flag) OVER (
      PARTITION BY client_id 
      ORDER BY event_ts_trunc 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_seq
  FROM gaps
)
SELECT
  -- Use session start date instead of event date for consistent session_id
  CONCAT(
    client_id, 
    '_', 
    CAST(session_seq AS VARCHAR(50))
  ) AS session_id,
  client_id,
  session_seq,
  MIN(event_ts) AS session_start,
  MAX(event_ts) AS session_end,
  COUNT(1) AS events_count,
  APPROX_COUNT_DISTINCT(page_url) AS unique_pages,
  MAX(channel) AS channel,
  MAX(device_category) AS device,
  DATEDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS session_duration_seconds
FROM sequenced
GROUP BY client_id, session_seq;

select * from vw_sessions


--3. Transactions View

CREATE VIEW vw_transactions AS
SELECT
  transaction_id,
  client_id,
  MIN(event_ts) AS transaction_time,
  SUM(revenue) AS revenue,  -- if multiple lines per tx
  MAX(channel) AS channel,
  MAX(device_category) AS device
FROM vw_events_enriched
WHERE LOWER(event_name) LIKE '%checkout_completed%'
   OR LOWER(event_name) LIKE '%purchase%'
   OR LOWER(event_name) LIKE '%order_complete%'
GROUP BY transaction_id, client_id;


select * from vw_transactions


-- 4 Attribution view (7-day lookback)

CREATE VIEW vw_attribution AS
WITH tx AS (
  SELECT * FROM vw_transactions
),
user_events AS (
  SELECT client_id, event_ts, channel
  FROM vw_events_enriched
)
SELECT
  t.transaction_id,
  t.client_id,
  t.transaction_time,
  t.revenue,
  -- first click in the prior 7 days (including transaction_time)
  (SELECT TOP 1 ue.channel
   FROM user_events ue
   WHERE ue.client_id = t.client_id
     AND ue.event_ts BETWEEN DATEADD(DAY, -7, t.transaction_time) AND t.transaction_time
   ORDER BY ue.event_ts ASC) AS first_click_channel,
  -- last click in the prior 7 days
  (SELECT TOP 1 ue.channel
   FROM user_events ue
   WHERE ue.client_id = t.client_id
     AND ue.event_ts BETWEEN DATEADD(DAY, -7, t.transaction_time) AND t.transaction_time
   ORDER BY ue.event_ts DESC) AS last_click_channel
FROM tx t;

select * from vw_attribution