# Purpose

This folder contains SQL transformation scripts that create staging and curated views from the raw events.csv dataset. The supplied SQL code creates the staging_events table and the following views:

vw_events_enriched — parses JSON event_data, converts timestamps, extracts UTM and channel, and derives device category.

vw_sessions — sessionization logic (30-minute inactivity gap) producing session_id, session start/end, events count, etc.

vw_transactions — extracts transactions (purchase/checkout_completed) and aggregates revenue per transaction.

vw_attribution — 7-day lookback first-click and last-click attribution for each transaction.

The actual SQL is provided in the uploaded file: /mnt/data/Part2 - Transformation Code.sql.

# Environment Requirements

Microsoft SQL Server v16 or newer

SQL Server Management Studio (SSMS) v20.2.1

# Input Data

Import events.csv into a table named staging_events (the SQL file also contains a CREATE TABLE statement for staging_events).

In SSMS: Tasks → Import Flat File and map to dbo.staging_events.

## Verify import:

SELECT TOP 50 * FROM dbo.staging_events;
Files

code/ — (if you split the provided SQL into separate files, place them here).

Single provided SQL file: /mnt/data/Part2 - Transformation Code.sql (contains table DDL and view definitions).

architecture-diagram.png — optional visual of pipeline flow.

How to Run (recommended order)

You can run the entire provided SQL file, or run the following logical steps in SSMS in order.

## Option A — Run the full SQL file

Open SSMS → File → Open → /mnt/data/Part2 - Transformation Code.sql (or copy/paste contents).

Make sure the target database is selected in the toolbar's database dropdown.

Execute the script. It will create staging_events (if not exists) and the views listed above.

## Option B — Run step-by-step (recommended for review)

Create staging table (the script includes the CREATE TABLE [dbo].[staging_events] DDL). Execute this first if you prefer explicit control.

Import events.csv into staging_events using the Import Flat File wizard.

Create / Replace vw_events_enriched — this view normalizes timestamps, extracts JSON fields (transaction_id, revenue, product_id), parses UTM params from page_url, derives channel and device_category.

Create / Replace vw_sessions — runs sessionization using a 30-minute inactivity window; creates session-level aggregates.

Create / Replace vw_transactions — filters purchase-related events and aggregates revenue by transaction_id.

Create / Replace vw_attribution — computes first/last click channels within a 7-day lookback window for transactions.

## Run each view creation statement and verify results after each step:

-- After creating vw_events_enriched
SELECT TOP 50 * FROM dbo.vw_events_enriched;


-- After creating vw_sessions
SELECT TOP 50 * FROM dbo.vw_sessions;


-- After creating vw_transactions
SELECT TOP 50 * FROM dbo.vw_transactions;


-- After creating vw_attribution
SELECT TOP 50 * FROM dbo.vw_attribution;
Important Implementation Notes (from the SQL code)

event_data is parsed as JSON using ISJSON(...) and JSON_VALUE(...). Revenue is cast to FLOAT with TRY_CAST.

UTM parameters are extracted using STRING_SPLIT() over the query-string portion of page_url — scripts assume page_url contains ? when query params exist.

channel derivation falls back to direct, organic_search, social, or referral based on utm_source or referrer content.

Sessionization truncates milliseconds (DATEADD(MILLISECOND, -DATEPART(MILLISECOND, event_ts), event_ts)) and uses a 30-minute gap to start new sessions.

vw_sessions uses APPROX_COUNT_DISTINCT(page_url) (SQL Server 2019+ compatibility; supported in SQL Server 2019 and later — SQL Server 2022/16 should support this function). If unavailable in your server, replace with COUNT(DISTINCT page_url).

Transaction detection checks event names containing checkout_completed, purchase, or order_complete (case-insensitive).

Attribution uses a 7-day lookback window prior to transaction_time (inclusive) to find first and last channels.

## Troubleshooting

If CONVERT(DATETIMEOFFSET, timestamp) fails, inspect timestamp formats. Consider using TRY_CONVERT or TRY_PARSE with an explicit format.

If STRING_SPLIT usage on an empty or NULL page_url causes issues, ensure the WHERE clauses guarding CHARINDEX('?', page_url) > 0 are present (script includes such guards).

If APPROX_COUNT_DISTINCT is not available, replace with COUNT(DISTINCT ...) for exact counts.

Testing & Validation

Compare the number of transactions between vw_transactions and a sample export from raw data to confirm aggregation.

Validate a sample transaction_id by querying all events for that client_id to ensure first/last click attribution is correct.

## Versioning and Deployment Recommendations

Keep the provided SQL as a source-controlled file (e.g., place it under part2-transformation/code/Part2-Transformation-Code.sql).

When promoting to production, convert views into materialized tables (scheduled jobs) if performance is a concern and refresh them via SQL Agent jobs.

Add a config.example or header comment in the SQL file for schema/table name overrides.
