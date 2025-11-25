# Revenue Completeness, Null-Revenue Detection & Quality Analysis 

using SQL Server
Microsoft SQL Server 2016+ | SSMS 20.2.1

## Purpose

## Part 3 focuses on analytical SQL views and stored procedures that evaluate the quality and completeness of transactional revenue data.

The analysis provides:

Revenue completeness KPIs

Identification of missing/zero/negative revenue transactions

Daily/rolling trends

Dimensional breakdown (channel, device, hour, day of week)

Client-level completeness scoring

Automated alerts & reports

This analysis is entirely SQL-based and runs on:

Microsoft SQL Server v16+

SQL Server Management Studio (SSMS) v20.2.1

The SQL used in this part is available in the uploaded file:
📄 /mnt/data/Revenue Transactions check code.sql 

## Revenue Transactions check code

## Prerequisites

Before running Part 3 scripts:

✔ Dataset Imported

You must have the event dataset loaded and transformed via Part 2:

vw_transactions

vw_events_enriched

vw_sessions

vw_attribution

✔ Required base views

The analysis depends entirely on vw_transactions, created in Part 2.

Verify it exists:

SELECT TOP 50 * FROM vw_transactions;

Files Included

Inside /part3-analysis/code/ include:

Revenue Transactions check code.sql (all views & stored procedures)

(Optional) Additional KPI queries or export scripts

Inside /part3-analysis/:

executive-summary.pdf — Insights from the SQL analysis

supporting-analysis/ — Optional screenshots or charts

## What the SQL Creates (Summary)

1. vw_dq_revenue_completeness

Overall completeness metrics:

% of transactions with revenue

Null / zero / negative revenue counts

Revenue statistics (avg/min/max/total)

PASS/WARNING/FAIL validation thresholds

2. vw_dq_null_revenue_transactions

Row-level identification of:

NULL_REVENUE

ZERO_REVENUE

NEGATIVE_REVENUE

Severity flags

Potential root causes

Age categorization

3. vw_dq_revenue_completeness_trend

Daily & rolling 7-day revenue completeness:

Total transactions per day

Daily % completeness

Threshold flag (below/acceptable)

4. vw_dq_revenue_completeness_by_dimension

## Breakdowns by:

Channel

Device

Hour of day

Day of week

5. vw_dq_revenue_completeness_by_client

Client-level completeness analysis:

Total transactions

Completeness %

Behavior flags

Days since last transaction

6. sp_dq_check_revenue_completeness_alerts

Real-time alert procedure with parameters:

Custom completeness threshold

Lookback window (hours)

Returns PASS/FAIL + details

7. sp_dq_revenue_completeness_report

Generates a comprehensive multi-section report inside SSMS output.

##  How to Run (Step-by-Step)

## 1. Open SQL File

In SSMS:

File → Open → Revenue Transactions check code.sql


Execute entire file or run section-by-section.

## 2. Create All Views

Execute the view creation sections:

CREATE OR ALTER VIEW vw_dq_revenue_completeness ...
CREATE OR ALTER VIEW vw_dq_null_revenue_transactions ...
CREATE OR ALTER VIEW vw_dq_revenue_completeness_trend ...
CREATE OR ALTER VIEW vw_dq_revenue_completeness_by_dimension ...
CREATE OR ALTER VIEW vw_dq_revenue_completeness_by_client ...


Verify example:

SELECT TOP 10 * FROM vw_dq_revenue_completeness;

## 3. Run Alert Procedure

Default threshold (95%):

EXEC sp_dq_check_revenue_completeness_alerts;


Custom threshold:

EXEC sp_dq_check_revenue_completeness_alerts 
  @threshold_pct = 98.0,
  @lookback_hours = 48;

## 4. Generate Full Quality Report
EXEC sp_dq_revenue_completeness_report @threshold_pct = 95.0;


## This prints:

Overall summary

Recent trend

Dimensions below threshold

Top affected clients

Sample problematic transactions

Validation & Testing Queries
Daily trend (last 30 days):
SELECT * FROM vw_dq_revenue_completeness_trend
WHERE transaction_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY transaction_date DESC;

Identify problematic channels/devices:
SELECT * 
FROM vw_dq_revenue_completeness_by_dimension
WHERE revenue_completeness_pct < 95
ORDER BY revenue_completeness_pct ASC;

Investigate NULL revenue cases:
SELECT TOP 20 *
FROM vw_dq_null_revenue_transactions
ORDER BY transaction_time DESC;

Find worst-performing clients:
SELECT *
FROM vw_dq_revenue_completeness_by_client
WHERE client_status IN ('BELOW_THRESHOLD','MAJORITY_NULL_REVENUE')
ORDER BY revenue_completeness_pct ASC;

Optional: SQL Agent Scheduling

The script includes a ready-to-run SQL Agent job:

Job Name: DQ_Hourly_Revenue_Completeness_Check

Frequency: runs hourly

Action: executes sp_dq_check_revenue_completeness_alerts

Optional: email alert integration

To install:

-- Run the Job creation section from the script
EXEC dbo.sp_add_job ...

## Troubleshooting
Issue	Cause	Fix
All revenue NULL	Parsing error in Part2 transformation	Re-run vw_transactions logic
No rows returned in trend view	vw_transactions missing timestamps	Validate transaction_time conversion
Views fail to create	Old version of SQL Server	Ensure SQL Server 2016+
Rolling average error	Missing window function	Enable correct compatibility level

## Check compatibility:

SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME();

