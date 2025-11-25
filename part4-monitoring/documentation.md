# Part 4 — Monitoring

Monitoring SQL Scripts for Data Pipeline Health Checks
Microsoft SQL Server 2016+ | SSMS 20.2.1


## Purpose

This section contains SQL-based monitoring logic designed to track the health of your data pipeline, validate data integrity, detect anomalies, and log pipeline performance over time.

The monitoring system works entirely inside Microsoft SQL Server and uses:

A staging table for raw events

Metrics table for storing each monitoring run

Alerts table for storing warnings/critical issues

Stored procedures that perform automated checks

(Optional) SQL Server Agent job to schedule monitoring daily

The SQL code used here comes from the uploaded file:
/mnt/data/Part 4 Monitoring Code.sql

## Environment Requirements

Microsoft SQL Server v16 or newer

SQL Server Management Studio (SSMS) v20.2.1

SQL Server Agent enabled (only required if using scheduled runs)

Database Mail configured (only required for email alerts)

Input Data Requirements

Monitoring depends on the same raw dataset used in earlier tasks:

events.csv

## Before running monitoring scripts:

Import events.csv using SSMS
Right-click database → Tasks → Import Flat File

Load into table:
monitoring.events_staging (created by the SQL script)

Validate import:

SELECT TOP 50 * FROM monitoring.events_staging;

## Files Included

Place these inside /part4-monitoring/code/:

## Part 4 Monitoring Code.sql — contains:

Creation of monitoring schema

events_staging table

pipeline_metrics table

pipeline_alerts table

Stored procedure: usp_load_events_staging

Stored procedure: usp_run_pipeline_monitoring

SQL Agent job creation script (optional)

## How to Run (Step-by-Step)

## 1. Create Schema and Tables

Run the first section of the SQL file to create:

monitoring.events_staging

monitoring.pipeline_metrics

monitoring.pipeline_alerts

Validation:

SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('monitoring');

## 2. Load Events Using BULK INSERT

The SQL script includes a loader stored procedure:
EXEC monitoring.usp_load_events_staging 
     @file_path = 'C:\\path\\to\\events.csv';

This procedure:

Truncates the existing staging table

Loads CSV using BULK INSERT (UTF-8)

Parses timestamps

Extracts JSON fields from event_data

##  Validation:

SELECT COUNT(*) FROM monitoring.events_staging;

## 3. Run the Monitoring Procedure

Execute the monitoring engine:

EXEC monitoring.usp_run_pipeline_monitoring;


This procedure:

✔ Checks event volume

Compares daily volume to historical averages.

✔ Detects bad timestamp formats

Logs percentage of invalid timestamps.

✔ Detects duplicate transaction IDs

Checks for potential data processing issues.

✔ Validates revenue extraction

Ensures JSON revenue values are valid.

✔ Logs monitoring results

Writes to:
monitoring.pipeline_metrics

✔ Issues alerts

Writes to:
monitoring.pipeline_alerts

## 4. Review Monitoring Output
Pipeline Metrics

Logged per run:

SELECT TOP 50 *
FROM monitoring.pipeline_metrics
ORDER BY run_time DESC;

Alerts (Warnings / Critical Issues)
SELECT TOP 50 *
FROM monitoring.pipeline_alerts
ORDER BY run_time DESC;

## Optional (Recommended): SQL Server Agent Job

The SQL script includes job creation logic that:

Loads CSV daily

Runs monitoring

Logs all outcomes

To enable it:

Run the SQL Agent job creation section.

Ensure SQL Server Agent is running:

In SSMS → Object Explorer

Right-click SQL Server Agent → Start

You can adjust:

Schedule time (default 2:30 AM)

Email/notifier rules

Log retention

Optional Email Alerts

If your SQL environment has Database Mail configured, you can enable:

Email notification for warning-level alerts

Email notification for critical alerts

Modify the appropriate section inside:
usp_run_pipeline_monitoring

Example:

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'default-mail-profile',
    @recipients = 'alerts@company.com',
    @subject = 'Pipeline Alert',
    @body = 'A data anomaly has been detected.';

## Troubleshooting
BULK INSERT fails

Ensure SQL Server service account has file access.

Save the CSV as UTF-8.

JSON_VALUE returns NULL

Check if event_data contains escaped quotes.

Run ISJSON(event_data) to verify.

Timestamps fail to convert

Use:

TRY_CONVERT(DATETIME2, event_timestamp)

Monitoring procedure not logging

Check permissions to monitoring.pipeline_metrics

Ensure transaction commits are enabled

Enhancement Recommendations

Add Power BI dashboard connected to pipeline_metrics

Add Slack/Teams alert integrations

Add anomaly detection using simple SQL z-score logic

Periodic purging of staging + alert tables
