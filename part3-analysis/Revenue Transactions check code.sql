-- ============================================================================
-- NULL REVENUE TRANSACTION MONITORING AND ALERTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. REVENUE COMPLETENESS QUALITY CHECK VIEW
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_dq_revenue_completeness AS
SELECT
  CAST(GETDATE() AS DATE) AS check_date,
  
  -- Transaction counts
  COUNT(*) AS total_transactions,
  SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
  SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
  
  -- Revenue completeness percentage
  CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  ) AS revenue_completeness_pct,
  
  -- Additional revenue quality metrics
  SUM(CASE WHEN revenue = 0 THEN 1 ELSE 0 END) AS transactions_with_zero_revenue,
  SUM(CASE WHEN revenue < 0 THEN 1 ELSE 0 END) AS transactions_with_negative_revenue,
  SUM(CASE WHEN revenue > 0 THEN 1 ELSE 0 END) AS transactions_with_positive_revenue,
  
  -- Revenue statistics (for non-null values)
  AVG(CASE WHEN revenue IS NOT NULL THEN revenue END) AS avg_revenue,
  MIN(CASE WHEN revenue IS NOT NULL THEN revenue END) AS min_revenue,
  MAX(CASE WHEN revenue IS NOT NULL THEN revenue END) AS max_revenue,
  SUM(CASE WHEN revenue IS NOT NULL THEN revenue ELSE 0 END) AS total_revenue,
  
  -- Threshold-based validation (95% threshold)
  CASE 
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) >= 95.0 THEN 'PASS'
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) >= 90.0 THEN 'WARNING'
    ELSE 'FAIL'
  END AS validation_status_95pct,
  
  -- Alternative thresholds
  CASE 
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) >= 98.0 THEN 'PASS'
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) >= 95.0 THEN 'WARNING'
    ELSE 'FAIL'
  END AS validation_status_98pct,
  
  -- Gap from threshold
  95.0 - CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  ) AS gap_from_95pct_threshold

FROM vw_transactions;

-- ----------------------------------------------------------------------------
-- 2. DETAILED NULL REVENUE TRANSACTIONS
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_dq_null_revenue_transactions AS
SELECT
  transaction_id,
  client_id,
  transaction_time,
  revenue,
  channel,
  device,
  
  -- Flag the issue type
  CASE
    WHEN revenue IS NULL THEN 'NULL_REVENUE'
    WHEN revenue = 0 THEN 'ZERO_REVENUE'
    WHEN revenue < 0 THEN 'NEGATIVE_REVENUE'
    ELSE 'VALID_REVENUE'
  END AS revenue_issue_type,
  
  -- Issue severity
  CASE
    WHEN revenue IS NULL THEN 'HIGH'
    WHEN revenue < 0 THEN 'CRITICAL'
    WHEN revenue = 0 THEN 'MEDIUM'
    ELSE 'NONE'
  END AS issue_severity,
  
  -- Potential root cause
  CASE
    WHEN revenue IS NULL AND transaction_id IS NULL 
      THEN 'Missing transaction_id - data extraction issue'
    WHEN revenue IS NULL AND transaction_id IS NOT NULL 
      THEN 'Transaction captured but revenue not recorded'
    WHEN revenue = 0 
      THEN 'Zero value transaction - possible test or refund'
    WHEN revenue < 0 
      THEN 'Negative revenue - likely refund or data error'
    ELSE 'N/A'
  END AS potential_root_cause,
  
  -- Days since transaction
  DATEDIFF(DAY, transaction_time, GETDATE()) AS days_since_transaction,
  
  -- Age category
  CASE
    WHEN DATEDIFF(HOUR, transaction_time, GETDATE()) <= 24 THEN 'Last 24 hours'
    WHEN DATEDIFF(DAY, transaction_time, GETDATE()) <= 7 THEN 'Last 7 days'
    WHEN DATEDIFF(DAY, transaction_time, GETDATE()) <= 30 THEN 'Last 30 days'
    ELSE 'Older than 30 days'
  END AS transaction_age_category

FROM vw_transactions
WHERE 
  revenue IS NULL 
  OR revenue = 0 
  OR revenue < 0;

-- ----------------------------------------------------------------------------
-- 3. TIME-BASED REVENUE COMPLETENESS TREND
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_dq_revenue_completeness_trend AS
SELECT
  CAST(transaction_time AS DATE) AS transaction_date,
  COUNT(*) AS total_transactions,
  SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
  SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
  
  -- Daily completeness percentage
  CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  ) AS revenue_completeness_pct,
  
  -- Rolling 7-day average completeness
  AVG(CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  )) OVER (
    ORDER BY CAST(transaction_time AS DATE)
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7day_completeness_pct,
  
  -- Total revenue for the day
  SUM(CASE WHEN revenue IS NOT NULL THEN revenue ELSE 0 END) AS total_revenue,
  
  -- Flag days below threshold
  CASE 
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) < 95.0 THEN 'BELOW_THRESHOLD'
    ELSE 'ACCEPTABLE'
  END AS daily_status

FROM vw_transactions
WHERE transaction_time >= DATEADD(DAY, -90, GETDATE())
GROUP BY CAST(transaction_time AS DATE);

-- ----------------------------------------------------------------------------
-- 4. CHANNEL AND DEVICE BREAKDOWN
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_dq_revenue_completeness_by_dimension AS
SELECT
  dimension_type,
  dimension_value,
  total_transactions,
  transactions_with_revenue,
  transactions_with_null_revenue,
  revenue_completeness_pct,
  
  -- Flag dimensions with issues
  CASE 
    WHEN revenue_completeness_pct < 95.0 THEN 'BELOW_THRESHOLD'
    WHEN revenue_completeness_pct < 98.0 THEN 'WARNING'
    ELSE 'ACCEPTABLE'
  END AS dimension_status
  
FROM (
  -- By Channel
  SELECT
    'CHANNEL' AS dimension_type,
    ISNULL(channel, 'Unknown') AS dimension_value,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
    CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) AS revenue_completeness_pct
  FROM vw_transactions
  GROUP BY channel
  
  UNION ALL
  
  -- By Device
  SELECT
    'DEVICE' AS dimension_type,
    ISNULL(device, 'Unknown') AS dimension_value,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
    CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) AS revenue_completeness_pct
  FROM vw_transactions
  GROUP BY device
  
  UNION ALL
  
  -- By Hour of Day
  SELECT
    'HOUR_OF_DAY' AS dimension_type,
    CAST(DATEPART(HOUR, transaction_time) AS VARCHAR(10)) AS dimension_value,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
    CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) AS revenue_completeness_pct
  FROM vw_transactions
  GROUP BY DATEPART(HOUR, transaction_time)
  
  UNION ALL
  
  -- By Day of Week
  SELECT
    'DAY_OF_WEEK' AS dimension_type,
    DATENAME(WEEKDAY, transaction_time) AS dimension_value,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
    CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) AS revenue_completeness_pct
  FROM vw_transactions
  GROUP BY DATENAME(WEEKDAY, transaction_time), DATEPART(WEEKDAY, transaction_time)
) dimensions;

-- ----------------------------------------------------------------------------
-- 5. CLIENT-LEVEL ANALYSIS (Identify problematic clients)
-- ----------------------------------------------------------------------------

CREATE OR ALTER VIEW vw_dq_revenue_completeness_by_client AS
SELECT
  client_id,
  COUNT(*) AS total_transactions,
  SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS transactions_with_revenue,
  SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS transactions_with_null_revenue,
  
  -- Completeness percentage
  CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  ) AS revenue_completeness_pct,
  
  -- Revenue statistics
  SUM(CASE WHEN revenue IS NOT NULL THEN revenue ELSE 0 END) AS total_revenue,
  AVG(CASE WHEN revenue IS NOT NULL THEN revenue END) AS avg_revenue,
  
  -- Behavioral flag
  CASE
    WHEN SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) = COUNT(*) 
      THEN 'ALL_TRANSACTIONS_NULL_REVENUE'
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) < 50.0 THEN 'MAJORITY_NULL_REVENUE'
    WHEN CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    ) < 95.0 THEN 'BELOW_THRESHOLD'
    ELSE 'ACCEPTABLE'
  END AS client_status,
  
  -- Last transaction info
  MAX(transaction_time) AS last_transaction_time,
  DATEDIFF(DAY, MAX(transaction_time), GETDATE()) AS days_since_last_transaction

FROM vw_transactions
GROUP BY client_id
HAVING 
  -- Only show clients with issues
  SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) > 0
  OR CAST(
    100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
    NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
  ) < 95.0;

-- ----------------------------------------------------------------------------
-- 6. AUTOMATED ALERT STORED PROCEDURE WITH CONFIGURABLE THRESHOLD
-- ----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE sp_dq_check_revenue_completeness_alerts
  @threshold_pct DECIMAL(5,2) = 95.0,
  @lookback_hours INT = 24
AS
BEGIN
  SET NOCOUNT ON;
  
  DECLARE @total_transactions INT;
  DECLARE @transactions_with_revenue INT;
  DECLARE @completeness_pct DECIMAL(5,2);
  DECLARE @null_revenue_count INT;
  
  -- Calculate metrics for recent transactions
  SELECT 
    @total_transactions = COUNT(*),
    @transactions_with_revenue = SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END),
    @null_revenue_count = SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END),
    @completeness_pct = CAST(
      100.0 * SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) / 
      NULLIF(COUNT(*), 0) AS DECIMAL(5,2)
    )
  FROM vw_transactions
  WHERE transaction_time >= DATEADD(HOUR, -@lookback_hours, GETDATE());
  
  -- Return alert if below threshold
  IF @completeness_pct < @threshold_pct
  BEGIN
    SELECT 
      GETDATE() AS alert_timestamp,
      'CRITICAL' AS alert_severity,
      'NULL_REVENUE_THRESHOLD_BREACH' AS alert_type,
      CONCAT(
        'Revenue completeness is ', 
        @completeness_pct, 
        '% (threshold: ', 
        @threshold_pct, 
        '%)'
      ) AS alert_message,
      @total_transactions AS total_transactions,
      @transactions_with_revenue AS transactions_with_revenue,
      @null_revenue_count AS null_revenue_transactions,
      @completeness_pct AS current_completeness_pct,
      @threshold_pct AS threshold_pct,
      @threshold_pct - @completeness_pct AS gap_from_threshold,
      @lookback_hours AS lookback_hours,
      'Investigate revenue tracking implementation and data pipeline' AS recommended_action;
  END
  ELSE
  BEGIN
    SELECT 
      GETDATE() AS alert_timestamp,
      'INFO' AS alert_severity,
      'NULL_REVENUE_CHECK_PASSED' AS alert_type,
      CONCAT(
        'Revenue completeness is ', 
        @completeness_pct, 
        '% - Above threshold of ', 
        @threshold_pct, 
        '%'
      ) AS alert_message,
      @total_transactions AS total_transactions,
      @transactions_with_revenue AS transactions_with_revenue,
      @null_revenue_count AS null_revenue_transactions,
      @completeness_pct AS current_completeness_pct,
      @threshold_pct AS threshold_pct,
      NULL AS gap_from_threshold,
      @lookback_hours AS lookback_hours,
      'No action required' AS recommended_action;
  END
END;
GO

-- ----------------------------------------------------------------------------
-- 7. COMPREHENSIVE REVENUE QUALITY REPORT
-- ----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE sp_dq_revenue_completeness_report
  @threshold_pct DECIMAL(5,2) = 95.0
AS
BEGIN
  SET NOCOUNT ON;
  
  -- Overall Summary
  PRINT '============================================';
  PRINT 'REVENUE COMPLETENESS QUALITY REPORT';
  PRINT '============================================';
  PRINT '';
  
  SELECT 
    'OVERALL SUMMARY' AS section,
    total_transactions,
    transactions_with_revenue,
    transactions_with_null_revenue,
    revenue_completeness_pct,
    validation_status_95pct,
    gap_from_95pct_threshold,
    total_revenue
  FROM vw_dq_revenue_completeness;
  
  -- Recent Trend (Last 7 Days)
  PRINT '';
  PRINT 'RECENT TREND (Last 7 Days)';
  PRINT '-------------------------------------------';
  
  SELECT 
    transaction_date,
    total_transactions,
    transactions_with_revenue,
    revenue_completeness_pct,
    daily_status,
    total_revenue
  FROM vw_dq_revenue_completeness_trend
  WHERE transaction_date >= DATEADD(DAY, -7, GETDATE())
  ORDER BY transaction_date DESC;
  
  -- Dimensions Below Threshold
  PRINT '';
  PRINT 'DIMENSIONS BELOW THRESHOLD';
  PRINT '-------------------------------------------';
  
  SELECT 
    dimension_type,
    dimension_value,
    total_transactions,
    transactions_with_null_revenue,
    revenue_completeness_pct,
    dimension_status
  FROM vw_dq_revenue_completeness_by_dimension
  WHERE revenue_completeness_pct < @threshold_pct
  ORDER BY revenue_completeness_pct ASC;
  
  -- Top Clients with Issues
  PRINT '';
  PRINT 'TOP 20 CLIENTS WITH REVENUE ISSUES';
  PRINT '-------------------------------------------';
  
  SELECT TOP 20
    client_id,
    total_transactions,
    transactions_with_null_revenue,
    revenue_completeness_pct,
    client_status,
    last_transaction_time
  FROM vw_dq_revenue_completeness_by_client
  ORDER BY transactions_with_null_revenue DESC;
  
  -- Sample Null Revenue Transactions
  PRINT '';
  PRINT 'SAMPLE NULL REVENUE TRANSACTIONS (Last 10)';
  PRINT '-------------------------------------------';
  
  SELECT TOP 10
    transaction_id,
    client_id,
    transaction_time,
    channel,
    device,
    revenue_issue_type,
    potential_root_cause
  FROM vw_dq_null_revenue_transactions
  ORDER BY transaction_time DESC;
END;
GO

-- ----------------------------------------------------------------------------
-- 8. USAGE EXAMPLES AND SCHEDULED JOB
-- ----------------------------------------------------------------------------


-- ==================== USAGE EXAMPLES ====================

-- 1. Check current revenue completeness
SELECT * FROM vw_dq_revenue_completeness;

-- 2. Run alert check with default 95% threshold
EXEC sp_dq_check_revenue_completeness_alerts;

-- 3. Run alert check with custom threshold (98%) and 48-hour lookback
EXEC sp_dq_check_revenue_completeness_alerts 
  @threshold_pct = 98.0, 
  @lookback_hours = 48;

-- 4. Generate comprehensive report
EXEC sp_dq_revenue_completeness_report @threshold_pct = 95.0;

-- 5. View recent trend
SELECT * FROM vw_dq_revenue_completeness_trend
WHERE transaction_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY transaction_date DESC;

-- 6. Find problematic dimensions
SELECT * FROM vw_dq_revenue_completeness_by_dimension
WHERE revenue_completeness_pct < 95.0
ORDER BY revenue_completeness_pct ASC;

-- 7. Investigate specific null revenue transactions
SELECT * FROM vw_dq_null_revenue_transactions
WHERE transaction_age_category = 'Last 24 hours'
ORDER BY transaction_time DESC;

-- 8. Find clients with consistent revenue issues
SELECT * FROM vw_dq_revenue_completeness_by_client
WHERE client_status IN ('ALL_TRANSACTIONS_NULL_REVENUE', 'MAJORITY_NULL_REVENUE')
ORDER BY transactions_with_null_revenue DESC;


-- ==================== SCHEDULE SQL AGENT JOB ====================

USE msdb;
GO

-- Create job for hourly revenue completeness checks
EXEC dbo.sp_add_job
    @job_name = N'DQ_Hourly_Revenue_Completeness_Check',
    @enabled = 1,
    @description = N'Checks revenue completeness against 95% threshold every hour';

EXEC sp_add_jobstep
    @job_name = N'DQ_Hourly_Revenue_Completeness_Check',
    @step_name = N'Check_Revenue_Completeness',
    @subsystem = N'TSQL',
    @command = N'EXEC sp_dq_check_revenue_completeness_alerts @threshold_pct = 95.0, @lookback_hours = 1;',
    @database_name = N'YourDatabaseName', -- Update this
    @on_success_action = 1; -- Quit with success

-- Schedule to run every hour
EXEC sp_add_schedule
    @schedule_name = N'Hourly',
    @freq_type = 4, -- Daily
    @freq_interval = 1,
    @freq_subday_type = 8, -- Hours
    @freq_subday_interval = 1;

EXEC sp_attach_schedule
    @job_name = N'DQ_Hourly_Revenue_Completeness_Check',
    @schedule_name = N'Hourly';

EXEC sp_add_jobserver
    @job_name = N'DQ_Hourly_Revenue_Completeness_Check';
GO


-- ==================== EMAIL ALERT CONFIGURATION ====================
-- Configure Database Mail to send alerts when threshold is breached

EXEC msdb.dbo.sp_update_job
    @job_name = N'DQ_Hourly_Revenue_Completeness_Check',
    @notify_level_email = 2, -- On failure
    @notify_email_operator_name = N'DataOpsTeam';
