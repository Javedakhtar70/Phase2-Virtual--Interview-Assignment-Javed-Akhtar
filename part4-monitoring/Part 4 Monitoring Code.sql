--1 Schema & helper objects (run once)
--Run this once to create schema, tables, and Database Mail stored procedure wrappers.


-- 1. Create schema & staging table
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'monitoring') 
    EXEC('CREATE SCHEMA monitoring');

GO

IF OBJECT_ID('monitoring.events_staging','U') IS NULL
BEGIN
CREATE TABLE monitoring.events_staging (
    client_id       NVARCHAR(255) NULL,
    page_url        NVARCHAR(MAX) NULL,
    referrer        NVARCHAR(MAX) NULL,
    timestamp_str   NVARCHAR(128) NULL,
    event_name      NVARCHAR(255) NULL,
    event_data      NVARCHAR(MAX) NULL,
    user_agent      NVARCHAR(MAX) NULL,
    -- parsed columns for convenience
    timestamp_parsed DATETIME2 NULL,
    transaction_id  NVARCHAR(255) NULL,
    revenue_val     DECIMAL(18,4) NULL,
    session_id      NVARCHAR(512) NULL
);
END
GO

-- 2. Metrics table to accumulate daily monitoring results
IF OBJECT_ID('monitoring.pipeline_metrics','U') IS NULL
BEGIN
CREATE TABLE monitoring.pipeline_metrics (
    run_date        DATE NOT NULL,
    run_time        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    total_events    INT,
    bad_timestamps  INT,
    bad_ts_pct      FLOAT,
    tx_count        INT,
    tx_with_revenue INT,
    tx_with_revenue_ratio FLOAT,
    total_revenue   DECIMAL(18,2),
    duplicate_tx_count INT,
    session_raw_count INT NULL,
    session_events_sum INT NULL,
    session_mismatch_pct FLOAT NULL,
    overall_status  NVARCHAR(16),
    notes           NVARCHAR(MAX)
);
END
GO

-- 3. Simple alerts table (history of alerts)
IF OBJECT_ID('monitoring.pipeline_alerts','U') IS NULL
BEGIN
CREATE TABLE monitoring.pipeline_alerts (
    alert_id INT IDENTITY(1,1) PRIMARY KEY,
    run_time DATETIME2 DEFAULT SYSUTCDATETIME(),
    severity NVARCHAR(16),
    alert_name NVARCHAR(128),
    detail NVARCHAR(MAX)
);
END
GO

-- 4. Optionally, ensure Database Mail is enabled if you want email alerts.
-- (This step is admin-level - skip if DB Mail not configured)
-- You can use msdb.dbo.sp_send_dbmail to send email when criticals occur.


/*
2) Stored procedure to load CSV into staging
This uses BULK INSERT. Make sure the SQL Server service account can access the file path 
or use a UNC share
*/


CREATE OR ALTER PROCEDURE monitoring.usp_load_events_staging
    @FilePath NVARCHAR(4000) = N'C:\Users\Admin\Documents\New Job\New Applications\Puffy\events.csv'
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE monitoring.events_staging;

    BEGIN TRY
        ---------------------------------------------------------
        -- Build BULK INSERT dynamically (cannot use variable directly)
        ---------------------------------------------------------
        DECLARE @sql NVARCHAR(MAX) = N'
            BULK INSERT monitoring.events_staging
            FROM ''' + @FilePath + N'''
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                TABLOCK,
                CODEPAGE = ''65001''
            );
        ';

        PRINT @sql;  -- optional for debugging

        EXEC(@sql);

        ---------------------------------------------------------
        -- Parse timestamp
        ---------------------------------------------------------
        UPDATE monitoring.events_staging
        SET timestamp_parsed = TRY_CAST(timestamp_str AS DATETIME2)
        WHERE timestamp_str IS NOT NULL;

        ---------------------------------------------------------
        -- Extract fields from JSON
        ---------------------------------------------------------
        UPDATE monitoring.events_staging
        SET 
            transaction_id = COALESCE(
                NULLIF(JSON_VALUE(event_data,'$.transaction_id'),''), 
                NULLIF(JSON_VALUE(event_data,'$.order_id'),''), 
                NULLIF(JSON_VALUE(event_data,'$.orderId'),'')
            ),
            revenue_val = TRY_CAST(
                COALESCE(
                    JSON_VALUE(event_data,'$.revenue'),
                    JSON_VALUE(event_data,'$.total'),
                    JSON_VALUE(event_data,'$.value'),
                    JSON_VALUE(event_data,'$.amount')
                ) AS DECIMAL(18,4)
            )
        WHERE event_data IS NOT NULL;

    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        RAISERROR('Error in usp_load_events_staging: %s', 16, 1, @err);
        THROW;
    END CATCH
END;
GO


/*
3) Stored procedure that runs the monitoring checks

This implements the checks and writes to monitoring.pipeline_metrics and 
monitoring.pipeline_alerts. You can call this as a separate Agent job step after load.
*/

CREATE OR ALTER PROCEDURE monitoring.usp_run_pipeline_monitoring
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @total_events INT,
        @bad_ts INT,
        @bad_ts_pct FLOAT,
        @tx_count INT,
        @tx_with_rev INT,
        @tx_with_rev_ratio FLOAT,
        @total_revenue DECIMAL(18,2),
        @dup_tx_count INT,
        @session_events_sum INT = NULL,
        @session_raw_count INT = NULL,
        @session_mismatch_pct FLOAT = NULL,
        @overall_status NVARCHAR(16) = 'ok';

    -- 1. total events
    SELECT @total_events = COUNT(*) FROM monitoring.events_staging;

    -- 2. bad timestamps
    SELECT @bad_ts = COUNT(*) FROM monitoring.events_staging WHERE timestamp_parsed IS NULL OR RTRIM(LTRIM(timestamp_str)) = '';
    SET @bad_ts_pct = CASE WHEN @total_events > 0 THEN CAST(@bad_ts AS FLOAT) / @total_events ELSE 0 END;

    -- 3. transactions & revenue coverage
    SELECT @tx_count = COUNT(*) 
    FROM monitoring.events_staging
    WHERE event_name IS NOT NULL AND LOWER(event_name) LIKE '%checkout%' OR LOWER(event_name) LIKE '%purchase%' OR LOWER(event_name) LIKE '%order_complete%';

    -- tx with revenue: use revenue_val parsed earlier or try JSON extract on the fly
    SELECT @tx_with_rev = COUNT(*) 
    FROM monitoring.events_staging
    WHERE ( (event_name IS NOT NULL AND (LOWER(event_name) LIKE '%checkout%' OR LOWER(event_name) LIKE '%purchase%' OR LOWER(event_name) LIKE '%order_complete%'))
            AND (revenue_val IS NOT NULL OR TRY_CAST(JSON_VALUE(event_data,'$.revenue') AS DECIMAL(18,4)) IS NOT NULL OR TRY_CAST(JSON_VALUE(event_data,'$.total') AS DECIMAL(18,4)) IS NOT NULL) );

    SET @tx_with_rev_ratio = CASE WHEN @tx_count > 0 THEN CAST(@tx_with_rev AS FLOAT) / @tx_count ELSE 1.0 END;

    -- total revenue
    SELECT @total_revenue = ISNULL(SUM(revenue_val),0.0) FROM monitoring.events_staging;

    -- 4. duplicate transaction_id count
    SELECT @dup_tx_count = COUNT(*) 
    FROM (
        SELECT transaction_id FROM monitoring.events_staging
        WHERE transaction_id IS NOT NULL AND RTRIM(LTRIM(transaction_id)) <> ''
        GROUP BY transaction_id HAVING COUNT(*) > 1
    ) d;

    -- 5. sessionization reconciliation (if you have vw_sessions in analytics schema)
    IF OBJECT_ID('analytics.vw_sessions','V') IS NOT NULL
    BEGIN
        SELECT @session_raw_count = (SELECT COUNT(*) FROM raw_events); -- update to your raw table
        -- sum events_count from vw_sessions
        SELECT @session_events_sum = ISNULL(SUM(events_count),0) FROM analytics.vw_sessions;
        IF @session_raw_count IS NULL
        BEGIN
            -- fallback: use staging total as raw count
            SET @session_raw_count = @total_events;
        END
        IF @session_raw_count > 0
            SET @session_mismatch_pct = ABS(CAST(@session_events_sum - @session_raw_count AS FLOAT) / @session_raw_count);
    END

    -- decide status based on thresholds (example thresholds - tune them)
    IF @total_events = 0
        SET @overall_status = 'critical';
    ELSE IF @bad_ts_pct > 0.01 OR (@tx_count > 0 AND @tx_with_rev_ratio < 0.95) OR @dup_tx_count > 0
        SET @overall_status = 'warning';

    IF @bad_ts_pct > 0.01 OR (@tx_count > 0 AND @tx_with_rev_ratio < 0.95) OR @dup_tx_count > 0
    BEGIN
        INSERT INTO monitoring.pipeline_alerts (severity, alert_name, detail)
        VALUES ('warning', 'Data QC issues', CONCAT('bad_ts_pct=', FORMAT(@bad_ts_pct,'P2'), '; tx_with_rev_ratio=', FORMAT(@tx_with_rev_ratio,'P2'), '; dup_tx=', @dup_tx_count));
    END

    IF @total_events = 0 OR (@tx_count > 0 AND @tx_with_rev_ratio < 0.80) OR @dup_tx_count > 5
    BEGIN
        INSERT INTO monitoring.pipeline_alerts (severity, alert_name, detail)
        VALUES ('critical', 'Severe pipeline problem', CONCAT('total_events=', @total_events, '; tx_with_rev_ratio=', FORMAT(@tx_with_rev_ratio,'P2'), '; dup_tx=', @dup_tx_count));
    END

    -- 6. write metrics
    INSERT INTO monitoring.pipeline_metrics (run_date, total_events, bad_timestamps, bad_ts_pct, tx_count, tx_with_revenue, tx_with_revenue_ratio, total_revenue, duplicate_tx_count, session_raw_count, session_events_sum, session_mismatch_pct, overall_status)
    VALUES (CAST(SYSDATETIME() AS DATE), @total_events, @bad_ts, @bad_ts_pct, ISNULL(@tx_count,0), ISNULL(@tx_with_rev,0), ISNULL(@tx_with_rev_ratio,0.0), ISNULL(@total_revenue,0.0), ISNULL(@dup_tx_count,0), @session_raw_count, @session_events_sum, @session_mismatch_pct, @overall_status);

    -- 7. Optionally send DB Mail for criticals (requires Database Mail configured)
    IF EXISTS (SELECT 1 FROM monitoring.pipeline_alerts WHERE severity = 'critical' AND run_time >= DATEADD(MINUTE,-5, SYSUTCDATETIME()))
    BEGIN
        -- NOTE: configure Database Mail profile and replace 'YourMailProfile' and recipients
        DECLARE @subject NVARCHAR(200) = 'CRITICAL: Pipeline Monitor Alert';
        DECLARE @body NVARCHAR(MAX) = 'Critical alerts detected. Please check monitoring.pipeline_alerts table for details.';
        -- Uncomment the next line after Database Mail is configured
        -- EXEC msdb.dbo.sp_send_dbmail @profile_name='YourMailProfile', @recipients='ops@example.com', @subject=@subject, @body=@body;
    END

END
GO

/*
4) Create a SQL Server Agent Job (2 steps)
Create a job that first runs the usp_load_events_staging (with the CSV path), 
then usp_run_pipeline_monitoring.
*/
USE msdb;
GO
-- VARIABLES 
DECLARE @JobName SYSNAME = N'Pipeline_Daily_Load_and_Monitor';
DECLARE @ScheduleName SYSNAME = N'Daily_2_30AM';

IF EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobschedules js
    JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
	JOIN msdb.dbo.sysschedules s
    ON js.schedule_id = s.schedule_id
    WHERE j.name = @JobName
      AND s.schedule_uid = @ScheduleName
)
BEGIN
	   PRINT 'Existing schedule found for job - deleting before recreating.';
		EXEC msdb.dbo.sp_delete_jobschedule 
			@job_name = @JobName,
			@Schedule_name = @ScheduleName;
END
ELSE
	BEGIN
		PRINT 'No existing schedule found (or different name). Proceeding to create schedule.';
	END
GO



EXEC sp_add_job @job_name = N'Pipeline_Daily_Load_and_Monitor';
-- Step 1: load
EXEC sp_add_jobstep 
    @job_name = N'Pipeline_Daily_Load_and_Monitor',
    @step_name = N'Load_Events_Staging',
    @subsystem = N'TSQL',
    @command = N'EXEC monitoring.usp_load_events_staging ''C:\Users\Admin\Documents\New Job\New Applications\Puffy\Phase2\HoDIA - ST dataset-20251118T051305Z-1-001\HoDIA - ST dataset\events.csv'';',
    @database_name = N'master';

-- Step 2: run checks
EXEC sp_add_jobstep 
    @job_name = N'Pipeline_Daily_Load_and_Monitor',
    @step_name = N'Run_Pipeline_Monitoring',
    @subsystem = N'TSQL',
    @command = N'EXEC monitoring.usp_run_pipeline_monitoring;',
    @database_name = N'master';

-- Schedule: daily at 02:30 AM (adjust)
EXEC sp_add_jobschedule 
    @job_name = N'Pipeline_Daily_Load_and_Monitor',
    @name = N'Daily_2_30AM',
    @freq_type = 4,  -- daily
    @active_start_time = 013600;

-- Enable the job
EXEC sp_add_jobserver @job_name = N'Pipeline_Daily_Load_and_Monitor';
GO
