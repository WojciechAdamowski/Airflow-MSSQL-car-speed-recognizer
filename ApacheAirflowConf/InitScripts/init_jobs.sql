USE msdb;
GO

/*

JOB: archive_all_retention_tables_job - OUTDATED


IF NOT EXISTS(SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'archive_all_retention_tables_job')
EXEC dbo.sp_add_job
    @job_name = N'archive_all_retention_tables_job',
    @enabled = 1,
    @description = N'Daily archiving job for bronze tables';
GO

IF NOT EXISTS(
    SELECT 1
    FROM msdb.dbo.sysjobsteps s
    INNER JOIN msdb.dbo.sysjobs j ON s.job_id = j.job_id
    WHERE j.name = N'archive_all_retention_tables_job' AND s.step_name = N'Run archive procedure'
)
EXEC dbo.sp_add_jobstep
    @job_name = N'archive_all_retention_tables_job',
    @step_name = N'Run archive procedure',
    @subsystem = N'TSQL',
    @database_name = N'car_speed_recognizer',
    @command = N'EXEC meta.archive_all_retention_tables;';
GO

IF NOT EXISTS(SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'Daily_0000')
EXEC dbo.sp_add_schedule
    @schedule_name = N'Daily_0000',
    @freq_type = 4,              -- daily
    @freq_interval = 1,          -- every day
    @active_start_time = 000000;  -- 02:00:00
GO

IF NOT EXISTS
(
    SELECT 1
    FROM msdb.dbo.sysjobschedules js
    INNER JOIN msdb.dbo.sysjobs j
        ON js.job_id = j.job_id
    INNER JOIN msdb.dbo.sysschedules s
        ON js.schedule_id = s.schedule_id
    WHERE j.name = N'archive_all_retention_tables_job'
      AND s.name = N'Daily_0000'
)
EXEC dbo.sp_attach_schedule
    @job_name = N'archive_all_retention_tables_job',
    @schedule_name = N'Daily_0000';
GO

IF NOT EXISTS
(
    SELECT 1
    FROM msdb.dbo.sysjobservers js
    INNER JOIN msdb.dbo.sysjobs j
        ON js.job_id = j.job_id
    WHERE j.name = N'archive_all_retention_tables_job'
)
EXEC dbo.sp_add_jobserver
    @job_name = N'archive_all_retention_tables_job';
GO

*/