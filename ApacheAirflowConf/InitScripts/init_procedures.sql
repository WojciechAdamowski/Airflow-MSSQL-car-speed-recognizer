USE car_speed_recognizer


/*
    Creating meta schema objects
*/
IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_aul_audit_load')
EXEC('
    CREATE PROCEDURE [meta].[load_aul_audit_load]
    (
        @aul_action               varchar(10),           -- started / finished
        @aul_load_id              bigint = NULL OUTPUT,

        @aul_pipeline_name        sysname = NULL,
        @aul_procedure_name       sysname = NULL,
        @aul_source_schema_name   sysname = NULL,
        @aul_source_table_name    sysname = NULL,
        @aul_target_schema_name   sysname = NULL,
        @aul_target_table_name    sysname = NULL,

        @aul_logical_date         datetime = NULL,
        @aul_window_from_time     datetime = NULL,
        @aul_window_to_time       datetime = NULL,

        @aul_count_loaded_rows    int = NULL,
        @aul_count_inserted_rows  int = NULL,
        @aul_count_updated_rows   int = NULL,
        @aul_count_deleted_rows   int = NULL,
        @aul_count_rejected_rows  int = NULL,

        @aul_status               varchar(20) = NULL,    -- success / failed
        @aul_error_message        nvarchar(4000) = NULL,
        @aul_batch_id             varchar(100) = NULL
    )
    AS
    BEGIN
        SET NOCOUNT ON;

        IF @aul_action = ''started''
        BEGIN
            INSERT INTO [meta].[aul_audit_load]
            (
                aul_pipeline_name,
                aul_procedure_name,
                aul_source_schema_name,
                aul_source_table_name,
                aul_target_schema_name,
                aul_target_table_name,
                aul_run_start_time,
                aul_logical_date,
                aul_window_from_time,
                aul_window_to_time,
                aul_status,
                aul_batch_id
            )
            VALUES
            (
                @aul_pipeline_name,
                @aul_procedure_name,
                @aul_source_schema_name,
                @aul_source_table_name,
                @aul_target_schema_name,
                @aul_target_table_name,
                sysdatetime(),
                @aul_logical_date,
                @aul_window_from_time,
                @aul_window_to_time,
                ''started'',
                @aul_batch_id
            );

            SET @aul_load_id = SCOPE_IDENTITY();
            RETURN;
        END

        IF @aul_action = ''finished''
        BEGIN
            UPDATE [meta].[aul_audit_load]
            SET
                aul_run_end_time        = sysdatetime(),
                aul_count_loaded_rows   = @aul_count_loaded_rows,
                aul_count_inserted_rows = @aul_count_inserted_rows,
                aul_count_updated_rows  = @aul_count_updated_rows,
                aul_count_deleted_rows  = @aul_count_deleted_rows,
                aul_count_rejected_rows = @aul_count_rejected_rows,
                aul_status              = @aul_status,
                aul_error_message       = @aul_error_message
            WHERE aul_load_id = @aul_load_id;

            RETURN;
        END
    END;
')

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_alf_audit_loaded_files')
EXEC('
    CREATE PROCEDURE [meta].[load_alf_audit_loaded_files] (
        @alf_batch_id             VARCHAR(500)
        , @alf_pipeline_name        VARCHAR(500)
        , @alf_source_rows          INT
        , @alf_rejected_rows        INT
        , @alf_file_size_bytes      BIGINT
        , @alf_file_path            VARCHAR(500)
        , @alf_status               VARCHAR(100)
        , @alf_error_message        VARCHAR(1000)
        , @alf_load_start_datetime  DATETIME
        , @alf_load_end_datetime    DATETIME
        , @alf_source_system        VARCHAR(200)
        , @alf_target_schema        SYSNAME
        , @alf_target_table         SYSNAME
    ) AS
    BEGIN
        SET NOCOUNT ON;

        INSERT INTO [meta].[alf_audit_loaded_files] (
            alf_batch_id
            ,alf_pipeline_name
            ,alf_source_rows
            ,alf_rejected_rows
            ,alf_file_size_bytes
            ,alf_file_path
            ,alf_status
            ,alf_error_message
            ,alf_load_start_datetime
            ,alf_load_end_datetime
            ,alf_source_system
            ,alf_target_schema
            ,alf_target_table
        ) VALUES (
            @alf_batch_id
            , @alf_pipeline_name
            , @alf_source_rows
            , @alf_rejected_rows
            , @alf_file_size_bytes
            , @alf_file_path
            , @alf_status
            , @alf_error_message
            , @alf_load_start_datetime
            , @alf_load_end_datetime
            , @alf_source_system
            , @alf_target_schema
            , @alf_target_table
        )
    END
')

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_aup_audit_process')
EXEC('
    CREATE PROCEDURE [meta].[load_aup_audit_process] (
        @aup_id INT                         = NULL OUTPUT
        , @aup_batch_id VARCHAR(500)        = NULL
        , @aup_pipeline_name VARCHAR(500)   = NULL
        , @aup_start_datetime DATETIME      = NULL
        , @aup_end_datetime DATETIME        = NULL
        , @aup_status NVARCHAR(20)          = NULL
        , @aup_error_message NVARCHAR(MAX)  = NULL
        , @action VARCHAR(20)
    ) AS
        BEGIN
            SET NOCOUNT ON;

            IF @action = ''started''
                BEGIN
                    INSERT INTO [meta].[aup_audit_process] (
                        aup_batch_id
                        , aup_pipeline_name
                        , aup_start_datetime
                        , aup_status
                    ) VALUES (
                        @aup_batch_id
                        , @aup_pipeline_name
                        , @aup_start_datetime
                        , @aup_status
                    )

                    SET @aup_id = SCOPE_IDENTITY();
                    SELECT @aup_id
                END

            IF @action = ''finished''
                BEGIN
                    UPDATE [meta].[aup_audit_process]
                    SET aup_end_datetime = @aup_end_datetime
                        ,aup_error_message = @aup_error_message
                        ,aup_status = @aup_status
                    WHERE aup_id = @aup_id
                END

        END
')


/*
    Creating silver schema objects
*/

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_d_vet_vehicle_type')
EXEC('
    CREATE PROCEDURE [silver].[load_d_vet_vehicle_type] (
        @aul_window_from_time DATETIME
        ,@aul_window_to_time DATETIME
        ,@aul_pipeline_name SYSNAME
        ,@aul_logical_date DATETIME
        ,@aul_batch_id VARCHAR(100)
    ) AS
    BEGIN
        SET NOCOUNT ON;

        /*********** AUDIT ***********/
        DECLARE @load_id BIGINT;
        DECLARE @count_loaded_rows INT = 0;
        DECLARE @count_inserted_rows INT = 0;
        DECLARE @count_updated_rows INT = 0;
        DECLARE @count_deleted_rows INT = 0;
        DECLARE @count_rejected_rows INT = 0;
        DECLARE @actions TABLE (
            action_name nvarchar(10)
        );

        EXECUTE [meta].[load_aul_audit_load]
            @aul_action = ''started'',
            @aul_load_id = @load_id OUTPUT,
            @aul_pipeline_name = @aul_pipeline_name,
            @aul_procedure_name = ''silver.load_d_vet_vehicle_type'',
            @aul_source_schema_name = ''bronze'',
            @aul_source_table_name = ''fs_car_speed_catches'',
            @aul_target_schema_name = ''silver'',
            @aul_target_table_name = ''d_vet_vehicle_type'',
            @aul_logical_date = @aul_logical_date,
            @aul_window_from_time = @aul_window_from_time,
            @aul_window_to_time = @aul_window_to_time,
            @aul_batch_id = @aul_batch_id;
        /*****************************/

        BEGIN TRANSACTION;
        BEGIN TRY

                SELECT
                    DISTINCT vehicle_type AS [d_vet_name]
                INTO #source
                FROM [bronze].[fs_car_speed_catches]
                WHERE md_insert_datetime >= @aul_window_from_time AND md_insert_datetime <= @aul_window_to_time

            MERGE [silver].[d_vet_vehicle_type] AS t
            USING #source AS s
            ON t.d_vet_name = s.d_vet_name
            WHEN NOT MATCHED THEN INSERT (
                d_vet_name
            ) VALUES (
                s.d_vet_name
            )
            OUTPUT $action INTO @actions;


            /*********** AUDIT ***********/
            SET @count_loaded_rows = ISNULL((SELECT COUNT(1) FROM #source), 0)
            SET @count_inserted_rows = ISNULL((SELECT SUM(IIF(action_name = ''INSERT'', 1, 0)) FROM @actions), 0)

            EXECUTE [meta].[load_aul_audit_load]
                @aul_action = ''finished''
                , @aul_load_id = @load_id
                , @aul_count_loaded_rows = @count_loaded_rows
                , @aul_count_inserted_rows = @count_inserted_rows
                , @aul_count_updated_rows = @count_updated_rows
                , @aul_count_deleted_rows = @count_deleted_rows
                , @aul_count_rejected_rows = @count_rejected_rows
                , @aul_status = ''success''
            /*****************************/

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH

            /*********** AUDIT ***********/
            EXECUTE [meta].[load_aul_audit_load]
                @aul_action = ''finished''
                , @aul_load_id = @load_id
                , @aul_count_loaded_rows = @count_loaded_rows
                , @aul_count_inserted_rows = @count_inserted_rows
                , @aul_count_updated_rows = @count_updated_rows
                , @aul_count_deleted_rows = @count_deleted_rows
                , @aul_count_rejected_rows = @count_rejected_rows
                , @aul_status = ''failed''
                , @aul_error_message = ERROR_MESSAGE
            /*****************************/

            IF @@TRANCOUNT>0
                ROLLBACK TRANSACTION;
            THROW;
        END CATCH
    END
')

IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_d_seg_segment')
EXEC('
    CREATE PROCEDURE [silver].[load_d_seg_segment] (
        @aul_window_from_time DATETIME
        ,@aul_window_to_time DATETIME
        ,@aul_pipeline_name SYSNAME
        ,@aul_logical_date DATETIME
        ,@aul_batch_id VARCHAR(100)
    ) AS
    BEGIN
        SET NOCOUNT ON;

        /*********** AUDIT ***********/
        DECLARE @load_id BIGINT;
        DECLARE @count_loaded_rows INT = 0;
        DECLARE @count_inserted_rows INT = 0;
        DECLARE @count_updated_rows INT = 0;
        DECLARE @count_deleted_rows INT = 0;
        DECLARE @count_rejected_rows INT = 0;
        DECLARE @actions TABLE (
            action_name NVARCHAR(10)
            , updated_id INT
        );

        EXECUTE [meta].[load_aul_audit_load]
            @aul_action = ''started'',
            @aul_load_id = @load_id OUTPUT,
            @aul_pipeline_name = @aul_pipeline_name,
            @aul_procedure_name = ''silver.load_d_seg_segment'',
            @aul_source_schema_name = ''bronze'',
            @aul_source_table_name = ''fs_car_speed_catches'',
            @aul_target_schema_name = ''silver'',
            @aul_target_table_name = ''d_seg_segment'',
            @aul_logical_date = @aul_logical_date,
            @aul_window_from_time = @aul_window_from_time,
            @aul_window_to_time = @aul_window_to_time,
            @aul_batch_id = @aul_batch_id;
        /*****************************/

        BEGIN TRANSACTION;
        BEGIN TRY;
            DROP TABLE IF EXISTS #source
            DROP TABLE IF EXISTS #incorrect_source
            DROP TABLE IF EXISTS #correct_source
            DROP TABLE IF EXISTS #to_merge_source

            SELECT DISTINCT
                fs_csc.segment_id           AS [d_seg_source_id]
                , fs_csc.segment_name       AS [d_seg_name]
                , fs_csc.segment_length_m   AS [d_seg_length_m]
                , fs_csc.speed_limit_kmh    AS [d_seg_speed_limit]
            INTO #source
            FROM [bronze].[fs_car_speed_catches] AS fs_csc
            LEFT JOIN [meta].[alf_audit_loaded_files] AS alf ON alf.alf_file_path = fs_csc.md_file_path
            WHERE fs_csc.md_insert_datetime >= @aul_window_from_time
                AND fs_csc.md_insert_datetime <= @aul_window_to_time
                AND alf.alf_status = ''success''

            SELECT DISTINCT
                [d_seg_source_id]
                , [d_seg_name]
                , [d_seg_length_m]
                , [d_seg_speed_limit]
            INTO #incorrect_source
            FROM #source AS fs_csc
            WHERE [d_seg_source_id] = 999999
                    OR NOT [d_seg_name] LIKE ''%-%''
                    OR [d_seg_length_m] <= 0
                    OR [d_seg_speed_limit] IS NULL
            ORDER BY [d_seg_source_id]

            SELECT *
            INTO #correct_source
            FROM (
                SELECT DISTINCT
                    [d_seg_source_id]
                    , [d_seg_name]
                    , [d_seg_length_m]
                    , [d_seg_speed_limit]
                FROM #source AS fs_csc

                EXCEPT

                SELECT * FROM #incorrect_source
            ) AS s

            SELECT *
            INTO #to_merge_source
            FROM (
                SELECT DISTINCT
                    [d_seg_source_id]
                    , [d_seg_name]
                    , [d_seg_length_m]
                    , [d_seg_speed_limit]
                FROM #correct_source AS fs_csc

                EXCEPT

                SELECT
                    [d_seg_source_id]
                    , [d_seg_name]
                    , [d_seg_length_m]
                    , [d_seg_speed_limit]
                FROM [silver].[d_seg_segment] WHERE md_is_current = 1
            ) AS s

            /*********** INSERT (new rows) and UPDATE (mark as old rows) ***********/

            MERGE INTO [silver].[d_seg_segment] AS t
            USING #to_merge_source AS s
            ON t.md_is_current = 1 AND t.d_seg_source_id = s.d_seg_source_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.md_end_datetime = @aul_window_to_time
                    , t.md_is_current = 0
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    [d_seg_source_id]
                    , [d_seg_name]
                    , [d_seg_length_m]
                    , [d_seg_speed_limit]
                    , [md_start_datetime]
                    , [md_end_datetime]
                    , [md_is_current]
                ) VALUES (
                    [d_seg_source_id]
                    , [d_seg_name]
                    , [d_seg_length_m]
                    , [d_seg_speed_limit]
                    , @aul_window_to_time
                    , NULL
                    , 1
                )
            OUTPUT
                $action                     AS [action_name]
                , inserted.d_seg_source_id  AS [updated_id]
            INTO @actions (action_name, updated_id) ;


            /*********** INSERT (newest versions) ***********/

            INSERT INTO [silver].[d_seg_segment] (
                [d_seg_source_id]
                , [d_seg_name]
                , [d_seg_length_m]
                , [d_seg_speed_limit]
                , [md_start_datetime]
                , [md_end_datetime]
                , [md_is_current]
            ) SELECT
                [d_seg_source_id]
                , [d_seg_name]
                , [d_seg_length_m]
                , [d_seg_speed_limit]
                , @aul_window_to_time
                , NULL
                , 1
            FROM #source s
            JOIN @actions a ON s.d_seg_source_id = a.updated_id AND a.action_name = ''UPDATE''

            /*********** AUDIT ***********/

            SET @count_loaded_rows = ISNULL((SELECT COUNT(1) FROM (
                SELECT DISTINCT
                    fs_csc.segment_id           AS [d_seg_source_id]
                    , fs_csc.segment_name       AS [d_seg_name]
                    , fs_csc.segment_length_m   AS [d_seg_length_m]
                    , fs_csc.speed_limit_kmh    AS [d_seg_speed_limit]
                FROM [bronze].[fs_car_speed_catches] AS fs_csc
                WHERE md_insert_datetime >= @aul_window_from_time
                    AND md_insert_datetime <= @aul_window_to_time
            ) AS s), 0)

            SET @count_inserted_rows = ISNULL((SELECT SUM(IIF(action_name = ''INSERT'', 1, 0)) FROM @actions)
                + (SELECT COUNT(1) FROM #source s JOIN @actions a ON s.d_seg_source_id = a.updated_id AND a.action_name = ''UPDATE''), 0)

            SET @count_updated_rows = ISNULL((SELECT SUM(IIF(action_name = ''UPDATE'', 1, 0)) FROM @actions), 0)

            SET @count_rejected_rows = ISNULL((SELECT COUNT(1) FROM #incorrect_source), 0)

            EXECUTE [meta].[load_aul_audit_load]
                @aul_action = ''finished''
                , @aul_load_id = @load_id
                , @aul_count_loaded_rows = @count_loaded_rows
                , @aul_count_inserted_rows = @count_inserted_rows
                , @aul_count_updated_rows = @count_updated_rows
                , @aul_count_deleted_rows = @count_deleted_rows
                , @aul_count_rejected_rows = @count_rejected_rows
                , @aul_status = ''success''
            /*****************************/

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH;

            /*********** AUDIT ***********/
            EXECUTE [meta].[load_aul_audit_load]
                @aul_action = ''finished''
                , @aul_load_id = @load_id
                , @aul_count_loaded_rows = @count_loaded_rows
                , @aul_count_inserted_rows = @count_inserted_rows
                , @aul_count_updated_rows = @count_updated_rows
                , @aul_count_deleted_rows = @count_deleted_rows
                , @aul_count_rejected_rows = @count_rejected_rows
                , @aul_status = ''failed''
                , @aul_error_message = ERROR_MESSAGE
            /*****************************/

            IF @@ROWCOUNT > 0
                ROLLBACK TRANSACTION;
            THROW;
        END CATCH
    END
')