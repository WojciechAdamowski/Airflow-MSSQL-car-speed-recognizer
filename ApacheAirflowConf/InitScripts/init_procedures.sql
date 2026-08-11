USE car_speed_recognizer

SET QUOTED_IDENTIFIER ON

/*
    Creating meta schema objects
*/
IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_aua_audit_aggregate')
EXEC('
    CREATE PROCEDURE [meta].[load_aua_audit_aggregate]
(
    @aua_action                 VARCHAR(10)
    , @aua_id                   BIGINT = NULL OUTPUT
    , @aua_target_schema_name   SYSNAME = NULL
    , @aua_target_table_name    SYSNAME = NULL
    , @aua_procedure_name       SYSNAME = NULL
    , @aua_count_rows           INT = NULL
    , @aua_status               VARCHAR(20) = NULL
    , @aua_error_message        NVARCHAR(4000) = NULL
    , @aua_logical_date         DATETIME = NULL
    , @aua_from_datetime        DATETIME = NULL
    , @aua_to_datetime          DATETIME = NULL
    , @aua_batch_id             VARCHAR(100) = NULL
    , @aua_pipeline_name        SYSNAME = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @aua_action = ''started''
    BEGIN
        INSERT INTO meta.aua_audit_aggregate (
            aua_target_schema_name
            , aua_target_table_name
            , aua_procedure_name
            , aua_status
            , aua_logical_date
            , aua_from_datetime
            , aua_to_datetime
            , aua_batch_id
            , aua_pipeline_name
        )
        VALUES (
            @aua_target_schema_name
            , @aua_target_table_name
            , @aua_procedure_name
            , ''started''
            , @aua_logical_date
            , @aua_from_datetime
            , @aua_to_datetime
            , @aua_batch_id
            , @aua_pipeline_name
        );

        SET @aua_id = SCOPE_IDENTITY();
        RETURN;
    END

    IF @aua_action = ''finished''
    BEGIN
        UPDATE meta.aua_audit_aggregate
        SET   aua_run_end_time = SYSDATETIME()
            , aua_count_rows = @aua_count_rows
            , aua_status = @aua_status
            , aua_error_message = @aua_error_message
        WHERE aua_id = @aua_id;

        RETURN;
    END
END
    ')

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_aul_audit_load')
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
        @alf_id                     INT = NULL OUTPUT
        , @alf_batch_id             VARCHAR(500) = NULL
        , @alf_pipeline_name        VARCHAR(500) = NULL
        , @alf_source_rows          INT = NULL
        , @alf_rejected_rows        INT = NULL
        , @alf_file_size_bytes      BIGINT = NULL
        , @alf_file_path            VARCHAR(500) = NULL
        , @alf_status               VARCHAR(100) = NULL
        , @alf_error_message        VARCHAR(1000) = NULL
        , @alf_load_start_datetime  DATETIME = NULL
        , @alf_load_end_datetime    DATETIME = NULL
        , @alf_source_system        VARCHAR(200) = NULL
        , @alf_target_schema        SYSNAME = NULL
        , @alf_target_table         SYSNAME = NULL
        , @action                   VARCHAR(10) = NULL
    ) AS
    BEGIN
        SET NOCOUNT ON;

        IF @action = ''started''
            BEGIN
                 INSERT INTO [meta].[alf_audit_loaded_files] (
                    alf_batch_id
                    ,alf_pipeline_name
                    ,alf_file_path
                    ,alf_status
                    ,alf_load_start_datetime
                    ,alf_source_system
                    ,alf_target_schema
                    ,alf_target_table
                ) VALUES (
                    @alf_batch_id
                    , @alf_pipeline_name
                    , @alf_file_path
                    , ''started''
                    , @alf_load_start_datetime
                    , @alf_source_system
                    , @alf_target_schema
                    , @alf_target_table
                )

                SET @alf_id = SCOPE_IDENTITY()
                SELECT @alf_id
            END

        IF @action = ''finished''
            BEGIN
                UPDATE [meta].[alf_audit_loaded_files] SET
                    alf_source_rows = @alf_source_rows
                    , alf_rejected_rows = @alf_rejected_rows
                    , alf_file_size_bytes = @alf_file_size_bytes
                    , alf_status = @alf_status
                    , alf_error_message = @alf_error_message
                    , alf_load_end_datetime = @alf_load_end_datetime
                WHERE alf_id = @alf_id
            END
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
            action_name NVARCHAR(10)
            , updated_id INT
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

            DROP TABLE IF EXISTS #source

            /***** BLANK ROW INSERT ******/

            IF NOT EXISTS (SELECT 1 FROM [silver].[d_vet_vehicle_type] WHERE d_vet_id = 0)
            BEGIN

                SET IDENTITY_INSERT [silver].[d_vet_vehicle_type] ON

                INSERT INTO [silver].[d_vet_vehicle_type](d_vet_id, d_vet_name)
                VAlUES (0, ''EMPTY'')

                SET IDENTITY_INSERT [silver].[d_vet_vehicle_type] OFF

            END

            /*****************************/

            SELECT
                DISTINCT vehicle_type AS [d_vet_name]
            INTO #source
            FROM [bronze].[fs_car_speed_catches]
            WHERE md_insert_datetime >= @aul_window_from_time
              AND md_insert_datetime <= @aul_window_to_time

            MERGE [silver].[d_vet_vehicle_type] AS t
            USING #source AS s
            ON t.d_vet_name = s.d_vet_name
            WHEN NOT MATCHED THEN INSERT (
                d_vet_name
            ) VALUES (
                s.d_vet_name
            )
            OUTPUT
                $action     AS [action_name]
                , NULL      AS [updated_id]
            INTO @actions (action_name, updated_id);


            /*********** AUDIT ***********/
            SET @count_loaded_rows = (SELECT COUNT(*) FROM #source)

            SET @count_inserted_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''INSERT'')

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

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_d_seg_segment')
EXEC('


    CREATE  PROCEDURE [silver].[load_d_seg_segment] (
        @aul_window_from_time DATETIME
        ,@aul_window_to_time DATETIME
        ,@aul_pipeline_name SYSNAME
        ,@aul_logical_date DATETIME
        ,@aul_batch_id VARCHAR(100)
    ) AS
    BEGIN
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

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

            /***** BLANK ROW INSERT ******/

            IF NOT EXISTS (SELECT 1 FROM [silver].[d_seg_segment] WHERE d_seg_id = 0)
            BEGIN

                SET IDENTITY_INSERT [silver].[d_seg_segment] ON

                INSERT INTO [silver].[d_seg_segment] (d_seg_id, d_seg_source_id, d_seg_name, d_seg_length_m, d_seg_speed_limit, md_start_datetime, md_is_current)
                VAlUES (0, -1, ''EMPTY'', 0, 0, @aul_window_to_time, 1)

                SET IDENTITY_INSERT [silver].[d_seg_segment] OFF

            END

            /*****************************/

            SELECT
                [d_seg_source_id]
                ,[d_seg_name]
                ,[d_seg_length_m]
                ,[d_seg_speed_limit]
            INTO #source
            FROM (
                SELECT DISTINCT
                    fs_csc.segment_id                                                                            AS [d_seg_source_id]
                    , fs_csc.segment_name                                                                        AS [d_seg_name]
                    , fs_csc.segment_length_m                                                                    AS [d_seg_length_m]
                    , fs_csc.speed_limit_kmh                                                                     AS [d_seg_speed_limit]
                    , ROW_NUMBER() OVER (PARTITION BY fs_csc.segment_id ORDER BY fs_csc.md_insert_datetime DESC) AS rn
                FROM [bronze].[fs_car_speed_catches] AS fs_csc
                        LEFT JOIN [meta].[alf_audit_loaded_files] AS alf
                                  ON alf.alf_file_path = fs_csc.md_file_path
                WHERE fs_csc.md_insert_datetime >= @aul_window_from_time
                     AND fs_csc.md_insert_datetime <= @aul_window_to_time
                     AND alf.alf_status = ''success''
            ) AS s
            WHERE rn = 1

            SELECT
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
                SELECT
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
                SELECT
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
            FROM #to_merge_source s
            JOIN @actions a ON s.d_seg_source_id = a.updated_id AND a.action_name = ''UPDATE''

            /*********** AUDIT ***********/

            COMMIT TRANSACTION;

            SET @count_loaded_rows = ISNULL((SELECT COUNT(*) FROM (
                SELECT DISTINCT
                    fs_csc.segment_id           AS [d_seg_source_id]
                    , fs_csc.segment_name       AS [d_seg_name]
                    , fs_csc.segment_length_m   AS [d_seg_length_m]
                    , fs_csc.speed_limit_kmh    AS [d_seg_speed_limit]
                FROM [bronze].[fs_car_speed_catches] AS fs_csc
                WHERE md_insert_datetime >= @aul_window_from_time
                    AND md_insert_datetime <= @aul_window_to_time
            ) AS s), 0)

            SET @count_inserted_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''INSERT'')
                + (SELECT COUNT(1) FROM #source s JOIN @actions a ON s.d_seg_source_id = a.updated_id AND a.action_name = ''UPDATE'')

            SET @count_updated_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''UPDATE'' )

            SET @count_rejected_rows = (SELECT COUNT(*) FROM #incorrect_source)

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
        END TRY
        BEGIN CATCH;
            DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();

            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;

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
                , @aul_error_message = @error_message;
            /*****************************/

            THROW;
        END CATCH
    END

')

IF NOT EXISTS(SELECT 1 FROM sys.procedures WHERE name = 'load_d_veh_vehicle')
EXEC('
    CREATE PROCEDURE [silver].[load_d_veh_vehicle] (
        @aul_window_from_time DATETIME
        ,@aul_window_to_time DATETIME
        ,@aul_pipeline_name SYSNAME
        ,@aul_logical_date DATETIME
        ,@aul_batch_id VARCHAR(100)
    ) AS
    BEGIN
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

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
            @aul_procedure_name = ''silver.load_d_veh_vehicle'',
            @aul_source_schema_name = ''bronze'',
            @aul_source_table_name = ''fs_car_speed_catches'',
            @aul_target_schema_name = ''silver'',
            @aul_target_table_name = ''d_veh_vehicle'',
            @aul_logical_date = @aul_logical_date,
            @aul_window_from_time = @aul_window_from_time,
            @aul_window_to_time = @aul_window_to_time,
            @aul_batch_id = @aul_batch_id;
        /*****************************/

        BEGIN TRANSACTION;
        BEGIN TRY;

            DROP TABLE IF EXISTS #raw_source
            DROP TABLE IF EXISTS #source
            DROP TABLE IF EXISTS #incorrect_source
            DROP TABLE IF EXISTS #correct_source
            DROP TABLE IF EXISTS #to_merge_source

            /***** BLANK ROW INSERT ******/

            IF NOT EXISTS (SELECT 1 FROM [silver].[d_veh_vehicle] WHERE d_veh_id = 0)
            BEGIN

                SET IDENTITY_INSERT [silver].[d_veh_vehicle] ON

                INSERT INTO [silver].[d_veh_vehicle] (d_veh_id, d_veh_plate_number, d_vet_id)
                VAlUES (0, ''EMPTY'', 0)

                SET IDENTITY_INSERT [silver].[d_veh_vehicle] OFF

            END

            /*****************************/

            SELECT
                REPLACE(plate_number, '' '', '''')                                                          AS [d_veh_plate_number]
                , vehicle_type                                                                          AS [d_veh_vehicle_type]
                , ROW_NUMBER() OVER (PARTITION BY REPLACE(plate_number, '' '', '''') ORDER BY fs_csc.md_insert_datetime DESC) AS rn
            INTO #raw_source
            FROM [bronze].[fs_car_speed_catches] AS fs_csc
            LEFT JOIN [meta].[alf_audit_loaded_files] AS alf
                ON alf.alf_file_path = fs_csc.md_file_path
            WHERE
                fs_csc.md_insert_datetime >= @aul_window_from_time
                AND fs_csc.md_insert_datetime <= @aul_window_to_time
                AND alf.alf_status = ''success''

            SELECT
                [d_veh_plate_number]
                ,[d_veh_vehicle_type]
            INTO #source
            FROM #raw_source AS s
            WHERE rn = 1

            SELECT *
            INTO #incorrect_source
            FROM #source
            WHERE [d_veh_plate_number] IS NULL
                OR LEN([d_veh_plate_number]) != 7
                OR SUBSTRING([d_veh_plate_number], 0, 3) NOT IN (SELECT d_lpp_prefix FROM d_lpp_license_plate_prefixes)


            SELECT *
            INTO #correct_source
            FROM (
                SELECT * FROM #source

                EXCEPT

                SELECT * FROM #incorrect_source
            ) AS s

            SELECT *
            INTO #to_merge_source
            FROM (
                SELECT
                    [d_veh_plate_number]
                    ,[d_vet_id]
                FROM #correct_source AS veh
                LEFT JOIN silver.d_vet_vehicle_type AS vet ON vet.d_vet_name = veh.d_veh_vehicle_type

                EXCEPT

                SELECT
                    [d_veh_plate_number]
                    ,[d_vet_id]
                FROM [silver].[d_veh_vehicle]
            ) AS s

            MERGE [silver].[d_veh_vehicle] AS t
            USING #to_merge_source AS s
            ON t.d_veh_plate_number = s.d_veh_plate_number
            WHEN MATCHED THEN UPDATE SET
                t.d_vet_id = s.d_vet_id
            WHEN NOT MATCHED THEN INSERT (
                d_veh_plate_number
                , d_vet_id
            ) VALUES (
                s.d_veh_plate_number
                , s.d_vet_id
            )
            OUTPUT
                $action     AS [action_name]
                , NULL      AS [updated_id]
            INTO @actions (action_name, updated_id);



            /*********** AUDIT ***********/

            COMMIT TRANSACTION;

            SET @count_loaded_rows = (SELECT COUNT(*) FROM #raw_source)

            SET @count_inserted_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''INSERT'')

            SET @count_updated_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''UPDATE'')

            SET @count_rejected_rows = (SELECT COUNT(*) FROM #incorrect_source)

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
        END TRY
        BEGIN CATCH;
            DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();

            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;

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
                , @aul_error_message = @error_message;
            /*****************************/

            THROW;
        END CATCH
    END

    ')

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_f_spc_speed_catch')
EXEC('
CREATE PROCEDURE [silver].[load_f_spc_speed_catch] (
        @aul_window_from_time DATETIME
        ,@aul_window_to_time DATETIME
        ,@aul_pipeline_name SYSNAME
        ,@aul_logical_date DATETIME
        ,@aul_batch_id VARCHAR(100)
    ) AS
    BEGIN
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

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
            @aul_procedure_name = ''silver.load_f_spc_speed_catch'',
            @aul_source_schema_name = ''bronze'',
            @aul_source_table_name = ''fs_car_speed_catches'',
            @aul_target_schema_name = ''silver'',
            @aul_target_table_name = ''f_spc_speed_catch'',
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
            DROP TABLE IF EXISTS #to_insert_source

            SELECT
                entry_timestamp
                , exit_timestamp
                , segment_id
                , REPLACE(plate_number, '' '', '''') AS plate_number
                , alf.alf_id
            INTO #source
            FROM [bronze].[fs_car_speed_catches] AS fs_csc
            LEFT JOIN [meta].[alf_audit_loaded_files] AS alf
                ON alf.alf_file_path = fs_csc.md_file_path
            WHERE
                fs_csc.md_insert_datetime >= @aul_window_from_time
                AND fs_csc.md_insert_datetime <= @aul_window_to_time
                AND alf.alf_status = ''success''
                -- CHEAPER METHOD THAN MERGE WITH [f_spc_entry_timestamp], [f_spc_exit_timestamp], [d_seg_id], [d_veh_id]
                AND alf.alf_id NOT IN (SELECT DISTINCT md_alf_id FROM [silver].[f_spc_speed_catch])

            SELECT
                *
            INTO #incorrect_source
            FROM #source
            WHERE entry_timestamp IS NULL
                OR exit_timestamp IS NULL
                OR DATEDIFF(SECOND, entry_timestamp, exit_timestamp) <= 0

            SELECT *
            INTO #correct_source
            FROM (
                SELECT * FROM #source

                EXCEPT

                SELECT * FROM #incorrect_source
            ) AS s

            SELECT *
            INTO #to_insert_source
            FROM (
                SELECT
                    entry_timestamp                 AS [f_spc_entry_timestamp]
                    , exit_timestamp                AS [f_spc_exit_timestamp]
                    , CAST(
                        seg.d_seg_length_m / 1000.0 / (DATEDIFF(SECOND, spc.entry_timestamp, spc.exit_timestamp) / 3600.0
                    ) AS DECIMAL(6,2))              AS [f_spc_speed_km_h]
                    , CAST(
                        DATEDIFF(SECOND, entry_timestamp, exit_timestamp)
                    AS INT)                       AS [f_spc_duration_sec]
                    , ISNULL(seg.d_seg_id, 0)       AS [d_seg_id]
                    , ISNULL(veh.d_veh_id, 0)       AS [d_veh_id]
                    , alf_id                        AS [md_alf_id]
                FROM #correct_source AS spc
                LEFT JOIN silver.d_veh_vehicle AS veh ON veh.d_veh_plate_number = spc.plate_number
                LEFT JOIN silver.d_seg_segment AS seg ON seg.d_seg_source_id = spc.segment_id
            ) AS s


            INSERT INTO [silver].[f_spc_speed_catch] (
                f_spc_entry_timestamp
                , f_spc_exit_timestamp
                , f_spc_speed_km_h
                , f_spc_duration_sec
                , d_seg_id
                , d_veh_id
                , md_alf_id
            ) SELECT * FROM #to_insert_source


            /*********** AUDIT ***********/

            COMMIT TRANSACTION;

            SET @count_loaded_rows = (SELECT COUNT(*) FROM #source)

            SET @count_inserted_rows = (SELECT COUNT(*) FROM @actions WHERE action_name = ''INSERT'')

            SET @count_rejected_rows = (SELECT COUNT(*) FROM #incorrect_source)

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
        END TRY
        BEGIN CATCH;
            DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();

            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;

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
                , @aul_error_message = @error_message;
            /*****************************/

            THROW;
        END CATCH
    END
    ')

IF NOT EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'aggregate_a_swr_seg_weekly_ranking')
EXEC('
CREATE PROCEDURE [gold].[aggregate_a_swr_seg_weekly_ranking](
    @aua_logical_date DATETIME
    , @aua_from_datetime DATETIME
    , @aua_to_datetime DATETIME
    , @aua_batch_id VARCHAR(100)
    , @aua_pipeline_name SYSNAME
) AS
    BEGIN
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        /*********** AUDIT ***********/
        DECLARE @aggregate_id BIGINT;
        DECLARE @count_rows INT = 0;

        EXECUTE [meta].[load_aua_audit_aggregate]
            @aua_action = ''started'',
            @aua_id = @aggregate_id OUTPUT,
            @aua_target_schema_name = ''gold'',
            @aua_target_table_name = ''a_swr_seg_weekly_ranking'',
            @aua_procedure_name = ''gold.aggregate_a_swr_seg_weekly_ranking'',
            @aua_logical_date = @aua_logical_date,
            @aua_from_datetime = @aua_from_datetime,
            @aua_to_datetime = @aua_to_datetime,
            @aua_batch_id = @aua_batch_id,
            @aua_pipeline_name = @aua_pipeline_name;
        /*****************************/

        BEGIN TRANSACTION;
        BEGIN TRY

        TRUNCATE TABLE [gold].[a_swr_seg_weekly_ranking];

        ;WITH weekly_base AS (
            SELECT
                f.d_seg_id
                , CONCAT(
                    DATEPART(YEAR, f.f_spc_entry_timestamp)
                    , ''-W'',
                    RIGHT(''0'' + CAST(DATEPART(ISO_WEEK, f.f_spc_entry_timestamp) AS VARCHAR(2)), 2)
                  ) AS year_week
                , COUNT(*) AS total_crossings
                , SUM(IIF(f.f_spc_speed_km_h > d.d_seg_speed_limit, 1, 0)) AS speeding_count
            FROM silver.f_spc_speed_catch f
            JOIN silver.d_seg_segment d ON f.d_seg_id = d.d_seg_id
            WHERE d.d_seg_id != 0
                AND f.f_spc_entry_timestamp >= @aua_from_datetime
                AND f.f_spc_entry_timestamp < @aua_to_datetime
            GROUP BY
                f.d_seg_id
                , DATEPART(YEAR, f.f_spc_entry_timestamp)
                , DATEPART(ISO_WEEK, f.f_spc_entry_timestamp)
        )
        , weekly_pct AS (
            SELECT
                d_seg_id, year_week, total_crossings, speeding_count
                , CAST(speeding_count * 100.0 / total_crossings AS DECIMAL(5,2)) AS speeding_pct
            FROM weekly_base
        )

        INSERT INTO [gold].[a_swr_seg_weekly_ranking] (
            d_seg_id
            , a_swr_year_week
            , a_swr_total_crossings
            , a_swr_over_speeding_count
            , a_swr_over_speeding_percent
            , a_swr_rank_position
        )
        SELECT
            d_seg_id
            , year_week
            , total_crossings
            , speeding_count
            , speeding_pct
            , RANK() OVER (PARTITION BY year_week ORDER BY speeding_pct DESC) AS rank_position
        FROM weekly_pct;

        SET @count_rows = @@ROWCOUNT;

        /*********** AUDIT ***********/

        COMMIT TRANSACTION;

        EXECUTE [meta].[load_aua_audit_aggregate]
            @aua_action = ''finished''
            , @aua_id = @aggregate_id
            , @aua_count_rows = @count_rows
            , @aua_status = ''success''
        /*****************************/

        END TRY
        BEGIN CATCH;
            DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();

            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;

            /*********** AUDIT ***********/
            EXECUTE [meta].[load_aua_audit_aggregate]
                @aua_action = ''finished''
                , @aua_id = @aggregate_id
                , @aua_count_rows = 0
                , @aua_status = ''failed''
                , @aua_error_message = @error_message;
            /*****************************/

            THROW;
        END CATCH
    END
    ')