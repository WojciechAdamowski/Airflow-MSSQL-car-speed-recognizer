USE car_speed_recognizer


/*
    Creating meta schema objects
*/
IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_aul_audit_load')
EXEC('
    CREATE OR ALTER PROCEDURE [meta].[load_aul_audit_load]
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

/*
    Creating silver schema objects
*/


IF NOt EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'load_d_vet_vehicle_type')
EXEC('
    CREATE OR ALTER PROCEDURE [silver].[load_d_vet_vehicle_type] (
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



            MERGE [silver].[d_vet_vehicle_type] AS t
            USING (

                SELECT
                    DISTINCT vehicle_type AS [d_vet_name]
                FROM [bronze].[fs_car_speed_catches]
                WHERE md_insert_time >= @aul_window_from_time AND md_insert_time <= @aul_window_to_time

            ) AS s
            ON t.d_vet_name = s.d_vet_name
            WHEN NOT MATCHED THEN INSERT (
                d_vet_name
            ) VALUES (
                s.d_vet_name
            )
            OUTPUT $action INTO @actions;


            /*********** AUDIT ***********/
            SET @count_loaded_rows = (SELECT COUNT(*) FROM [bronze].[fs_car_speed_catches])
            SET @count_inserted_rows = (SELECT SUM(IIF(action_name = ''INSERT'', 1, 0)) FROM @actions)

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
