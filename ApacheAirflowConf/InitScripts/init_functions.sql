USE car_speed_recognizer;

IF NOT EXISTS (SELECT OBJECT_ID(N'meta.ths_tables_have_same_shape', N'FN'))
EXEC('
    CREATE FUNCTION meta.ths_tables_have_same_shape
(
    @source_schema SYSNAME,
    @source_table  SYSNAME,
    @target_schema SYSNAME,
    @target_table  SYSNAME
)
RETURNS BIT
AS
BEGIN
    DECLARE @Result BIT = 1;

    IF OBJECT_ID(QUOTENAME(@source_schema) + ''.'' + QUOTENAME(@source_table), ''U'') IS NULL
        RETURN 0;

    IF OBJECT_ID(QUOTENAME(@target_schema) + ''.'' + QUOTENAME(@target_table), ''U'') IS NULL
        RETURN 0;

    IF EXISTS
    (
        SELECT
            s.column_id,
            s.name,
            st.name       AS system_type_name,
            s.max_length,
            s.precision,
            s.scale,
            s.is_nullable,
            s.is_computed
        FROM sys.columns s
        INNER JOIN sys.types st
            ON s.user_type_id = st.user_type_id
        WHERE s.object_id = OBJECT_ID(QUOTENAME(@source_schema) + ''.'' + QUOTENAME(@source_table))

        EXCEPT

        SELECT
            t.column_id,
            t.name,
            tt.name       AS system_type_name,
            t.max_length,
            t.precision,
            t.scale,
            t.is_nullable,
            t.is_computed
        FROM sys.columns t
        INNER JOIN sys.types tt
            ON t.user_type_id = tt.user_type_id
        WHERE t.object_id = OBJECT_ID(QUOTENAME(@target_schema) + ''.'' + QUOTENAME(@target_table))
    )
    OR EXISTS
    (
        SELECT
            t.column_id,
            t.name,
            tt.name       AS system_type_name,
            t.max_length,
            t.precision,
            t.scale,
            t.is_nullable,
            t.is_computed
        FROM sys.columns t
        INNER JOIN sys.types tt
            ON t.user_type_id = tt.user_type_id
        WHERE t.object_id = OBJECT_ID(QUOTENAME(@target_schema) + ''.'' + QUOTENAME(@target_table))

        EXCEPT

        SELECT
            s.column_id,
            s.name,
            st.name       AS system_type_name,
            s.max_length,
            s.precision,
            s.scale,
            s.is_nullable,
            s.is_computed
        FROM sys.columns s
        INNER JOIN sys.types st
            ON s.user_type_id = st.user_type_id
        WHERE s.object_id = OBJECT_ID(QUOTENAME(@source_schema) + ''.'' + QUOTENAME(@source_table))
    )
    BEGIN
        SET @Result = 0;
    END

    RETURN @Result;
END;
GO
    ')
