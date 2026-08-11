USE car_speed_recognizer

SET QUOTED_IDENTIFIER ON

IF NOT EXISTS(SELECT 1 FROM sys.views WHERE name = 'V_Segment_Speeding_Live')
EXEC('
    CREATE VIEW [gold].[V_Segment_Speeding_Live] AS
    SELECT
        d_seg.d_seg_name                                    AS [Segment_Name]
        , d_seg.d_seg_speed_limit                           AS [Segment_Speed_Limit]
        , f_spc.f_spc_entry_timestamp                       AS [Speed_Catch_Entry_Timestamp]
        , f_spc.f_spc_speed_km_h                            AS [Speed_Catch_Speed_Km_H]
        , f_spc.f_spc_speed_km_h - d_seg.d_seg_speed_limit  AS [Over_Speed_Km_H]
        , d_veh.d_veh_plate_number                          AS [Vehicle_Plate_Number]
    FROM [silver].[f_spc_speed_catch] AS f_spc
    JOIN [silver].[d_seg_segment] AS d_seg ON f_spc.d_seg_id = d_seg.d_seg_id
    JOIN [silver].[d_veh_vehicle] AS d_veh ON f_spc.d_veh_id = d_veh.d_veh_id
    WHERE f_spc.f_spc_speed_km_h > d_seg.d_seg_speed_limit
        AND d_veh.d_veh_plate_number != ''EMPTY''
        AND d_seg.d_seg_name != ''EMPTY''
')

IF NOT EXISTS(SELECT 1 FROM sys.views WHERE name = 'MV_Segment_Daily_Stats')
BEGIN

    EXEC('CREATE VIEW [gold].[MV_Segment_Daily_Stats] WITH SCHEMABINDING AS
    SELECT
        d_seg.d_seg_name AS [Segment_Name]
        , CAST(f_spc.f_spc_entry_timestamp AS DATE) AS [Speed_Catch_Entry_Date]
        , COUNT_BIG(*) AS [Total_Crossing]
        , SUM(ISNULL(CAST(f_spc.f_spc_speed_km_h AS BIGINT), 0)) AS [Sum_Speed_Km_H]
    FROM [silver].[f_spc_speed_catch] AS f_spc
    JOIN [silver].[d_seg_segment] AS d_seg ON f_spc.d_seg_id = d_seg.d_seg_id
    GROUP BY
        d_seg.d_seg_name
        , CAST(f_spc.f_spc_entry_timestamp AS DATE)
    ')

    IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name = 'IX_seg_daily_mat')
    CREATE UNIQUE CLUSTERED INDEX IX_seg_daily_mat
    ON gold.MV_Segment_Daily_Stats (Segment_Name, Speed_Catch_Entry_Date)

END

IF NOT EXISTS(SELECT 1 FROM sys.views WHERE name = 'AV_Segment_Weekly_Ranking')
EXEC('CREATE VIEW [gold].[AV_Segment_Weekly_Ranking] AS
SELECT
    a_swr_year_week AS [Year_Week]
    , a_swr_total_crossings AS [Total_Crossings]
    , a_swr_over_speeding_count AS [Over_Speed_Counts]
    , a_swr_over_speeding_percent AS [Over_Speed_Counts_Percent]
    , a_swr_rank_position AS [Rank_Segment_Per_Year_Week]
FROM [gold].[a_swr_seg_weekly_ranking]')