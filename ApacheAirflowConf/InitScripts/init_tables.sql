USE car_speed_recognizer

/*
    Creating meta schema objects
*/

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'aua_audit_aggregate')
CREATE TABLE [meta].[aua_audit_aggregate](
    aua_id INT IDENTITY(1, 1) PRIMARY KEY
    , aua_target_schema_name  SYSNAME NOT NULL
    , aua_target_table_name   SYSNAME NOT NULL
    , aua_procedure_name      SYSNAME NOT NULL
    , aua_run_start_time      DATETIME DEFAULT GETDATE()
    , aua_run_end_time        DATETIME NULL
    , aua_count_rows          INT NULL
    , aua_status              VARCHAR(20) NOT NULL
    , aua_error_message       NVARCHAR(4000) NULL
    , aua_logical_date        DATETIME NULL
    , aua_from_datetime       DATETIME NULL
    , aua_to_datetime         DATETIME NULL
    , aua_batch_id            VARCHAR(100) NULL
    , aua_pipeline_name       SYSNAME
    , md_insert_datetime      DATETIME DEFAULT GETDATE()
)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'aul_audit_load')
CREATE TABLE meta.aul_audit_load (
    aul_load_id             bigint IDENTITY(1,1) PRIMARY KEY,
    aul_pipeline_name       sysname             NULL,
    aul_procedure_name      sysname             NOT NULL,
    aul_source_schema_name  sysname             NULL,
    aul_source_table_name   sysname             NOT NULL,
    aul_target_schema_name  sysname             NULL,
    aul_target_table_name   sysname             NOT NULL,

    aul_run_start_time      datetime            NOT NULL,
    aul_run_end_time        datetime            NULL,
    aul_logical_date        datetime            NULL,
    aul_window_from_time    datetime            NULL,
    aul_window_to_time      datetime            NULL,

    aul_count_loaded_rows   int                 NULL,
    aul_count_inserted_rows int                 NULL,
    aul_count_updated_rows  int                 NULL,
    aul_count_deleted_rows  int                 NULL,
    aul_count_rejected_rows int                 NULL,

    aul_status              varchar(20)         NOT NULL,
    aul_error_message       nvarchar(4000)      NULL,
    aul_batch_id            varchar(100)        NULL,
    md_insert_datetime      datetime            NOT NULL DEFAULT GETDATE()
);

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'alf_audit_loaded_files')
CREATE TABLE [meta].[alf_audit_loaded_files] (
    alf_id                      INT IDENTITY(1,1) PRIMARY KEY
    , alf_batch_id              VARCHAR(500)    NOT NULL
    , alf_pipeline_name         VARCHAR(500)    NOT NULL
    , alf_source_rows           INT
    , alf_rejected_rows         INT
    , alf_file_size_bytes       BIGINT
    , alf_file_path             VARCHAR(500)    NOT NULL
    , alf_status                VARCHAR(100)    NOT NULL
    , alf_error_message         VARCHAR(1000)
    , alf_load_start_datetime   DATETIME
    , alf_load_end_datetime     DATETIME
    , alf_source_system         VARCHAR(200)
    , alf_target_schema         SYSNAME
    , alf_target_table          SYSNAME
    , md_insert_datetime        DATETIME DEFAULT GETDATE()
)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'aup_audit_process')
CREATE TABLE [meta].[aup_audit_process] (
    aup_id  INT IDENTITY(1, 1) PRIMARY KEY
    , aup_batch_id  VARCHAR(500) NOT NULL
    , aup_pipeline_name VARCHAR(500) NOT NULL
    , aup_start_datetime DATETIME
    , aup_end_datetime DATETIME
    , aup_duration_s AS DATEDIFF(SECOND, aup_start_datetime, aup_end_datetime)
    , aup_status NVARCHAR(20)
    , aup_error_message NVARCHAR(MAX)
    , md_insert_datetime DATETIME DEFAULT GETDATE()
    , md_update_datetime DATETIME
)

/*
    Creating bronze schema objects
*/

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'fs_car_speed_catches')
CREATE TABLE [bronze].[fs_car_speed_catches] (
    entry_timestamp     DATETIME,
    exit_timestamp      DATETIME,
    segment_id          INT,
    segment_name        NVARCHAR(300),
    segment_length_m    INT,
    speed_limit_kmh     INT,
    plate_number        NVARCHAR(50),
    vehicle_type        NVARCHAR(50),
    md_insert_datetime  DATETIME DEFAULT GETDATE(),
    md_ingestion_date   DATE DEFAULT (CAST(GETDATE() AS DATE)),
    md_batch_id         NVARCHAR(100),
    md_file_path        NVARCHAR(500)
)

/*
    Creating silver schema objects
*/
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'f_spc_speed_catch')
CREATE TABLE [silver].[f_spc_speed_catch] (
    f_spc_id INT IDENTITY(1, 1) PRIMARY KEY
    , f_spc_entry_timestamp DATETIME NOT NULL
    , f_spc_exit_timestamp DATETIME NOT NULL
    , f_spc_duration_sec AS DATEDIFF(SECOND, f_spc_entry_timestamp, f_spc_exit_timestamp) PERSISTED
    , f_spc_speed_km_h DECIMAL(6,2)
    , d_seg_id INT NOT NULL
    , d_veh_id INT NOT NULL
    , md_alf_id INT NOT NULL
    , md_insert_datetime DATETIME DEFAULT GETDATE()
)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_spc_seg_entry')
CREATE INDEX IX_spc_seg_entry ON silver.f_spc_speed_catch (d_seg_id, f_spc_entry_timestamp)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_spc_veh')
CREATE INDEX IX_spc_veh ON silver.f_spc_speed_catch (d_veh_id)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_veh_vehicle')
CREATE TABLE [silver].[d_veh_vehicle] (
    d_veh_id INT IDENTITY(1,1) PRIMARY KEY
    , d_veh_plate_number VARCHAR(7) NOT NULL
    , d_vet_id INT
    , md_insert_datetime DATETIME DEFAULT GETDATE()
    , md_update_datetime DATETIME
)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_veh_plate')
CREATE UNIQUE NONCLUSTERED INDEX UX_veh_plate
ON silver.d_veh_vehicle (d_veh_plate_number)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_seg_segment')
CREATE TABLE [silver].[d_seg_segment] (
    d_seg_id INT PRIMARY KEY IDENTITY
    , d_seg_source_id INT NOT NULL
    , d_seg_name NVARCHAR(100)
    , d_seg_length_m INT
    , d_seg_speed_limit INT
    , md_insert_datetime DATETIME DEFAULT GETDATE()
    , md_update_datetime DATETIME
    , md_start_datetime DATETIME
    , md_end_datetime DATETIME
    , md_is_current BIT
)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_seg_source_current')
CREATE NONCLUSTERED INDEX IX_seg_source_current
ON silver.d_seg_segment (d_seg_source_id, md_is_current)
INCLUDE (d_seg_id)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_vet_vehicle_type')
CREATE TABLE [silver].[d_vet_vehicle_type] (
    d_vet_id INT PRIMARY KEY IDENTITY
    , d_vet_name NVARCHAR(100)
    , md_insert_datetime DATETIME DEFAULT GETDATE()
)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_vet_name')
CREATE UNIQUE NONCLUSTERED INDEX UX_vet_name
ON silver.d_vet_vehicle_type (d_vet_name)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_lpp_license_plate_prefixes')
CREATE TABLE [silver].[d_lpp_license_plate_prefixes]  (
    d_lpp_id                  INT PRIMARY KEY IDENTITY,
    d_lpp_prefix              VARCHAR(3) NOT NULL,
    d_lpp_voivodeship         NVARCHAR(50) NOT NULL,
    d_lpp_registration_place  NVARCHAR(100) NOT NULL,
    d_lpp_unit_type           NVARCHAR(50) NOT NULL,
    md_insert_datetime        DATETIME DEFAULT GETDATE()
)

IF 0 = (SELECT COUNT(1) FROM [silver].[d_lpp_license_plate_prefixes])
INSERT INTO [silver].[d_lpp_license_plate_prefixes] (
   d_lpp_prefix
  , d_lpp_voivodeship
  , d_lpp_registration_place
  , d_lpp_unit_type
) VALUES
('WA', N'Mazowieckie', N'Warszawa (Białołęka)', N'Warsaw District'),
('WB', N'Mazowieckie', N'Warszawa (Bemowo)', N'Warsaw District'),
('WD', N'Mazowieckie', N'Warszawa (Bielany)', N'Warsaw District'),
('WE', N'Mazowieckie', N'Warszawa (Mokotów)', N'Warsaw District'),
('WF', N'Mazowieckie', N'Warszawa (Praga-Południe)', N'Warsaw District'),
('WH', N'Mazowieckie', N'Warszawa (Praga-Północ)', N'Warsaw District'),
('WI', N'Mazowieckie', N'Warszawa (Śródmieście)', N'Warsaw District'),
('WK', N'Mazowieckie', N'Warszawa (Ursus)', N'Warsaw District'),
('WN', N'Mazowieckie', N'Warszawa (Ursynów)', N'Warsaw District'),
('WT', N'Mazowieckie', N'Warszawa (Wawer)', N'Warsaw District'),
('KR', N'Małopolskie', N'Kraków', N'City with County Status'),
('KK', N'Małopolskie', N'Kraków', N'City with County Status (Alternative)'),
('KN', N'Małopolskie', N'Nowy Sącz', N'City with County Status'),
('PO', N'Wielkopolskie', N'Poznań', N'City with County Status'),
('PY', N'Wielkopolskie', N'Poznań', N'City with County Status (Alternative)'),
('PL', N'Wielkopolskie', N'Leszno', N'City with County Status'),
('DW', N'Dolnośląskie', N'Wrocław', N'City with County Status'),
('GD', N'Pomorskie', N'Gdańsk', N'City with County Status'),
('GA', N'Pomorskie', N'Gdynia', N'City with County Status'),
('LU', N'Lubelskie', N'Lublin', N'City with County Status'),
('RZ', N'Podkarpackie', N'Rzeszów', N'City with County Status'),
('BI', N'Podlaskie', N'Białystok', N'City with County Status'),
('EL', N'Warmińsko-Mazurskie', N'Elbląg', N'City with County Status'),
('CB', N'Kujawsko-Pomorskie', N'Bydgoszcz', N'City with County Status'),
('FG', N'Lubuskie', N'Gorzów Wielkopolski', N'City with County Status'),
('NO', N'Warmińsko-Mazurskie', N'Olsztyn', N'City with County Status'),
('SZ', N'Zachodniopomorskie', N'Szczecin', N'City with County Status'),
('ZG', N'Lubuskie', N'Zielona Góra', N'City with County Status'),
('OP', N'Opolskie', N'Opole', N'City with County Status'),
('PK', N'Wielkopolskie', N'Kalisz', N'City with County Status');

/*
    Creating gold schema objects
*/
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'a_swr_seg_weekly_ranking')
CREATE TABLE [gold].[a_swr_seg_weekly_ranking] (
    d_seg_id INT NOT NULL
    , a_swr_year_week VARCHAR(8) NOT NULL
    , a_swr_total_crossings INT NOT NULL
    , a_swr_over_speeding_count INT NOT NULL
    , a_swr_over_speeding_percent DECIMAL(5, 2) NOT NULL
    , a_swr_rank_position INT NOT NULL
    , md_insert_datetime DATETIME DEFAULT GETDATE()
    , PRIMARY KEY (d_seg_id, a_swr_year_week)
)