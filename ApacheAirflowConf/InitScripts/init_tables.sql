USE car_speed_recognizer

/*
    Creating meta schema objects
*/

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
    md_insert_time          datetime            NOT NULL DEFAULT GETDATE()
);

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
    md_insert_time      DATETIME DEFAULT GETDATE(),
    md_batch_id         NVARCHAR(100),
    md_file_name        NVARCHAR(100)
)

/*
    Creating silver schema objects
*/
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_seg_segment')
CREATE TABLE [silver].[d_seg_segment] (
    d_seg_id INT PRIMARY KEY IDENTITY
    , d_seg_source_id INT NOT NULL
    , d_seg_name NVARCHAR(100)
    , d_seg_length_m INT
    , d_seg_speed_limit INT
    , md_insert_time DATETIME DEFAULT GETDATE()
    , md_update_time DATETIME
    , md_start_time DATETIME
    , md_end_date DATETIME
    , md_is_current BIT
)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_vet_vehicle_type')
CREATE TABLE [silver].[d_vet_vehicle_type] (
    d_vet_id INT PRIMARY KEY IDENTITY
    , d_vet_name NVARCHAR(100)
    , md_insert_time DATETIME DEFAULT GETDATE()
)

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'd_lpp_license_plate_prefixes')
CREATE TABLE [silver].[d_lpp_license_plate_prefixes]  (
    d_lpp_id                  INT PRIMARY KEY IDENTITY,
    d_lpp_prefix              VARCHAR(3) NOT NULL,
    d_lpp_voivodeship         NVARCHAR(50) NOT NULL,
    d_lpp_registration_place  NVARCHAR(100) NOT NULL,
    d_lpp_unit_type           NVARCHAR(50) NOT NULL,
    md_insert_time            DATETIME DEFAULT GETDATE()
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
