IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'car_speed_recognizer')
BEGIN
  CREATE DATABASE car_speed_recognizer;
END;
GO

USE car_speed_recognizer

SET QUOTED_IDENTIFIER ON


/*
    Creating schema objects
*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
EXEC('CREATE SCHEMA bronze');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
EXEC('CREATE SCHEMA silver');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
EXEC('CREATE SCHEMA gold');

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')
EXEC('CREATE SCHEMA meta');
