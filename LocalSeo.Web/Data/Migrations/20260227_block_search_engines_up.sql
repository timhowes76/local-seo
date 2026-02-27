IF OBJECT_ID('dbo.AppSettings', 'U') IS NULL
BEGIN
    RETURN;
END;

IF COL_LENGTH('dbo.AppSettings', 'BlockSearchEngines') IS NULL
    ALTER TABLE dbo.AppSettings ADD BlockSearchEngines bit NOT NULL CONSTRAINT DF_AppSettings_BlockSearchEngines_Migration DEFAULT(0);
