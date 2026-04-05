IF COL_LENGTH('dbo.SearchRun', 'IsActive') IS NULL
  ALTER TABLE dbo.SearchRun ADD IsActive bit NOT NULL CONSTRAINT DF_SearchRun_IsActive_Migration DEFAULT(1);
GO

UPDATE dbo.SearchRun
SET IsActive = 1
WHERE IsActive IS NULL;
GO
