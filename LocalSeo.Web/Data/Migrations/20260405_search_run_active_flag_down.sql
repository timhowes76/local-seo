IF COL_LENGTH('dbo.SearchRun', 'IsActive') IS NOT NULL
  ALTER TABLE dbo.SearchRun DROP COLUMN IsActive;
GO
