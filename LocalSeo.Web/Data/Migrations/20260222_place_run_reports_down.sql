IF OBJECT_ID('dbo.PlaceRunReports','U') IS NULL
  RETURN;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceRunReports_Unique' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  DROP INDEX UX_PlaceRunReports_Unique ON dbo.PlaceRunReports;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceRunReports_RunId' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  DROP INDEX IX_PlaceRunReports_RunId ON dbo.PlaceRunReports;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceRunReports_ReportTypeVariant' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  DROP INDEX IX_PlaceRunReports_ReportTypeVariant ON dbo.PlaceRunReports;

IF OBJECT_ID('dbo.FK_PlaceRunReports_User','F') IS NOT NULL
  ALTER TABLE dbo.PlaceRunReports DROP CONSTRAINT FK_PlaceRunReports_User;

IF OBJECT_ID('dbo.FK_PlaceRunReports_SearchRun','F') IS NOT NULL
  ALTER TABLE dbo.PlaceRunReports DROP CONSTRAINT FK_PlaceRunReports_SearchRun;

IF OBJECT_ID('dbo.FK_PlaceRunReports_Place','F') IS NOT NULL
  ALTER TABLE dbo.PlaceRunReports DROP CONSTRAINT FK_PlaceRunReports_Place;

DROP TABLE dbo.PlaceRunReports;
