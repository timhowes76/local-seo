IF OBJECT_ID('dbo.PlaceRunReports','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceRunReports(
    ReportId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL,
    RunId bigint NOT NULL,
    ReportType nvarchar(50) NOT NULL,
    Variant nvarchar(20) NOT NULL,
    Version int NOT NULL CONSTRAINT DF_PlaceRunReports_Version DEFAULT(1),
    HtmlSnapshotPath nvarchar(260) NULL,
    PdfPath nvarchar(260) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceRunReports_CreatedUtc DEFAULT SYSUTCDATETIME(),
    CreatedByUserId int NULL,
    ContentHash nvarchar(64) NULL
  );
END;

IF OBJECT_ID('dbo.Place','U') IS NOT NULL
   AND NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.PlaceRunReports')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.Place')
    AND pc.name = 'PlaceId'
    AND rc.name = 'PlaceId'
)
  ALTER TABLE dbo.PlaceRunReports WITH CHECK ADD CONSTRAINT FK_PlaceRunReports_Place FOREIGN KEY (PlaceId) REFERENCES dbo.Place(PlaceId);

IF OBJECT_ID('dbo.SearchRun','U') IS NOT NULL
   AND NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.PlaceRunReports')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.SearchRun')
    AND pc.name = 'RunId'
    AND rc.name = 'SearchRunId'
)
  ALTER TABLE dbo.PlaceRunReports WITH CHECK ADD CONSTRAINT FK_PlaceRunReports_SearchRun FOREIGN KEY (RunId) REFERENCES dbo.SearchRun(SearchRunId);

IF OBJECT_ID('dbo.[User]','U') IS NOT NULL
   AND NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.PlaceRunReports')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.[User]')
    AND pc.name = 'CreatedByUserId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.PlaceRunReports WITH CHECK ADD CONSTRAINT FK_PlaceRunReports_User FOREIGN KEY (CreatedByUserId) REFERENCES dbo.[User](Id) ON DELETE SET NULL;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceRunReports_Unique' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  CREATE UNIQUE INDEX UX_PlaceRunReports_Unique ON dbo.PlaceRunReports(PlaceId, RunId, ReportType, Variant, Version);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceRunReports_RunId' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  CREATE INDEX IX_PlaceRunReports_RunId ON dbo.PlaceRunReports(RunId);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceRunReports_ReportTypeVariant' AND object_id=OBJECT_ID('dbo.PlaceRunReports'))
  CREATE INDEX IX_PlaceRunReports_ReportTypeVariant ON dbo.PlaceRunReports(ReportType, Variant);
