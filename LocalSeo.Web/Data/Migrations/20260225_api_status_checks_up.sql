IF OBJECT_ID('dbo.ApiStatusCheckDefinition','U') IS NULL
BEGIN
  CREATE TABLE dbo.ApiStatusCheckDefinition(
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Key] nvarchar(100) NOT NULL,
    DisplayName nvarchar(200) NOT NULL,
    Category nvarchar(100) NOT NULL,
    IsEnabled bit NOT NULL CONSTRAINT DF_ApiStatusCheckDefinition_IsEnabled DEFAULT(1),
    IntervalSeconds int NOT NULL CONSTRAINT DF_ApiStatusCheckDefinition_IntervalSeconds DEFAULT(300),
    TimeoutSeconds int NOT NULL CONSTRAINT DF_ApiStatusCheckDefinition_TimeoutSeconds DEFAULT(10),
    DegradedThresholdMs int NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_ApiStatusCheckDefinition_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_ApiStatusCheckDefinition_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID('dbo.ApiStatusCheckResult','U') IS NULL
BEGIN
  CREATE TABLE dbo.ApiStatusCheckResult(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DefinitionId int NOT NULL,
    CheckedUtc datetime2(3) NOT NULL,
    [Status] tinyint NOT NULL CONSTRAINT DF_ApiStatusCheckResult_Status DEFAULT(0),
    LatencyMs int NULL,
    [Message] nvarchar(500) NULL,
    DetailsJson nvarchar(max) NULL,
    HttpStatusCode int NULL,
    ErrorType nvarchar(200) NULL,
    ErrorMessage nvarchar(1000) NULL
  );
END;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.ApiStatusCheckResult')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.ApiStatusCheckDefinition')
    AND pc.name = 'DefinitionId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.ApiStatusCheckResult WITH CHECK
  ADD CONSTRAINT FK_ApiStatusCheckResult_Definition
  FOREIGN KEY (DefinitionId) REFERENCES dbo.ApiStatusCheckDefinition(Id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_ApiStatusCheckDefinition_Key' AND object_id=OBJECT_ID('dbo.ApiStatusCheckDefinition'))
  CREATE UNIQUE INDEX UX_ApiStatusCheckDefinition_Key ON dbo.ApiStatusCheckDefinition([Key]);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ApiStatusCheckResult_DefinitionId_CheckedUtc' AND object_id=OBJECT_ID('dbo.ApiStatusCheckResult'))
  CREATE INDEX IX_ApiStatusCheckResult_DefinitionId_CheckedUtc
    ON dbo.ApiStatusCheckResult(DefinitionId, CheckedUtc DESC)
    INCLUDE([Status], LatencyMs, [Message]);

