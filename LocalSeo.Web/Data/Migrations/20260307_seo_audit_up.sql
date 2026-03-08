IF OBJECT_ID('dbo.SeoAuditRule','U') IS NULL
BEGIN
  CREATE TABLE dbo.SeoAuditRule(
    SeoAuditRuleId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    RuleKey nvarchar(120) NOT NULL,
    [Name] nvarchar(200) NOT NULL,
    [Description] nvarchar(1000) NULL,
    RuleGroup nvarchar(100) NULL,
    RuleMode nvarchar(40) NOT NULL CONSTRAINT DF_SeoAuditRule_RuleMode DEFAULT(N'Fixed'),
    RuleType nvarchar(80) NOT NULL,
    EntityType nvarchar(40) NOT NULL,
    Severity nvarchar(40) NOT NULL CONSTRAINT DF_SeoAuditRule_Severity DEFAULT(N'Warning'),
    WarningScoreImpact int NOT NULL CONSTRAINT DF_SeoAuditRule_WarningScoreImpact DEFAULT(0),
    FailScoreImpact int NOT NULL CONSTRAINT DF_SeoAuditRule_FailScoreImpact DEFAULT(0),
    SortOrder int NOT NULL CONSTRAINT DF_SeoAuditRule_SortOrder DEFAULT(0),
    IsActive bit NOT NULL CONSTRAINT DF_SeoAuditRule_IsActive DEFAULT(1),
    ShowInActionsTab bit NOT NULL CONSTRAINT DF_SeoAuditRule_ShowInActionsTab DEFAULT(1),
    WhyItMattersText nvarchar(max) NULL,
    RecommendedActionText nvarchar(max) NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SeoAuditRule_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SeoAuditRule_UpdatedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID('dbo.SeoAuditRuleParameter','U') IS NULL
BEGIN
  CREATE TABLE dbo.SeoAuditRuleParameter(
    SeoAuditRuleParameterId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SeoAuditRuleId bigint NOT NULL,
    ParameterName nvarchar(120) NOT NULL,
    ParameterValue nvarchar(4000) NOT NULL,
    ValueType nvarchar(30) NOT NULL CONSTRAINT DF_SeoAuditRuleParameter_ValueType DEFAULT(N'String'),
    SortOrder int NOT NULL CONSTRAINT DF_SeoAuditRuleParameter_SortOrder DEFAULT(0),
    IsActive bit NOT NULL CONSTRAINT DF_SeoAuditRuleParameter_IsActive DEFAULT(1)
  );
END;

IF OBJECT_ID('dbo.SeoAuditResult','U') IS NULL
BEGIN
  CREATE TABLE dbo.SeoAuditResult(
    SeoAuditResultId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL,
    SeoAuditRuleId bigint NOT NULL,
    [Status] nvarchar(30) NOT NULL,
    ScoreImpactApplied int NOT NULL CONSTRAINT DF_SeoAuditResult_ScoreImpactApplied DEFAULT(0),
    ActualValue nvarchar(1000) NULL,
    ExpectedValue nvarchar(1000) NULL,
    GapValue nvarchar(1000) NULL,
    SummaryText nvarchar(2000) NULL,
    WhyItMattersText nvarchar(max) NULL,
    RecommendedActionText nvarchar(max) NULL,
    SortOrderSnapshot int NOT NULL CONSTRAINT DF_SeoAuditResult_SortOrderSnapshot DEFAULT(0),
    LastSourceSearchRunId bigint NULL,
    LastEvaluatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SeoAuditResult_LastEvaluatedAtUtc DEFAULT SYSUTCDATETIME(),
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SeoAuditResult_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SeoAuditResult_UpdatedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_SeoAuditRule_RuleKey' AND object_id=OBJECT_ID('dbo.SeoAuditRule'))
  CREATE UNIQUE INDEX UX_SeoAuditRule_RuleKey ON dbo.SeoAuditRule(RuleKey);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditRule_IsActive_SortOrder' AND object_id=OBJECT_ID('dbo.SeoAuditRule'))
  CREATE INDEX IX_SeoAuditRule_IsActive_SortOrder ON dbo.SeoAuditRule(IsActive, SortOrder, SeoAuditRuleId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditRuleParameter_Rule_Sort' AND object_id=OBJECT_ID('dbo.SeoAuditRuleParameter'))
  CREATE INDEX IX_SeoAuditRuleParameter_Rule_Sort ON dbo.SeoAuditRuleParameter(SeoAuditRuleId, IsActive, SortOrder, SeoAuditRuleParameterId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_SeoAuditResult_Place_Rule' AND object_id=OBJECT_ID('dbo.SeoAuditResult'))
  CREATE UNIQUE INDEX UX_SeoAuditResult_Place_Rule ON dbo.SeoAuditResult(PlaceId, SeoAuditRuleId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditResult_Place_Status_Sort' AND object_id=OBJECT_ID('dbo.SeoAuditResult'))
  CREATE INDEX IX_SeoAuditResult_Place_Status_Sort ON dbo.SeoAuditResult(PlaceId, [Status], SortOrderSnapshot, ScoreImpactApplied DESC);

IF NOT EXISTS (
  SELECT 1 FROM sys.foreign_key_columns fkc
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.SeoAuditRuleParameter')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.SeoAuditRule'))
  ALTER TABLE dbo.SeoAuditRuleParameter WITH CHECK ADD CONSTRAINT FK_SeoAuditRuleParameter_SeoAuditRule FOREIGN KEY (SeoAuditRuleId) REFERENCES dbo.SeoAuditRule(SeoAuditRuleId) ON DELETE CASCADE;

IF NOT EXISTS (
  SELECT 1 FROM sys.foreign_key_columns fkc
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.SeoAuditResult')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.Place')
    AND fkc.constraint_object_id = OBJECT_ID('dbo.FK_SeoAuditResult_Place'))
  ALTER TABLE dbo.SeoAuditResult WITH CHECK ADD CONSTRAINT FK_SeoAuditResult_Place FOREIGN KEY (PlaceId) REFERENCES dbo.Place(PlaceId) ON DELETE CASCADE;

IF NOT EXISTS (
  SELECT 1 FROM sys.foreign_key_columns fkc
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.SeoAuditResult')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.SeoAuditRule')
    AND fkc.constraint_object_id = OBJECT_ID('dbo.FK_SeoAuditResult_SeoAuditRule'))
  ALTER TABLE dbo.SeoAuditResult WITH CHECK ADD CONSTRAINT FK_SeoAuditResult_SeoAuditRule FOREIGN KEY (SeoAuditRuleId) REFERENCES dbo.SeoAuditRule(SeoAuditRuleId) ON DELETE CASCADE;

IF OBJECT_ID('dbo.SearchRun','U') IS NOT NULL
   AND OBJECT_ID('dbo.FK_SeoAuditResult_LastSourceSearchRun','F') IS NULL
  ALTER TABLE dbo.SeoAuditResult WITH CHECK ADD CONSTRAINT FK_SeoAuditResult_LastSourceSearchRun FOREIGN KEY (LastSourceSearchRunId) REFERENCES dbo.SearchRun(SearchRunId);
