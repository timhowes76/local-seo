IF OBJECT_ID('dbo.FK_SeoAuditResult_LastSourceSearchRun','F') IS NOT NULL
  ALTER TABLE dbo.SeoAuditResult DROP CONSTRAINT FK_SeoAuditResult_LastSourceSearchRun;
IF OBJECT_ID('dbo.FK_SeoAuditResult_SeoAuditRule','F') IS NOT NULL
  ALTER TABLE dbo.SeoAuditResult DROP CONSTRAINT FK_SeoAuditResult_SeoAuditRule;
IF OBJECT_ID('dbo.FK_SeoAuditResult_Place','F') IS NOT NULL
  ALTER TABLE dbo.SeoAuditResult DROP CONSTRAINT FK_SeoAuditResult_Place;
IF OBJECT_ID('dbo.FK_SeoAuditRuleParameter_SeoAuditRule','F') IS NOT NULL
  ALTER TABLE dbo.SeoAuditRuleParameter DROP CONSTRAINT FK_SeoAuditRuleParameter_SeoAuditRule;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditResult_Place_Status_Sort' AND object_id=OBJECT_ID('dbo.SeoAuditResult'))
  DROP INDEX IX_SeoAuditResult_Place_Status_Sort ON dbo.SeoAuditResult;
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_SeoAuditResult_Place_Rule' AND object_id=OBJECT_ID('dbo.SeoAuditResult'))
  DROP INDEX UX_SeoAuditResult_Place_Rule ON dbo.SeoAuditResult;
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditRuleParameter_Rule_Sort' AND object_id=OBJECT_ID('dbo.SeoAuditRuleParameter'))
  DROP INDEX IX_SeoAuditRuleParameter_Rule_Sort ON dbo.SeoAuditRuleParameter;
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SeoAuditRule_IsActive_SortOrder' AND object_id=OBJECT_ID('dbo.SeoAuditRule'))
  DROP INDEX IX_SeoAuditRule_IsActive_SortOrder ON dbo.SeoAuditRule;
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_SeoAuditRule_RuleKey' AND object_id=OBJECT_ID('dbo.SeoAuditRule'))
  DROP INDEX UX_SeoAuditRule_RuleKey ON dbo.SeoAuditRule;

IF OBJECT_ID('dbo.SeoAuditResult','U') IS NOT NULL
  DROP TABLE dbo.SeoAuditResult;
IF OBJECT_ID('dbo.SeoAuditRuleParameter','U') IS NOT NULL
  DROP TABLE dbo.SeoAuditRuleParameter;
IF OBJECT_ID('dbo.SeoAuditRule','U') IS NOT NULL
  DROP TABLE dbo.SeoAuditRule;
