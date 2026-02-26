IF OBJECT_ID('dbo.ApiStatusCheckResult','U') IS NOT NULL
BEGIN
  IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ApiStatusCheckResult_DefinitionId_CheckedUtc' AND object_id=OBJECT_ID('dbo.ApiStatusCheckResult'))
    DROP INDEX IX_ApiStatusCheckResult_DefinitionId_CheckedUtc ON dbo.ApiStatusCheckResult;

  IF OBJECT_ID('dbo.FK_ApiStatusCheckResult_Definition','F') IS NOT NULL
    ALTER TABLE dbo.ApiStatusCheckResult DROP CONSTRAINT FK_ApiStatusCheckResult_Definition;

  DROP TABLE dbo.ApiStatusCheckResult;
END;

IF OBJECT_ID('dbo.ApiStatusCheckDefinition','U') IS NOT NULL
BEGIN
  IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_ApiStatusCheckDefinition_Key' AND object_id=OBJECT_ID('dbo.ApiStatusCheckDefinition'))
    DROP INDEX UX_ApiStatusCheckDefinition_Key ON dbo.ApiStatusCheckDefinition;

  DROP TABLE dbo.ApiStatusCheckDefinition;
END;

