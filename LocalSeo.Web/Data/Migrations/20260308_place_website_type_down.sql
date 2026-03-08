UPDATE dbo.SeoAuditRule
SET
  [Description] = N'No website URL is stored.',
  WhyItMattersText = N'A missing website removes a major conversion path and signals an obvious optimisation opportunity.',
  RecommendedActionText = N'Add the correct website URL to the business profile.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'NoWebsite';

IF EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_Place_WebsiteType_Valid'
    AND parent_object_id = OBJECT_ID('dbo.Place')
)
  ALTER TABLE dbo.Place DROP CONSTRAINT CK_Place_WebsiteType_Valid;

DECLARE @DefaultConstraintName sysname;
SELECT @DefaultConstraintName = dc.name
FROM sys.default_constraints dc
JOIN sys.columns c
  ON c.object_id = dc.parent_object_id
 AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID('dbo.Place')
  AND c.name = 'WebsiteType';

IF @DefaultConstraintName IS NOT NULL
  EXEC(N'ALTER TABLE dbo.Place DROP CONSTRAINT ' + QUOTENAME(@DefaultConstraintName) + N';');

IF COL_LENGTH('dbo.Place', 'WebsiteType') IS NOT NULL
  ALTER TABLE dbo.Place DROP COLUMN WebsiteType;
