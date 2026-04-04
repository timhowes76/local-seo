UPDATE dbo.SeoAuditRule
SET
  Severity = N'Info',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'KeywordsInDomainName';
