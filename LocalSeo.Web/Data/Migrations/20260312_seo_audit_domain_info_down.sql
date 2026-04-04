UPDATE dbo.SeoAuditRule
SET
  Severity = N'Opportunity',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'KeywordsInDomainName';
