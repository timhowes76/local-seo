UPDATE dbo.SeoAuditRule
SET
  [Description] = N'Shows how closely the business title matches the run keyword and town terms.',
  WhyItMattersText = N'Keywords in the business title are widely considered a strong local ranking signal, so this helps explain title relevance against the run intent.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'KeywordsInBusinessTitle';

UPDATE dbo.SeoAuditRule
SET
  [Description] = N'Checks whether the formatted address includes the town targeted by the run.',
  WhyItMattersText = N'Being physically located in the searched town is widely considered a strong local relevance signal.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'PhysicalAddressMatchesSearchTown';
