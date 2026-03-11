UPDATE dbo.SeoAuditRule
SET
  [Description] = N'Shows how closely the business title matches the run service phrase using close variants and the search town.',
  WhyItMattersText = N'Keywords in the business title are widely considered a strong local ranking signal, so this helps explain title relevance against the run intent without rewarding vague generic words.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'KeywordsInBusinessTitle';

UPDATE dbo.SeoAuditRule
SET
  [Description] = N'Checks whether the formatted address appears to be located in the run town rather than only using the town as a postal area.',
  WhyItMattersText = N'Being physically located in the searched town is widely considered a strong local relevance signal, so postal-town spillover should not be treated as a true match.',
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey = N'PhysicalAddressMatchesSearchTown';
