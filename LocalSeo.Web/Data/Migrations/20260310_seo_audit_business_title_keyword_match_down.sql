DELETE ar
FROM dbo.SeoAuditResult ar
JOIN dbo.SeoAuditRule r ON r.SeoAuditRuleId = ar.SeoAuditRuleId
WHERE r.RuleKey = N'KeywordsInBusinessTitle';

DELETE p
FROM dbo.SeoAuditRuleParameter p
JOIN dbo.SeoAuditRule r ON r.SeoAuditRuleId = p.SeoAuditRuleId
WHERE r.RuleKey = N'KeywordsInBusinessTitle';

DELETE FROM dbo.SeoAuditRule
WHERE RuleKey = N'KeywordsInBusinessTitle';
