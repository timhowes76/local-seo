MERGE dbo.SeoAuditRule AS target
USING (VALUES
  (N'PrimaryCategoryMatchesRun', N'Primary category matches run', N'Checks whether the primary GBP category matches the category targeted by the run.', N'Categories', N'Benchmark', N'PrimaryCategoryMatch', N'GBP_CATEGORIES', N'Critical', 0, 10, 260, 1, 1, N'Primary category is one of the strongest local ranking signals and is directly configurable in the profile.', N'Set the primary GBP category to the closest match for the service you want to rank for.')
) AS source(RuleKey, [Name], [Description], RuleGroup, RuleMode, RuleType, EntityType, Severity, WarningScoreImpact, FailScoreImpact, SortOrder, IsActive, ShowInActionsTab, WhyItMattersText, RecommendedActionText)
ON target.RuleKey = source.RuleKey
WHEN MATCHED THEN
  UPDATE SET
    target.[Name] = source.[Name],
    target.[Description] = source.[Description],
    target.RuleGroup = source.RuleGroup,
    target.RuleMode = source.RuleMode,
    target.RuleType = source.RuleType,
    target.EntityType = source.EntityType,
    target.Severity = source.Severity,
    target.WarningScoreImpact = source.WarningScoreImpact,
    target.FailScoreImpact = source.FailScoreImpact,
    target.SortOrder = source.SortOrder,
    target.IsActive = source.IsActive,
    target.ShowInActionsTab = source.ShowInActionsTab,
    target.WhyItMattersText = source.WhyItMattersText,
    target.RecommendedActionText = source.RecommendedActionText,
    target.UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(RuleKey, [Name], [Description], RuleGroup, RuleMode, RuleType, EntityType, Severity, WarningScoreImpact, FailScoreImpact, SortOrder, IsActive, ShowInActionsTab, WhyItMattersText, RecommendedActionText, CreatedAtUtc, UpdatedAtUtc)
  VALUES(source.RuleKey, source.[Name], source.[Description], source.RuleGroup, source.RuleMode, source.RuleType, source.EntityType, source.Severity, source.WarningScoreImpact, source.FailScoreImpact, source.SortOrder, source.IsActive, source.ShowInActionsTab, source.WhyItMattersText, source.RecommendedActionText, SYSUTCDATETIME(), SYSUTCDATETIME());
