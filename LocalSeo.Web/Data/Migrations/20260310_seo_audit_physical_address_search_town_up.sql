MERGE dbo.SeoAuditRule AS target
USING (VALUES
  (N'PhysicalAddressMatchesSearchTown', N'Physical address in search town', N'Checks whether the formatted address includes the town targeted by the run.', N'Location Context', N'Benchmark', N'PhysicalAddressInSearchTown', N'GBP_PROFILE', N'Info', 0, 8, 270, 1, 1, N'Being physically located in the searched town is widely considered a strong local relevance signal.', NULL)
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
