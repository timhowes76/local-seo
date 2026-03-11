MERGE dbo.SeoAuditRule AS target
USING (VALUES
  (N'TownCentreDistanceRelative', N'Distance from town centre', N'Shows how close the business is to the town centre compared with other places in the same run.', N'Location Context', N'CompetitorRelative', N'TownCentreDistanceRank', N'GBP_PROFILE', N'Info', 3, 5, 240, 1, 1, N'This is contextual ranking information only and helps explain how central the business location is within the run.', NULL)
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
