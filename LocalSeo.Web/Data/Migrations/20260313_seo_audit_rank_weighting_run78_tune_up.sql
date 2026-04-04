UPDATE dbo.SeoAuditRule
SET
  WarningScoreImpact = CASE RuleKey
    WHEN N'LowReviewCount' THEN 0
    WHEN N'TownCentreDistanceRelative' THEN 2
    WHEN N'HomepageTopicalKeywordRelevance' THEN 0
    WHEN N'HomepageInternalLinking' THEN 2
    ELSE WarningScoreImpact
  END,
  FailScoreImpact = CASE RuleKey
    WHEN N'MissingSecondaryCategories' THEN 2
    WHEN N'NoResponsesToReviews' THEN 20
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 0
    WHEN N'ReviewVelocity' THEN 4
    WHEN N'LowReviewCount' THEN 2
    WHEN N'TownCentreDistanceRelative' THEN 11
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 0
    WHEN N'KeywordsInLandingPageHeadings' THEN 1
    WHEN N'HomepageNicheFocus' THEN 1
    WHEN N'HomepageTopicalKeywordRelevance' THEN 3
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 4
    WHEN N'WebsiteUsesHttpsByDefault' THEN 0
    ELSE FailScoreImpact
  END,
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey IN (
  N'MissingSecondaryCategories',
  N'NoResponsesToReviews',
  N'NoOwnerResponsesToRecentReviews',
  N'ReviewVelocity',
  N'LowReviewCount',
  N'TownCentreDistanceRelative',
  N'PhysicalAddressMatchesSearchTown',
  N'KeywordsInLandingPageHeadings',
  N'HomepageNicheFocus',
  N'HomepageTopicalKeywordRelevance',
  N'HomepageInternalLinking',
  N'KeywordsInInternalLinkAnchorText',
  N'WebsiteUsesHttpsByDefault'
);
