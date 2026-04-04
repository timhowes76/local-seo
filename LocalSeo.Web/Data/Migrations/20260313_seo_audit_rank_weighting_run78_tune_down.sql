UPDATE dbo.SeoAuditRule
SET
  WarningScoreImpact = CASE RuleKey
    WHEN N'LowReviewCount' THEN 2
    WHEN N'TownCentreDistanceRelative' THEN 5
    WHEN N'HomepageTopicalKeywordRelevance' THEN 1
    WHEN N'HomepageInternalLinking' THEN 1
    ELSE WarningScoreImpact
  END,
  FailScoreImpact = CASE RuleKey
    WHEN N'MissingSecondaryCategories' THEN 3
    WHEN N'NoResponsesToReviews' THEN 17
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 3
    WHEN N'ReviewVelocity' THEN 6
    WHEN N'LowReviewCount' THEN 3
    WHEN N'TownCentreDistanceRelative' THEN 9
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 1
    WHEN N'KeywordsInLandingPageHeadings' THEN 0
    WHEN N'HomepageNicheFocus' THEN 0
    WHEN N'HomepageTopicalKeywordRelevance' THEN 1
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 0
    WHEN N'WebsiteUsesHttpsByDefault' THEN 1
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
