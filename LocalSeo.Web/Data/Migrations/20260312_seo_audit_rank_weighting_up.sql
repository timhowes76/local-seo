UPDATE dbo.SeoAuditRule
SET
  WarningScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 0
    WHEN N'DescriptionTooShort' THEN 0
    WHEN N'MissingSecondaryCategories' THEN 0
    WHEN N'NoResponsesToReviews' THEN 0
    WHEN N'NotAlwaysRespondingToReviews' THEN 0
    WHEN N'TimeToLeaveReviewResponse' THEN 13
    WHEN N'NoQas' THEN 0
    WHEN N'NoUpdates' THEN 0
    WHEN N'NoPhotos' THEN 0
    WHEN N'OverallRatingBelow4' THEN 2
    WHEN N'NoRecentReviews' THEN 0
    WHEN N'LowReviewCount' THEN 2
    WHEN N'NoReviewsWithText' THEN 1
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 0
    WHEN N'NoRecentPosts' THEN 1
    WHEN N'VeryFewPhotos' THEN 0
    WHEN N'QasPresentButUnanswered' THEN 0
    WHEN N'ReviewVelocity' THEN 4
    WHEN N'BurstyReviews' THEN 0
    WHEN N'RatingTrendingDownward' THEN 0
    WHEN N'LowEngagementOnReviews' THEN 1
    WHEN N'BusinessHoursMissing' THEN 0
    WHEN N'TownCentreDistanceRelative' THEN 5
    WHEN N'KeywordsInBusinessTitle' THEN 0
    WHEN N'PrimaryCategoryMatchesRun' THEN 0
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 0
    WHEN N'HtmlNapMatchesGbpNap' THEN 0
    WHEN N'KeywordsInLandingPageTitleTag' THEN 0
    WHEN N'KeywordsInLandingPageHeadings' THEN 0
    WHEN N'HomepageNicheFocus' THEN 0
    WHEN N'HomepageTopicalKeywordRelevance' THEN 1
    WHEN N'HomepageInternalLinking' THEN 1
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 0
    WHEN N'WebsiteUsesHttpsByDefault' THEN 0
    WHEN N'KeywordsInDomainName' THEN 0
    ELSE WarningScoreImpact
  END,
  FailScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 0
    WHEN N'DescriptionTooShort' THEN 0
    WHEN N'MissingSecondaryCategories' THEN 3
    WHEN N'NoResponsesToReviews' THEN 17
    WHEN N'NotAlwaysRespondingToReviews' THEN 0
    WHEN N'TimeToLeaveReviewResponse' THEN 20
    WHEN N'NoQas' THEN 0
    WHEN N'NoUpdates' THEN 1
    WHEN N'NoPhotos' THEN 0
    WHEN N'OverallRatingBelow4' THEN 3
    WHEN N'NoRecentReviews' THEN 0
    WHEN N'LowReviewCount' THEN 3
    WHEN N'NoReviewsWithText' THEN 1
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 3
    WHEN N'NoRecentPosts' THEN 1
    WHEN N'VeryFewPhotos' THEN 0
    WHEN N'QasPresentButUnanswered' THEN 0
    WHEN N'ReviewVelocity' THEN 6
    WHEN N'BurstyReviews' THEN 0
    WHEN N'RatingTrendingDownward' THEN 0
    WHEN N'LowEngagementOnReviews' THEN 2
    WHEN N'BusinessHoursMissing' THEN 1
    WHEN N'TownCentreDistanceRelative' THEN 9
    WHEN N'KeywordsInBusinessTitle' THEN 0
    WHEN N'PrimaryCategoryMatchesRun' THEN 4
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 1
    WHEN N'HtmlNapMatchesGbpNap' THEN 0
    WHEN N'KeywordsInLandingPageTitleTag' THEN 0
    WHEN N'KeywordsInLandingPageHeadings' THEN 0
    WHEN N'HomepageNicheFocus' THEN 0
    WHEN N'HomepageTopicalKeywordRelevance' THEN 1
    WHEN N'HomepageInternalLinking' THEN 2
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 0
    WHEN N'WebsiteUsesHttpsByDefault' THEN 1
    WHEN N'KeywordsInDomainName' THEN 0
    ELSE FailScoreImpact
  END,
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE RuleKey IN (
  N'MissingDescription',
  N'DescriptionTooShort',
  N'MissingSecondaryCategories',
  N'NoResponsesToReviews',
  N'NotAlwaysRespondingToReviews',
  N'TimeToLeaveReviewResponse',
  N'NoQas',
  N'NoUpdates',
  N'NoPhotos',
  N'OverallRatingBelow4',
  N'NoRecentReviews',
  N'LowReviewCount',
  N'NoReviewsWithText',
  N'NoOwnerResponsesToRecentReviews',
  N'NoRecentPosts',
  N'VeryFewPhotos',
  N'QasPresentButUnanswered',
  N'ReviewVelocity',
  N'BurstyReviews',
  N'RatingTrendingDownward',
  N'LowEngagementOnReviews',
  N'BusinessHoursMissing',
  N'TownCentreDistanceRelative',
  N'KeywordsInBusinessTitle',
  N'PrimaryCategoryMatchesRun',
  N'PhysicalAddressMatchesSearchTown',
  N'HtmlNapMatchesGbpNap',
  N'KeywordsInLandingPageTitleTag',
  N'KeywordsInLandingPageHeadings',
  N'HomepageNicheFocus',
  N'HomepageTopicalKeywordRelevance',
  N'HomepageInternalLinking',
  N'KeywordsInInternalLinkAnchorText',
  N'WebsiteUsesHttpsByDefault',
  N'KeywordsInDomainName'
);
