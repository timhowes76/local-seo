UPDATE dbo.SeoAuditRule
SET
  WarningScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 0
    WHEN N'DescriptionTooShort' THEN 4
    WHEN N'MissingSecondaryCategories' THEN 0
    WHEN N'NoResponsesToReviews' THEN 0
    WHEN N'NotAlwaysRespondingToReviews' THEN 4
    WHEN N'TimeToLeaveReviewResponse' THEN 4
    WHEN N'NoQas' THEN 0
    WHEN N'NoUpdates' THEN 0
    WHEN N'NoPhotos' THEN 0
    WHEN N'OverallRatingBelow4' THEN 7
    WHEN N'NoRecentReviews' THEN 6
    WHEN N'LowReviewCount' THEN 7
    WHEN N'NoReviewsWithText' THEN 7
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 0
    WHEN N'NoRecentPosts' THEN 4
    WHEN N'VeryFewPhotos' THEN 4
    WHEN N'QasPresentButUnanswered' THEN 4
    WHEN N'ReviewVelocity' THEN 6
    WHEN N'BurstyReviews' THEN 6
    WHEN N'RatingTrendingDownward' THEN 7
    WHEN N'LowEngagementOnReviews' THEN 5
    WHEN N'BusinessHoursMissing' THEN 0
    WHEN N'TownCentreDistanceRelative' THEN 3
    WHEN N'KeywordsInBusinessTitle' THEN 6
    WHEN N'PrimaryCategoryMatchesRun' THEN 0
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 0
    WHEN N'HtmlNapMatchesGbpNap' THEN 77
    WHEN N'KeywordsInLandingPageTitleTag' THEN 73
    WHEN N'KeywordsInLandingPageHeadings' THEN 68
    WHEN N'HomepageNicheFocus' THEN 59
    WHEN N'HomepageTopicalKeywordRelevance' THEN 59
    WHEN N'HomepageInternalLinking' THEN 54
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 52
    WHEN N'WebsiteUsesHttpsByDefault' THEN 0
    WHEN N'KeywordsInDomainName' THEN 49
    ELSE WarningScoreImpact
  END,
  FailScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 8
    WHEN N'DescriptionTooShort' THEN 5
    WHEN N'MissingSecondaryCategories' THEN 10
    WHEN N'NoResponsesToReviews' THEN 6
    WHEN N'NotAlwaysRespondingToReviews' THEN 6
    WHEN N'TimeToLeaveReviewResponse' THEN 6
    WHEN N'NoQas' THEN 6
    WHEN N'NoUpdates' THEN 5
    WHEN N'NoPhotos' THEN 6
    WHEN N'OverallRatingBelow4' THEN 10
    WHEN N'NoRecentReviews' THEN 9
    WHEN N'LowReviewCount' THEN 10
    WHEN N'NoReviewsWithText' THEN 10
    WHEN N'NoOwnerResponsesToRecentReviews' THEN 6
    WHEN N'NoRecentPosts' THEN 5
    WHEN N'VeryFewPhotos' THEN 6
    WHEN N'QasPresentButUnanswered' THEN 6
    WHEN N'ReviewVelocity' THEN 9
    WHEN N'BurstyReviews' THEN 9
    WHEN N'RatingTrendingDownward' THEN 10
    WHEN N'LowEngagementOnReviews' THEN 7
    WHEN N'BusinessHoursMissing' THEN 8
    WHEN N'TownCentreDistanceRelative' THEN 9
    WHEN N'KeywordsInBusinessTitle' THEN 9
    WHEN N'PrimaryCategoryMatchesRun' THEN 10
    WHEN N'PhysicalAddressMatchesSearchTown' THEN 8
    WHEN N'HtmlNapMatchesGbpNap' THEN 153
    WHEN N'KeywordsInLandingPageTitleTag' THEN 146
    WHEN N'KeywordsInLandingPageHeadings' THEN 135
    WHEN N'HomepageNicheFocus' THEN 118
    WHEN N'HomepageTopicalKeywordRelevance' THEN 117
    WHEN N'HomepageInternalLinking' THEN 107
    WHEN N'KeywordsInInternalLinkAnchorText' THEN 103
    WHEN N'WebsiteUsesHttpsByDefault' THEN 102
    WHEN N'KeywordsInDomainName' THEN 97
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
