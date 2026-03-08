DELETE p
FROM dbo.SeoAuditRuleParameter p
JOIN dbo.SeoAuditRule r ON r.SeoAuditRuleId = p.SeoAuditRuleId
WHERE r.RuleKey IN (
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
  N'BusinessHoursMissing'
);

DELETE FROM dbo.SeoAuditRule
WHERE RuleKey IN (
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
  N'BusinessHoursMissing'
);

UPDATE dbo.SeoAuditRule
SET
  WarningScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 4
    WHEN N'DescriptionTooShort' THEN 4
    WHEN N'MissingSecondaryCategories' THEN 4
    WHEN N'NoResponsesToReviews' THEN 5
    WHEN N'NotAlwaysRespondingToReviews' THEN 4
    WHEN N'TimeToLeaveReviewResponse' THEN 4
    WHEN N'NoWebsite' THEN 5
    WHEN N'NoQas' THEN 3
    WHEN N'NoUpdates' THEN 4
    WHEN N'NoPhotos' THEN 4
    WHEN N'OverallRatingBelow4' THEN 4
    ELSE WarningScoreImpact
  END,
  FailScoreImpact = CASE RuleKey
    WHEN N'MissingDescription' THEN 10
    WHEN N'DescriptionTooShort' THEN 8
    WHEN N'MissingSecondaryCategories' THEN 9
    WHEN N'NoResponsesToReviews' THEN 10
    WHEN N'NotAlwaysRespondingToReviews' THEN 8
    WHEN N'TimeToLeaveReviewResponse' THEN 9
    WHEN N'NoWebsite' THEN 10
    WHEN N'NoQas' THEN 7
    WHEN N'NoUpdates' THEN 8
    WHEN N'NoPhotos' THEN 9
    WHEN N'OverallRatingBelow4' THEN 10
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
  N'NoWebsite',
  N'NoQas',
  N'NoUpdates',
  N'NoPhotos',
  N'OverallRatingBelow4'
);
