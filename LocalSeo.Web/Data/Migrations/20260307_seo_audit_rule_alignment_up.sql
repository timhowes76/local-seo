MERGE dbo.SeoAuditRule AS target
USING (VALUES
  (N'MissingDescription', N'Missing description', N'Description is missing entirely.', N'Profile Completeness', N'Fixed', N'MissingField', N'GBP_PROFILE', N'Critical', 0, 8, 10, 1, 1, N'A complete business description helps prospects understand what the business does and improves profile trust.', N'Add a clear business description that explains services, location relevance, and differentiators.'),
  (N'DescriptionTooShort', N'Description too short', N'Description exists but is below the recommended length.', N'Profile Completeness', N'Fixed', N'MinLength', N'GBP_PROFILE', N'Opportunity', 4, 5, 20, 1, 1, N'A fuller description gives sales teams a clear upsell opportunity around profile optimisation and messaging.', N'Expand the description to cover core services, local areas served, and differentiators.'),
  (N'MissingSecondaryCategories', N'Missing secondary categories', N'No secondary categories are stored.', N'Categories', N'Fixed', N'MissingSecondaryCategories', N'GBP_CATEGORIES', N'Warning', 0, 10, 30, 1, 1, N'Secondary categories improve relevance for additional service searches and reveal category optimisation work.', N'Add relevant secondary categories that match the real services the business provides.'),
  (N'NoResponsesToReviews', N'No responses to reviews', N'Reviews exist but there are no owner responses.', N'Reviews', N'Fixed', N'NoResponses', N'GBP_REVIEWS', N'Critical', 0, 6, 40, 1, 1, N'Not responding to reviews weakens trust and misses an easy customer engagement win.', N'Start replying to reviews, beginning with the most recent and most visible feedback.'),
  (N'NotAlwaysRespondingToReviews', N'Not always responding to reviews', N'Only some reviews have owner responses.', N'Reviews', N'Fixed', N'ResponseCoverage', N'GBP_REVIEWS', N'Opportunity', 4, 6, 50, 1, 1, N'Inconsistent review responses still leave sales opportunity in reputation management and process improvements.', N'Create a simple response process so every new review receives an owner reply.'),
  (N'TimeToLeaveReviewResponse', N'Time to leave a review response', N'Responses are slower than the target window.', N'Reviews', N'Fixed', N'ResponseTime', N'GBP_REVIEWS', N'Warning', 4, 6, 60, 1, 1, N'Faster responses show active business management and improve customer confidence.', N'Reply on the same day where possible and keep all responses within three days.'),
  (N'NoWebsite', N'No website', N'No website URL is stored.', N'Profile Completeness', N'Fixed', N'MissingWebsite', N'GBP_PROFILE', N'Critical', 0, 8, 70, 1, 1, N'A missing website removes a major conversion path and signals an obvious optimisation opportunity.', N'Add the correct website URL to the business profile.'),
  (N'NoQas', N'No Q&As', N'No question and answer entries are stored.', N'Engagement', N'Fixed', N'MissingQa', N'GBP_QA', N'Opportunity', 0, 6, 80, 1, 1, N'Questions and answers can improve relevance and address customer hesitation before contact.', N'Seed useful common questions and keep answers accurate and up to date.'),
  (N'NoUpdates', N'No updates', N'No updates are stored.', N'Engagement', N'Fixed', N'MissingUpdates', N'GBP_UPDATES', N'Opportunity', 0, 5, 90, 1, 1, N'Updates demonstrate activity and give the sales team a recurring content opportunity to sell.', N'Publish regular business updates, offers, or service highlights on the profile.'),
  (N'NoPhotos', N'No photos', N'No photo count is stored.', N'Media', N'Fixed', N'MissingPhotos', N'GBP_PHOTOS', N'Warning', 0, 6, 100, 1, 1, N'Photos improve profile quality and conversion confidence, making this a strong optimisation opportunity.', N'Add recent, high-quality photos that show the business, team, premises, and services.'),
  (N'OverallRatingBelow4', N'Overall rating below 4', N'Current stored rating is below the target threshold or missing.', N'Reviews', N'Fixed', N'RatingThreshold', N'GBP_REVIEWS', N'Critical', 7, 10, 110, 1, 1, N'Low ratings reduce trust and usually indicate a wider review-generation and service-recovery opportunity.', N'Improve review generation, resolve unhappy customer issues quickly, and respond to negative reviews professionally.'),
  (N'NoRecentReviews', N'No recent reviews', N'The latest stored review is older than the target recency window.', N'Reviews', N'Fixed', N'ReviewRecency', N'GBP_REVIEWS', N'Warning', 6, 9, 120, 1, 1, N'Fresh reviews support local ranking strength and give the sales team a clear review-generation opportunity to sell.', N'Create a steady review-generation process so new reviews keep landing every month.'),
  (N'LowReviewCount', N'Low review count', N'Total review count is below the current target band.', N'Reviews', N'Fixed', N'ReviewCountThreshold', N'GBP_REVIEWS', N'Critical', 7, 10, 130, 1, 1, N'A low review count limits trust, conversion confidence, and local ranking competitiveness.', N'Increase review generation volume with a repeatable post-sale follow-up process.'),
  (N'NoReviewsWithText', N'No reviews with text', N'Stored reviews do not contain enough written review content.', N'Reviews', N'Fixed', N'TextReviewCoverage', N'GBP_REVIEWS', N'Critical', 7, 10, 140, 1, 1, N'Written reviews provide richer trust signals and stronger ranking value than star-only reviews.', N'Ask customers to leave a short written review that mentions the real service experience.'),
  (N'NoOwnerResponsesToRecentReviews', N'No owner responses to recent reviews', N'The latest stored reviews do not include owner replies.', N'Reviews', N'Fixed', N'RecentResponseCoverage', N'GBP_REVIEWS', N'Warning', 0, 6, 150, 1, 1, N'Recent reviews are the most visible trust signals, so unanswered recent feedback leaves obvious value on the table.', N'Reply to the latest reviews first so the profile looks actively managed.'),
  (N'NoRecentPosts', N'No recent posts', N'The latest stored post is older than the target recency window.', N'Engagement', N'Fixed', N'PostRecency', N'GBP_UPDATES', N'Opportunity', 4, 5, 160, 1, 1, N'Recent posts show activity and create an ongoing content/service upsell opportunity.', N'Publish fresh profile updates at least monthly to keep the profile active.'),
  (N'VeryFewPhotos', N'Very few photos', N'Photo count is present but below the current target band.', N'Media', N'Fixed', N'PhotoCountThreshold', N'GBP_PHOTOS', N'Warning', 4, 6, 170, 1, 1, N'A very small photo library makes the profile look thin and gives sales an obvious visual-optimisation opportunity.', N'Add more recent photos of the premises, team, work, and services.'),
  (N'QasPresentButUnanswered', N'Q&As present but unanswered', N'Questions exist but none of the stored rows include answers.', N'Engagement', N'Fixed', N'QaAnswerCoverage', N'GBP_QA', N'Opportunity', 4, 6, 180, 1, 1, N'Unanswered questions leave customer hesitation unresolved and reduce the value of the Q&A section.', N'Answer the existing questions and seed new common questions where useful.'),
  (N'ReviewVelocity', N'Review velocity', N'Reviews are not arriving frequently enough over the latest 90-day window.', N'Reviews', N'Fixed', N'ReviewVelocity', N'GBP_REVIEWS', N'Warning', 6, 9, 190, 1, 1, N'Steady review velocity is a strong local ranking and trust signal, and a clear recurring service opportunity.', N'Build a monthly review-acquisition process so reviews arrive continuously rather than sporadically.'),
  (N'BurstyReviews', N'Bursty reviews', N'Too many recent reviews are concentrated in a single month.', N'Reviews', N'Fixed', N'ReviewBurstiness', N'GBP_REVIEWS', N'Warning', 6, 9, 200, 1, 1, N'Uneven review patterns can indicate one-off campaigns instead of a healthy ongoing review pipeline.', N'Spread review asks consistently through the month instead of batching them into a single push.'),
  (N'RatingTrendingDownward', N'Rating trending downward', N'Recent review ratings are weaker than the older baseline.', N'Reviews', N'Fixed', N'RatingTrend', N'GBP_REVIEWS', N'Warning', 7, 10, 210, 1, 1, N'A downward rating trend can signal service issues and creates a strong service-recovery and reputation-management opportunity.', N'Investigate the recent negative pattern, resolve operational issues, and increase review generation from happy customers.'),
  (N'LowEngagementOnReviews', N'Low engagement on reviews', N'Too many stored reviews are three stars or below.', N'Reviews', N'Fixed', N'LowRatingShare', N'GBP_REVIEWS', N'Warning', 5, 7, 220, 1, 1, N'A high share of weaker reviews reduces trust even if the headline rating still looks acceptable.', N'Address recurring customer issues and increase the share of positive service experiences that turn into reviews.'),
  (N'BusinessHoursMissing', N'Business hours missing', N'Regular opening hours are missing from the stored profile.', N'Profile Completeness', N'Fixed', N'MissingHours', N'GBP_PROFILE', N'Critical', 0, 8, 230, 1, 1, N'Missing opening hours makes the profile look incomplete and can cost conversions from ready-to-buy searchers.', N'Add accurate regular opening hours to the business profile.')
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
    target.UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(RuleKey, [Name], [Description], RuleGroup, RuleMode, RuleType, EntityType, Severity, WarningScoreImpact, FailScoreImpact, SortOrder, IsActive, ShowInActionsTab, WhyItMattersText, RecommendedActionText, CreatedAtUtc, UpdatedAtUtc)
  VALUES(source.RuleKey, source.[Name], source.[Description], source.RuleGroup, source.RuleMode, source.RuleType, source.EntityType, source.Severity, source.WarningScoreImpact, source.FailScoreImpact, source.SortOrder, source.IsActive, source.ShowInActionsTab, source.WhyItMattersText, source.RecommendedActionText, SYSUTCDATETIME(), SYSUTCDATETIME());

MERGE dbo.SeoAuditRuleParameter AS target
USING (
  SELECT
    r.SeoAuditRuleId,
    v.ParameterName,
    v.ParameterValue,
    v.ValueType,
    v.SortOrder,
    v.IsActive
  FROM dbo.SeoAuditRule r
  JOIN (VALUES
    (N'DescriptionTooShort', N'MinimumLength', N'495', N'Int', 10, 1),
    (N'DescriptionTooShort', N'MaximumLength', N'750', N'Int', 20, 1),
    (N'TimeToLeaveReviewResponse', N'MaximumWarningDays', N'3', N'Int', 10, 1),
    (N'OverallRatingBelow4', N'MinimumRating', N'4.0', N'Decimal', 10, 1),
    (N'NoRecentReviews', N'WarningDays', N'30', N'Int', 10, 1),
    (N'NoRecentReviews', N'FailDays', N'90', N'Int', 20, 1),
    (N'LowReviewCount', N'WarningReviewCount', N'10', N'Int', 10, 1),
    (N'LowReviewCount', N'FailReviewCount', N'5', N'Int', 20, 1),
    (N'NoReviewsWithText', N'MinimumTextReviewPct', N'50', N'Decimal', 10, 1),
    (N'NoOwnerResponsesToRecentReviews', N'RecentReviewCount', N'5', N'Int', 10, 1),
    (N'NoRecentPosts', N'WarningDays', N'30', N'Int', 10, 1),
    (N'NoRecentPosts', N'FailDays', N'90', N'Int', 20, 1),
    (N'VeryFewPhotos', N'WarningPhotoCount', N'5', N'Int', 10, 1),
    (N'VeryFewPhotos', N'FailPhotoCount', N'3', N'Int', 20, 1),
    (N'ReviewVelocity', N'LookbackDays', N'90', N'Int', 10, 1),
    (N'ReviewVelocity', N'WarningReviewsPerMonth', N'1.0', N'Decimal', 20, 1),
    (N'ReviewVelocity', N'FailReviewsPerMonth', N'0.5', N'Decimal', 30, 1),
    (N'BurstyReviews', N'LookbackMonths', N'6', N'Int', 10, 1),
    (N'BurstyReviews', N'DominantMonthPct', N'70', N'Decimal', 20, 1),
    (N'BurstyReviews', N'MinimumReviews', N'6', N'Int', 30, 1),
    (N'RatingTrendingDownward', N'LatestWindowSize', N'10', N'Int', 10, 1),
    (N'RatingTrendingDownward', N'MinimumPreviousReviewCount', N'5', N'Int', 20, 1),
    (N'LowEngagementOnReviews', N'MaximumLowRatingPct', N'30', N'Decimal', 10, 1),
    (N'LowEngagementOnReviews', N'LowRatingThreshold', N'3.0', N'Decimal', 20, 1)
  ) v(RuleKey, ParameterName, ParameterValue, ValueType, SortOrder, IsActive) ON v.RuleKey = r.RuleKey
) AS source
ON target.SeoAuditRuleId = source.SeoAuditRuleId
   AND target.ParameterName = source.ParameterName
WHEN MATCHED THEN
  UPDATE SET
    target.ParameterValue = source.ParameterValue,
    target.ValueType = source.ValueType,
    target.SortOrder = source.SortOrder,
    target.IsActive = source.IsActive
WHEN NOT MATCHED THEN
  INSERT(SeoAuditRuleId, ParameterName, ParameterValue, ValueType, SortOrder, IsActive)
  VALUES(source.SeoAuditRuleId, source.ParameterName, source.ParameterValue, source.ValueType, source.SortOrder, source.IsActive);
