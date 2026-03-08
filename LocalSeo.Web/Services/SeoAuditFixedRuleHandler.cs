using System.Globalization;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed class FixedSeoAuditRuleHandler(TimeProvider timeProvider) : ISeoAuditRuleHandler
{
    public bool CanEvaluate(SeoAuditRuleDefinition rule)
    {
        return string.Equals(rule.RuleMode, SeoAuditRuleModes.Fixed, StringComparison.OrdinalIgnoreCase);
    }

    public SeoAuditEvaluationResult Evaluate(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return rule.RuleKey switch
        {
            SeoAuditRuleKeys.MissingDescription => EvaluateMissingDescription(rule, context),
            SeoAuditRuleKeys.DescriptionTooShort => EvaluateDescriptionTooShort(rule, context),
            SeoAuditRuleKeys.MissingSecondaryCategories => EvaluateMissingSecondaryCategories(rule, context),
            SeoAuditRuleKeys.NoResponsesToReviews => EvaluateNoResponses(rule, context),
            SeoAuditRuleKeys.NotAlwaysRespondingToReviews => EvaluateResponseCoverage(rule, context),
            SeoAuditRuleKeys.TimeToLeaveReviewResponse => EvaluateResponseTime(rule, context),
            SeoAuditRuleKeys.NoWebsite => EvaluateNoWebsite(rule, context),
            SeoAuditRuleKeys.NoQas => EvaluateNoQas(rule, context),
            SeoAuditRuleKeys.NoUpdates => EvaluateNoUpdates(rule, context),
            SeoAuditRuleKeys.NoPhotos => EvaluateNoPhotos(rule, context),
            SeoAuditRuleKeys.OverallRatingBelow4 => EvaluateRating(rule, context),
            SeoAuditRuleKeys.NoRecentReviews => EvaluateNoRecentReviews(rule, context),
            SeoAuditRuleKeys.LowReviewCount => EvaluateLowReviewCount(rule, context),
            SeoAuditRuleKeys.NoReviewsWithText => EvaluateNoReviewsWithText(rule, context),
            SeoAuditRuleKeys.NoOwnerResponsesToRecentReviews => EvaluateNoResponsesToRecentReviews(rule, context),
            SeoAuditRuleKeys.NoRecentPosts => EvaluateNoRecentPosts(rule, context),
            SeoAuditRuleKeys.VeryFewPhotos => EvaluateVeryFewPhotos(rule, context),
            SeoAuditRuleKeys.QasPresentButUnanswered => EvaluateQasPresentButUnanswered(rule, context),
            SeoAuditRuleKeys.ReviewVelocity => EvaluateReviewVelocity(rule, context),
            SeoAuditRuleKeys.BurstyReviews => EvaluateBurstyReviews(rule, context),
            SeoAuditRuleKeys.RatingTrendingDownward => EvaluateRatingTrendingDownward(rule, context),
            SeoAuditRuleKeys.LowEngagementOnReviews => EvaluateLowEngagementOnReviews(rule, context),
            SeoAuditRuleKeys.BusinessHoursMissing => EvaluateBusinessHoursMissing(rule, context),
            _ => BuildNotApplicable(rule, "No evaluator is configured for this fixed rule key.")
        };
    }

    private SeoAuditEvaluationResult EvaluateNoRecentReviews(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.ReviewCount <= 0)
            return BuildNotApplicable(rule, "No stored reviews are available for a review recency check.");

        var datedReviews = context.Reviews.Where(x => x.ReviewTimestampUtc.HasValue).ToList();
        if (datedReviews.Count == 0)
            return BuildNotApplicable(rule, "Stored reviews do not include review timestamps.");

        var warningDays = GetIntParameter(rule, "WarningDays", 30);
        var failDays = GetIntParameter(rule, "FailDays", 90);
        var latestReviewUtc = datedReviews.Max(x => x.ReviewTimestampUtc)!.Value;
        var daysSinceLatest = Math.Max(0, (timeProvider.GetUtcNow().UtcDateTime.Date - latestReviewUtc.Date).Days);

        if (daysSinceLatest > failDays)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, $"{daysSinceLatest} days", $"<= {failDays} days", $"{daysSinceLatest - failDays} days late", $"The latest stored review is {daysSinceLatest} days old.");
        if (daysSinceLatest > warningDays)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{daysSinceLatest} days", $"<= {warningDays} days", $"{daysSinceLatest - warningDays} days late", $"The latest stored review is older than {warningDays} days.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{daysSinceLatest} days", $"<= {warningDays} days", "0", "Recent review activity is present in the stored review history.");
    }

    private static SeoAuditEvaluationResult EvaluateLowReviewCount(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var warningThreshold = GetIntParameter(rule, "WarningReviewCount", 10);
        var failThreshold = GetIntParameter(rule, "FailReviewCount", 5);
        var totalReviewCount = context.TotalReviewCount;

        if (totalReviewCount < failThreshold)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, totalReviewCount.ToString(CultureInfo.InvariantCulture), $">= {failThreshold}", (failThreshold - totalReviewCount).ToString(CultureInfo.InvariantCulture), $"Only {totalReviewCount} total review{(totalReviewCount == 1 ? string.Empty : "s")} are currently stored or known.");
        if (totalReviewCount < warningThreshold)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, totalReviewCount.ToString(CultureInfo.InvariantCulture), $">= {warningThreshold}", (warningThreshold - totalReviewCount).ToString(CultureInfo.InvariantCulture), $"Review count is below the recommended minimum of {warningThreshold}.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, totalReviewCount.ToString(CultureInfo.InvariantCulture), $">= {warningThreshold}", "0", "Total review count meets the current target band.");
    }

    private static SeoAuditEvaluationResult EvaluateNoReviewsWithText(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.TotalReviewCount <= 0)
            return BuildNotApplicable(rule, "No reviews exist for this place.");
        if (context.ReviewCount <= 0)
            return BuildNotApplicable(rule, "Stored review rows are not available for text coverage analysis.");

        var minimumPct = GetDecimalParameter(rule, "MinimumTextReviewPct", 50m);
        var textReviewCount = context.Reviews.Count(x => !string.IsNullOrWhiteSpace(x.ReviewText));
        if (textReviewCount <= 0)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0 text reviews", "At least 1 text review", "1 missing text review set", "None of the stored reviews include review text.");

        var pct = decimal.Round(textReviewCount * 100m / context.ReviewCount, 1, MidpointRounding.AwayFromZero);
        if (pct < minimumPct)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{pct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"{minimumPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"{decimal.Round(minimumPct - pct, 1, MidpointRounding.AwayFromZero).ToString("0.0", CultureInfo.InvariantCulture)}%", $"Only {textReviewCount} of {context.ReviewCount} stored reviews include text.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{pct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"{minimumPct.ToString("0.0", CultureInfo.InvariantCulture)}%", "0.0%", "Stored review text coverage meets the current target.");
    }

    private static SeoAuditEvaluationResult EvaluateNoResponsesToRecentReviews(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var recentReviewCount = GetIntParameter(rule, "RecentReviewCount", 5);
        if (context.ReviewCount < recentReviewCount)
            return BuildNotApplicable(rule, $"Fewer than {recentReviewCount} stored reviews are available.");

        var latestReviews = context.Reviews
            .OrderByDescending(x => x.EffectiveSortUtc)
            .Take(recentReviewCount)
            .ToList();

        if (latestReviews.Count < recentReviewCount)
            return BuildNotApplicable(rule, $"Fewer than {recentReviewCount} stored reviews are available.");

        if (latestReviews.All(x => !x.HasOwnerResponse))
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, $"0 of {recentReviewCount}", $"At least 1 of {recentReviewCount}", recentReviewCount.ToString(CultureInfo.InvariantCulture), $"None of the latest {recentReviewCount} stored reviews have an owner response.");

        var respondedCount = latestReviews.Count(x => x.HasOwnerResponse);
        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{respondedCount} of {recentReviewCount}", $"At least 1 of {recentReviewCount}", "0", $"The latest {recentReviewCount} stored reviews include owner responses.");
    }

    private SeoAuditEvaluationResult EvaluateNoRecentPosts(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.UpdateCount <= 0)
            return BuildNotApplicable(rule, "No stored posts are available and the absolute absence case is covered by the no updates rule.");

        var datedPosts = context.Updates.Where(x => x.EffectiveUpdateUtc.HasValue).ToList();
        if (datedPosts.Count == 0)
            return BuildNotApplicable(rule, "Stored posts do not include a usable post date.");

        var warningDays = GetIntParameter(rule, "WarningDays", 30);
        var failDays = GetIntParameter(rule, "FailDays", 90);
        var latestPostUtc = datedPosts.Max(x => x.EffectiveUpdateUtc)!.Value;
        var daysSinceLatest = Math.Max(0, (timeProvider.GetUtcNow().UtcDateTime.Date - latestPostUtc.Date).Days);

        if (daysSinceLatest > failDays)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, $"{daysSinceLatest} days", $"<= {failDays} days", $"{daysSinceLatest - failDays} days late", $"The latest stored post is {daysSinceLatest} days old.");
        if (daysSinceLatest > warningDays)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{daysSinceLatest} days", $"<= {warningDays} days", $"{daysSinceLatest - warningDays} days late", $"The latest stored post is older than {warningDays} days.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{daysSinceLatest} days", $"<= {warningDays} days", "0", "Recent post activity is present in the stored update history.");
    }

    private static SeoAuditEvaluationResult EvaluateVeryFewPhotos(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (!context.PhotoCount.HasValue)
            return BuildNotApplicable(rule, "Photo count is not stored for this place.");
        if (context.PhotoCount.Value <= 0)
            return BuildNotApplicable(rule, "The no photos rule already covers the absolute absence case.");

        var warningThreshold = GetIntParameter(rule, "WarningPhotoCount", 5);
        var failThreshold = GetIntParameter(rule, "FailPhotoCount", 3);
        if (context.PhotoCount.Value < failThreshold)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, context.PhotoCount.Value.ToString(CultureInfo.InvariantCulture), $">= {failThreshold}", (failThreshold - context.PhotoCount.Value).ToString(CultureInfo.InvariantCulture), $"Only {context.PhotoCount.Value} photo{(context.PhotoCount.Value == 1 ? string.Empty : "s")} are recorded.");
        if (context.PhotoCount.Value < warningThreshold)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, context.PhotoCount.Value.ToString(CultureInfo.InvariantCulture), $">= {warningThreshold}", (warningThreshold - context.PhotoCount.Value).ToString(CultureInfo.InvariantCulture), $"Photo count is below the recommended minimum of {warningThreshold}.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, context.PhotoCount.Value.ToString(CultureInfo.InvariantCulture), $">= {warningThreshold}", "0", "Photo count meets the current target band.");
    }

    private static SeoAuditEvaluationResult EvaluateQasPresentButUnanswered(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.QuestionAnswerCount <= 0)
            return BuildNotApplicable(rule, "No Q&A entries are stored for this place.");
        if (context.QaTableCount <= 0)
            return BuildNotApplicable(rule, "Detailed Q&A rows are not stored, so answer coverage cannot be evaluated.");

        var answeredCount = context.QuestionsAndAnswers.Count(x => !string.IsNullOrWhiteSpace(x.AnswerText) || x.AnswerTimestampUtc.HasValue);
        if (answeredCount <= 0)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"0 of {context.QaTableCount}", "At least 1 answered Q&A", context.QaTableCount.ToString(CultureInfo.InvariantCulture), "Questions exist, but none of the stored Q&A rows include an answer.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{answeredCount} of {context.QaTableCount}", "At least 1 answered Q&A", "0", "Stored Q&A rows include answers.");
    }

    private SeoAuditEvaluationResult EvaluateReviewVelocity(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var lookbackDays = GetIntParameter(rule, "LookbackDays", 90);
        var warningPerMonth = GetDecimalParameter(rule, "WarningReviewsPerMonth", 1m);
        var failPerMonth = GetDecimalParameter(rule, "FailReviewsPerMonth", 0.5m);

        var datedReviews = context.Reviews.Where(x => x.ReviewTimestampUtc.HasValue).ToList();
        if (datedReviews.Count == 0)
            return BuildNotApplicable(rule, "Stored reviews do not include review timestamps.");

        var cutoffUtc = timeProvider.GetUtcNow().UtcDateTime.Date.AddDays(-lookbackDays);
        var reviewsLast90Days = datedReviews.Count(x => x.ReviewTimestampUtc!.Value >= cutoffUtc);
        var reviewsPerMonth = decimal.Round(reviewsLast90Days / 3m, 2, MidpointRounding.AwayFromZero);

        if (reviewsPerMonth < failPerMonth)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, reviewsPerMonth.ToString("0.00", CultureInfo.InvariantCulture), $">= {failPerMonth.ToString("0.00", CultureInfo.InvariantCulture)}", decimal.Round(failPerMonth - reviewsPerMonth, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture), $"Review velocity is below {failPerMonth.ToString("0.00", CultureInfo.InvariantCulture)} reviews per month over the last {lookbackDays} days.");
        if (reviewsPerMonth < warningPerMonth)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, reviewsPerMonth.ToString("0.00", CultureInfo.InvariantCulture), $">= {warningPerMonth.ToString("0.00", CultureInfo.InvariantCulture)}", decimal.Round(warningPerMonth - reviewsPerMonth, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture), $"Review velocity is below the recommended {warningPerMonth.ToString("0.00", CultureInfo.InvariantCulture)} reviews per month over the last {lookbackDays} days.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, reviewsPerMonth.ToString("0.00", CultureInfo.InvariantCulture), $">= {warningPerMonth.ToString("0.00", CultureInfo.InvariantCulture)}", "0.00", "Review velocity meets the current target band.");
    }

    private SeoAuditEvaluationResult EvaluateBurstyReviews(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var lookbackMonths = GetIntParameter(rule, "LookbackMonths", 6);
        var dominantMonthPct = GetDecimalParameter(rule, "DominantMonthPct", 70m);
        var minimumReviews = GetIntParameter(rule, "MinimumReviews", 6);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime.Date;
        var startMonthUtc = new DateTime(nowUtc.Year, nowUtc.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(-(lookbackMonths - 1));
        var nextMonthUtc = startMonthUtc.AddMonths(lookbackMonths);

        var reviews = context.Reviews
            .Where(x => x.ReviewTimestampUtc.HasValue)
            .Select(x => x.ReviewTimestampUtc!.Value)
            .Where(x => x >= startMonthUtc && x < nextMonthUtc)
            .ToList();

        if (reviews.Count < minimumReviews)
            return BuildNotApplicable(rule, $"Fewer than {minimumReviews} timestamped reviews exist in the last {lookbackMonths} calendar months.");

        var monthlyBuckets = reviews
            .GroupBy(x => new { x.Year, x.Month })
            .Select(x => x.Count())
            .ToList();
        var dominantMonthCount = monthlyBuckets.Max();
        var dominantPct = decimal.Round(dominantMonthCount * 100m / reviews.Count, 1, MidpointRounding.AwayFromZero);

        if (dominantPct > dominantMonthPct)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{dominantPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"<= {dominantMonthPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"{decimal.Round(dominantPct - dominantMonthPct, 1, MidpointRounding.AwayFromZero).ToString("0.0", CultureInfo.InvariantCulture)}%", $"More than {dominantMonthPct.ToString("0.0", CultureInfo.InvariantCulture)}% of recent reviews landed in a single calendar month.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{dominantPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"<= {dominantMonthPct.ToString("0.0", CultureInfo.InvariantCulture)}%", "0.0%", "Recent review activity is not overly concentrated in a single month.");
    }

    private static SeoAuditEvaluationResult EvaluateRatingTrendingDownward(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var latestWindowSize = GetIntParameter(rule, "LatestWindowSize", 10);
        var minimumPreviousReviewCount = GetIntParameter(rule, "MinimumPreviousReviewCount", 5);
        var ratedReviews = context.Reviews
            .Where(x => x.ReviewTimestampUtc.HasValue && x.Rating.HasValue)
            .OrderByDescending(x => x.ReviewTimestampUtc)
            .ToList();

        if (ratedReviews.Count < latestWindowSize + minimumPreviousReviewCount)
            return BuildNotApplicable(rule, $"At least {latestWindowSize + minimumPreviousReviewCount} timestamped, rated reviews are required.");

        var latestAverage = ratedReviews.Take(latestWindowSize).Average(x => x.Rating!.Value);
        var previousReviews = ratedReviews.Skip(latestWindowSize).ToList();
        if (previousReviews.Count < minimumPreviousReviewCount)
            return BuildNotApplicable(rule, $"At least {minimumPreviousReviewCount} older rated reviews are required.");

        var previousAverage = previousReviews.Average(x => x.Rating!.Value);
        var change = decimal.Round(latestAverage - previousAverage, 2, MidpointRounding.AwayFromZero);
        if (change < 0m)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{latestAverage.ToString("0.00", CultureInfo.InvariantCulture)} vs {previousAverage.ToString("0.00", CultureInfo.InvariantCulture)}", "Stable or improving rating trend", Math.Abs(change).ToString("0.00", CultureInfo.InvariantCulture), "Average rating in the latest review window is lower than the older review baseline.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{latestAverage.ToString("0.00", CultureInfo.InvariantCulture)} vs {previousAverage.ToString("0.00", CultureInfo.InvariantCulture)}", "Stable or improving rating trend", "0.00", "Average rating trend is stable or improving.");
    }

    private static SeoAuditEvaluationResult EvaluateLowEngagementOnReviews(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var maximumLowRatingPct = GetDecimalParameter(rule, "MaximumLowRatingPct", 30m);
        var lowRatingThreshold = GetDecimalParameter(rule, "LowRatingThreshold", 3m);
        var ratedReviewCount = context.Reviews.Count(x => x.Rating.HasValue);
        if (ratedReviewCount <= 0)
            return BuildNotApplicable(rule, "No stored review ratings are available.");

        var lowRatingCount = context.Reviews.Count(x => x.Rating.HasValue && x.Rating.Value <= lowRatingThreshold);
        var lowRatingPct = decimal.Round(lowRatingCount * 100m / ratedReviewCount, 1, MidpointRounding.AwayFromZero);
        if (lowRatingPct > maximumLowRatingPct)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{lowRatingPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"<= {maximumLowRatingPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"{decimal.Round(lowRatingPct - maximumLowRatingPct, 1, MidpointRounding.AwayFromZero).ToString("0.0", CultureInfo.InvariantCulture)}%", $"More than {maximumLowRatingPct.ToString("0.0", CultureInfo.InvariantCulture)}% of stored reviews are {lowRatingThreshold.ToString("0.0", CultureInfo.InvariantCulture)} stars or below.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{lowRatingPct.ToString("0.0", CultureInfo.InvariantCulture)}%", $"<= {maximumLowRatingPct.ToString("0.0", CultureInfo.InvariantCulture)}%", "0.0%", "Low-rating review share is within the current target.");
    }

    private static SeoAuditEvaluationResult EvaluateBusinessHoursMissing(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return context.HasRegularOpeningHours
            ? BuildResult(rule, SeoAuditStatuses.Pass, 0, "Hours present", "Hours present", "0", "Regular opening hours are stored.")
            : BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "Hours missing", "Hours present", "1 missing hours section", "Regular opening hours are missing.");
    }

    private static SeoAuditEvaluationResult EvaluateMissingDescription(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.DescriptionLength == 0)
        {
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0", "Description required", "1 missing description", "No business description is stored.");
        }

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, context.DescriptionLength.ToString(CultureInfo.InvariantCulture), "At least 1 character", "0 missing", $"Description is present ({context.DescriptionLength} characters).");
    }

    private static SeoAuditEvaluationResult EvaluateDescriptionTooShort(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.DescriptionLength == 0)
            return BuildNotApplicable(rule, "Description length is not applicable until a description exists.");

        var threshold = GetIntParameter(rule, "MinimumLength", 495);
        var gap = Math.Max(0, threshold - context.DescriptionLength);
        if (context.DescriptionLength < threshold)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, context.DescriptionLength.ToString(CultureInfo.InvariantCulture), threshold.ToString(CultureInfo.InvariantCulture), gap.ToString(CultureInfo.InvariantCulture), $"Description is {gap} characters short of the recommended {threshold} characters.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, context.DescriptionLength.ToString(CultureInfo.InvariantCulture), threshold.ToString(CultureInfo.InvariantCulture), "0", $"Description length meets the current recommendation ({context.DescriptionLength}/{threshold}+).");
    }

    private static SeoAuditEvaluationResult EvaluateMissingSecondaryCategories(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.OtherCategoryCount <= 0)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0", "At least 1 secondary category", "1 missing", "No secondary categories are stored.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, context.OtherCategoryCount.ToString(CultureInfo.InvariantCulture), "At least 1 secondary category", "0", $"{context.OtherCategoryCount} secondary categor{(context.OtherCategoryCount == 1 ? "y is" : "ies are")} stored.");
    }

    private static SeoAuditEvaluationResult EvaluateNoResponses(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.ReviewCount <= 0)
            return BuildNotApplicable(rule, "No reviews are stored for this place.");

        if (context.RespondedReviewCount <= 0)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0 responded", $"{context.ReviewCount} reviews responded", context.ReviewCount.ToString(CultureInfo.InvariantCulture), "Reviews exist, but none have an owner response.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, $"{context.RespondedReviewCount} responded", "More than 0 responded reviews", "0", $"{context.RespondedReviewCount} of {context.ReviewCount} reviews have an owner response.");
    }

    private static SeoAuditEvaluationResult EvaluateResponseCoverage(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.ReviewCount <= 0)
            return BuildNotApplicable(rule, "No reviews are stored for this place.");
        if (context.RespondedReviewCount <= 0)
            return BuildNotApplicable(rule, "Coverage is not scored separately when there are zero review responses.");

        var responsePct = decimal.Round(context.RespondedReviewCount * 100m / context.ReviewCount, 1, MidpointRounding.AwayFromZero);
        if (context.RespondedReviewCount < context.ReviewCount)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, $"{responsePct.ToString("0.0", CultureInfo.InvariantCulture)}%", "100.0%", $"{decimal.Round(100m - responsePct, 1, MidpointRounding.AwayFromZero).ToString("0.0", CultureInfo.InvariantCulture)}%", $"{context.RespondedReviewCount} of {context.ReviewCount} reviews have responses.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, "100.0%", "100.0%", "0.0%", "Every stored review has an owner response.");
    }

    private static SeoAuditEvaluationResult EvaluateResponseTime(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (context.ResponseTimings.Count == 0)
            return BuildNotApplicable(rule, "No responded reviews with timestamps are available.");

        var maxWarningDays = GetIntParameter(rule, "MaximumWarningDays", 3);
        var sortedDays = context.ResponseTimings.Select(x => x.ResponseDays).OrderBy(x => x).ToList();
        var averageDays = sortedDays.Average();
        var medianDays = ComputeMedian(sortedDays);
        var breachedCount = context.ResponseTimings.Count(x => x.CalendarDayDiff > maxWarningDays);
        var exceededPct = decimal.Round(breachedCount * 100m / context.ResponseTimings.Count, 1, MidpointRounding.AwayFromZero);
        var hasSameDayOnly = context.ResponseTimings.All(x => x.CalendarDayDiff == 0);
        var hasWarningRange = context.ResponseTimings.Any(x => x.CalendarDayDiff >= 1 && x.CalendarDayDiff <= maxWarningDays);
        var actualValue = $"avg {averageDays.ToString("0.0", CultureInfo.InvariantCulture)}d, median {medianDays.ToString("0.0", CultureInfo.InvariantCulture)}d, >{maxWarningDays}d {exceededPct.ToString("0.0", CultureInfo.InvariantCulture)}%";
        var expectedValue = $"avg 0.0d, median 0.0d, >{maxWarningDays}d 0.0%";
        var gapValue = $"{breachedCount} late response{(breachedCount == 1 ? string.Empty : "s")}";

        if (breachedCount > 0)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, actualValue, expectedValue, gapValue, $"{breachedCount} responded review{(breachedCount == 1 ? string.Empty : "s")} exceeded {maxWarningDays} days.");
        if (hasWarningRange && !hasSameDayOnly)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, actualValue, expectedValue, "Responses took 1 to 3 days", "Responses are being sent, but not always on the same day.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, actualValue, expectedValue, "0", "All measured owner responses were sent on the same day.");
    }

    private static SeoAuditEvaluationResult EvaluateNoWebsite(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return context.WebsiteType switch
        {
            WebsiteType.RealWebsite => BuildResult(rule, SeoAuditStatuses.Pass, 0, "Proper website present", "Proper website present", "0", "A proper business website is stored."),
            WebsiteType.SocialProfile => BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "Social profile URL only", "Proper website present", "1 missing proper website", "Business profile does not provide a proper website. Social media profile links do not count as a business website."),
            _ => BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "No website", "Proper website present", "1 missing proper website", "No proper business website is stored.")
        };
    }

    private static SeoAuditEvaluationResult EvaluateNoQas(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return context.QuestionAnswerCount > 0
            ? BuildResult(rule, SeoAuditStatuses.Pass, 0, context.QuestionAnswerCount.ToString(CultureInfo.InvariantCulture), "At least 1 Q&A", "0", $"{context.QuestionAnswerCount} question/answer row{(context.QuestionAnswerCount == 1 ? string.Empty : "s")} are stored.")
            : BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0", "At least 1 Q&A", "1 missing Q&A section", "No question and answer entries are stored.");
    }

    private static SeoAuditEvaluationResult EvaluateNoUpdates(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return context.UpdateCount > 0
            ? BuildResult(rule, SeoAuditStatuses.Pass, 0, context.UpdateCount.ToString(CultureInfo.InvariantCulture), "At least 1 update", "0", $"{context.UpdateCount} update{(context.UpdateCount == 1 ? string.Empty : "s")} are stored.")
            : BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, "0", "At least 1 update", "1 missing update stream", "No updates are stored.");
    }

    private static SeoAuditEvaluationResult EvaluateNoPhotos(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return context.PhotoCount.GetValueOrDefault() > 0
            ? BuildResult(rule, SeoAuditStatuses.Pass, 0, context.PhotoCount!.Value.ToString(CultureInfo.InvariantCulture), "At least 1 photo", "0", $"{context.PhotoCount.Value} photo{(context.PhotoCount.Value == 1 ? string.Empty : "s")} are recorded.")
            : BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, context.PhotoCount?.ToString(CultureInfo.InvariantCulture) ?? "0", "At least 1 photo", "1 missing photo set", "No photo count is stored for this place.");
    }

    private static SeoAuditEvaluationResult EvaluateRating(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var minimumRating = GetDecimalParameter(rule, "MinimumRating", 4.0m);
        if (!context.LatestRating.HasValue)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, "No rating", minimumRating.ToString("0.0", CultureInfo.InvariantCulture), "Rating unavailable", "No current rating is stored.");
        if (context.LatestRating.Value < minimumRating)
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, context.LatestRating.Value.ToString("0.0", CultureInfo.InvariantCulture), minimumRating.ToString("0.0", CultureInfo.InvariantCulture), decimal.Round(minimumRating - context.LatestRating.Value, 1, MidpointRounding.AwayFromZero).ToString("0.0", CultureInfo.InvariantCulture), $"Current rating is below {minimumRating.ToString("0.0", CultureInfo.InvariantCulture)}.");

        return BuildResult(rule, SeoAuditStatuses.Pass, 0, context.LatestRating.Value.ToString("0.0", CultureInfo.InvariantCulture), minimumRating.ToString("0.0", CultureInfo.InvariantCulture), "0.0", "Current rating meets the minimum target.");
    }

    private static decimal ComputeMedian(IReadOnlyList<double> sortedDays)
    {
        if (sortedDays.Count == 0)
            return 0m;
        if (sortedDays.Count % 2 == 1)
            return decimal.Round((decimal)sortedDays[sortedDays.Count / 2], 1, MidpointRounding.AwayFromZero);

        var upper = sortedDays[sortedDays.Count / 2];
        var lower = sortedDays[(sortedDays.Count / 2) - 1];
        return decimal.Round(((decimal)lower + (decimal)upper) / 2m, 1, MidpointRounding.AwayFromZero);
    }

    private static int GetIntParameter(SeoAuditRuleDefinition rule, string parameterName, int fallback)
    {
        var raw = rule.Parameters.FirstOrDefault(x => x.IsActive && string.Equals(x.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))?.ParameterValue;
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : fallback;
    }

    private static decimal GetDecimalParameter(SeoAuditRuleDefinition rule, string parameterName, decimal fallback)
    {
        var raw = rule.Parameters.FirstOrDefault(x => x.IsActive && string.Equals(x.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))?.ParameterValue;
        return decimal.TryParse(raw, NumberStyles.Number, CultureInfo.InvariantCulture, out var value) ? value : fallback;
    }

    private static SeoAuditEvaluationResult BuildNotApplicable(SeoAuditRuleDefinition rule, string summaryText)
    {
        return BuildResult(rule, SeoAuditStatuses.NotApplicable, 0, null, null, null, summaryText);
    }

    private static SeoAuditEvaluationResult BuildResult(SeoAuditRuleDefinition rule, string status, int scoreImpactApplied, string? actualValue, string? expectedValue, string? gapValue, string summaryText)
    {
        return new SeoAuditEvaluationResult(
            rule.SeoAuditRuleId,
            rule.RuleKey,
            status,
            Math.Max(0, scoreImpactApplied),
            NormalizeNullable(actualValue),
            NormalizeNullable(expectedValue),
            NormalizeNullable(gapValue),
            summaryText.Trim(),
            NormalizeNullable(rule.WhyItMattersText),
            NormalizeNullable(rule.RecommendedActionText),
            rule.SortOrder);
    }

    private static string? NormalizeNullable(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }
}
