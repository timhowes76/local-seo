using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class SeoAuditFixedRuleHandlerTests
{
    private readonly TestTimeProvider timeProvider = new(new DateTimeOffset(2026, 3, 7, 12, 0, 0, TimeSpan.Zero));
    private readonly FixedSeoAuditRuleHandler handler;

    public SeoAuditFixedRuleHandlerTests()
    {
        handler = new FixedSeoAuditRuleHandler(timeProvider);
    }

    [Fact]
    public void Evaluate_DescriptionTooShort_ReturnsWarning_WhenBelowThreshold()
    {
        var rule = CreateRule(
            SeoAuditRuleKeys.DescriptionTooShort,
            warningScoreImpact: 4,
            failScoreImpact: 8,
            parameters:
            [
                new SeoAuditRuleParameterDefinition(1, 1, "MinimumLength", "495", SeoAuditParameterValueTypes.Int, 10, true)
            ]);
        var context = new PlaceAuditContext(
            "place-1",
            new string('a', 300),
            "https://example.com",
            5,
            2,
            "[\"Category A\"]",
            "[\"Mon-Fri 09:00-17:00\"]",
            4.5m,
            10,
            123,
            10,
            8,
            4,
            2,
            [],
            [],
            [],
            []);

        var result = handler.Evaluate(rule, context);

        Assert.Equal(SeoAuditStatuses.Warning, result.Status);
        Assert.Equal(4, result.ScoreImpactApplied);
        Assert.Contains("195", result.GapValue, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_ResponseTime_ReturnsFail_WhenResponseExceedsThreeDays()
    {
        var rule = CreateRule(
            SeoAuditRuleKeys.TimeToLeaveReviewResponse,
            warningScoreImpact: 4,
            failScoreImpact: 9,
            parameters:
            [
                new SeoAuditRuleParameterDefinition(2, 2, "MaximumWarningDays", "3", SeoAuditParameterValueTypes.Int, 10, true)
            ]);
        var context = new PlaceAuditContext(
            "place-2",
            "Useful description",
            "https://example.com",
            2,
            0,
            "[]",
            "[\"Mon-Fri 09:00-17:00\"]",
            4.7m,
            5,
            321,
            5,
            2,
            1,
            0,
            [
                new ReviewResponseTiming(
                    new DateTime(2026, 3, 1, 9, 0, 0, DateTimeKind.Utc),
                    new DateTime(2026, 3, 5, 9, 0, 0, DateTimeKind.Utc),
                    4d,
                    4)
            ],
            [],
            [],
            []);

        var result = handler.Evaluate(rule, context);

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(9, result.ScoreImpactApplied);
        Assert.Contains("exceeded 3 days", result.SummaryText, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_NoRecentReviews_ReturnsFail_WhenLatestReviewIsOlderThanFailThreshold()
    {
        var rule = CreateRule(
            SeoAuditRuleKeys.NoRecentReviews,
            warningScoreImpact: 6,
            failScoreImpact: 9,
            parameters:
            [
                new SeoAuditRuleParameterDefinition(3, 3, "WarningDays", "30", SeoAuditParameterValueTypes.Int, 10, true),
                new SeoAuditRuleParameterDefinition(4, 3, "FailDays", "90", SeoAuditParameterValueTypes.Int, 20, true)
            ]);
        var context = CreateContext(
            reviewCount: 8,
            latestUserRatingCount: 8,
            reviews:
            [
                new PlaceReviewAuditRow(
                    new DateTime(2025, 11, 1, 9, 0, 0, DateTimeKind.Utc),
                    new DateTime(2025, 11, 1, 9, 0, 0, DateTimeKind.Utc),
                    null,
                    "Great",
                    5m,
                    0,
                    false)
            ]);

        var result = handler.Evaluate(rule, context);

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(9, result.ScoreImpactApplied);
    }

    [Fact]
    public void Evaluate_BusinessHoursMissing_ReturnsFail_WhenHoursJsonIsBlank()
    {
        var rule = CreateRule(
            SeoAuditRuleKeys.BusinessHoursMissing,
            warningScoreImpact: 0,
            failScoreImpact: 8,
            parameters: []);
        var context = CreateContext(regularOpeningHoursJson: "[]");

        var result = handler.Evaluate(rule, context);

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(8, result.ScoreImpactApplied);
    }

    private static SeoAuditRuleDefinition CreateRule(
        string ruleKey,
        int warningScoreImpact,
        int failScoreImpact,
        IReadOnlyList<SeoAuditRuleParameterDefinition> parameters)
    {
        return new SeoAuditRuleDefinition(
            1,
            ruleKey,
            ruleKey,
            string.Empty,
            "Group",
            SeoAuditRuleModes.Fixed,
            SeoAuditRuleTypes.MissingField,
            SeoAuditEntityTypes.GbpProfile,
            SeoAuditSeverityLevels.Warning,
            warningScoreImpact,
            failScoreImpact,
            10,
            true,
            true,
            "Why",
            "Action",
            DateTime.UtcNow,
            DateTime.UtcNow,
            parameters);
    }

    private static PlaceAuditContext CreateContext(
        string? description = "Useful description",
        string? websiteUri = "https://example.com",
        int? photoCount = 5,
        int? storedQuestionAnswerCount = 2,
        string? otherCategoriesJson = "[\"Category A\"]",
        string? regularOpeningHoursJson = "[\"Mon-Fri 09:00-17:00\"]",
        decimal? latestRating = 4.5m,
        int? latestUserRatingCount = 10,
        long? lastSourceSearchRunId = 123,
        int reviewCount = 10,
        int respondedReviewCount = 8,
        int updateCount = 4,
        int qaTableCount = 2,
        IReadOnlyList<ReviewResponseTiming>? responseTimings = null,
        IReadOnlyList<PlaceReviewAuditRow>? reviews = null,
        IReadOnlyList<PlaceUpdateAuditRow>? updates = null,
        IReadOnlyList<PlaceQuestionAnswerAuditRow>? questionsAndAnswers = null)
    {
        return new PlaceAuditContext(
            "place-test",
            description,
            websiteUri,
            photoCount,
            storedQuestionAnswerCount,
            otherCategoriesJson,
            regularOpeningHoursJson,
            latestRating,
            latestUserRatingCount,
            lastSourceSearchRunId,
            reviewCount,
            respondedReviewCount,
            updateCount,
            qaTableCount,
            responseTimings ?? [],
            reviews ?? [],
            updates ?? [],
            questionsAndAnswers ?? []);
    }

    private sealed class TestTimeProvider(DateTimeOffset nowUtc) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => nowUtc;
    }
}
