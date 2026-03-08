using System.Text.Json;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface ISeoAuditService
{
    Task<SeoAuditPlaceSummary?> GetAuditSummaryForPlaceAsync(string placeId, CancellationToken ct);
    Task<IReadOnlyList<SeoAuditPlaceResultRow>> GetAuditResultsForPlaceAsync(string placeId, CancellationToken ct);
    Task<SeoAuditPlaceSummary> EvaluatePlaceAsync(string placeId, CancellationToken ct);
    Task<int> EvaluatePlacesAsync(IEnumerable<string> placeIds, CancellationToken ct);
    Task<int> RecalculateAllMissingAuditResultsAsync(CancellationToken ct);
    Task<int> RecalculateAllAuditResultsAsync(CancellationToken ct);
    Task<SeoAuditPlaceSummary> RecalculateAuditForPlaceAsync(string placeId, CancellationToken ct);
    Task<IReadOnlyList<SeoAuditRuleListRow>> GetAdminRuleListAsync(CancellationToken ct);
    Task<SeoAuditRuleEditModel?> GetAdminRuleEditModelAsync(long ruleId, CancellationToken ct);
    Task<(bool Success, string Message, long? RuleId)> CreateRuleAsync(SeoAuditRuleEditModel model, CancellationToken ct);
    Task<(bool Success, string Message)> UpdateRuleAsync(long ruleId, SeoAuditRuleEditModel model, CancellationToken ct);
    Task<(bool Success, string Message)> ToggleRuleActiveAsync(long ruleId, bool isActive, CancellationToken ct);
}

public interface ISeoAuditRepository
{
    Task<IReadOnlyList<SeoAuditRuleDefinition>> GetAllRulesAsync(CancellationToken ct);
    Task<IReadOnlyList<SeoAuditRuleListRow>> GetAdminRuleListAsync(CancellationToken ct);
    Task<SeoAuditRuleDefinition?> GetRuleByIdAsync(long ruleId, CancellationToken ct);
    Task<long> CreateRuleAsync(SeoAuditRuleUpsertRequest request, CancellationToken ct);
    Task<bool> UpdateRuleAsync(long ruleId, SeoAuditRuleUpsertRequest request, CancellationToken ct);
    Task<bool> SetRuleActiveAsync(long ruleId, bool isActive, CancellationToken ct);
    Task<PlaceAuditContext?> GetPlaceAuditContextAsync(string placeId, CancellationToken ct);
    Task UpsertAuditResultsAsync(string placeId, long? lastSourceSearchRunId, IReadOnlyList<SeoAuditEvaluationResult> results, DateTime nowUtc, CancellationToken ct);
    Task<IReadOnlyList<SeoAuditPlaceResultRow>> GetAuditResultsForPlaceAsync(string placeId, CancellationToken ct);
    Task<IReadOnlyList<string>> GetPlaceIdsMissingResultsAsync(CancellationToken ct);
    Task<IReadOnlyList<string>> GetAllPlaceIdsAsync(CancellationToken ct);
}

public interface ISeoAuditRuleHandler
{
    bool CanEvaluate(SeoAuditRuleDefinition rule);
    SeoAuditEvaluationResult Evaluate(SeoAuditRuleDefinition rule, PlaceAuditContext context);
}

public sealed record SeoAuditRuleDefinition(
    long SeoAuditRuleId,
    string RuleKey,
    string Name,
    string Description,
    string RuleGroup,
    string RuleMode,
    string RuleType,
    string EntityType,
    string Severity,
    int WarningScoreImpact,
    int FailScoreImpact,
    int SortOrder,
    bool IsActive,
    bool ShowInActionsTab,
    string WhyItMattersText,
    string RecommendedActionText,
    DateTime CreatedAtUtc,
    DateTime UpdatedAtUtc,
    IReadOnlyList<SeoAuditRuleParameterDefinition> Parameters);

public sealed record SeoAuditRuleParameterDefinition(
    long SeoAuditRuleParameterId,
    long SeoAuditRuleId,
    string ParameterName,
    string ParameterValue,
    string ValueType,
    int SortOrder,
    bool IsActive);

public sealed record SeoAuditRuleUpsertRequest(
    string RuleKey,
    string Name,
    string? Description,
    string? RuleGroup,
    string RuleMode,
    string RuleType,
    string EntityType,
    string Severity,
    int WarningScoreImpact,
    int FailScoreImpact,
    int SortOrder,
    bool IsActive,
    bool ShowInActionsTab,
    string? WhyItMattersText,
    string? RecommendedActionText,
    IReadOnlyList<SeoAuditRuleParameterUpsertRequest> Parameters,
    DateTime NowUtc);

public sealed record SeoAuditRuleParameterUpsertRequest(
    long? SeoAuditRuleParameterId,
    string ParameterName,
    string ParameterValue,
    string ValueType,
    int SortOrder,
    bool IsActive);

public sealed record SeoAuditEvaluationResult(
    long SeoAuditRuleId,
    string RuleKey,
    string Status,
    int ScoreImpactApplied,
    string? ActualValue,
    string? ExpectedValue,
    string? GapValue,
    string SummaryText,
    string? WhyItMattersText,
    string? RecommendedActionText,
    int SortOrderSnapshot);

public sealed record ReviewResponseTiming(
    DateTime ReviewTimestampUtc,
    DateTime OwnerTimestampUtc,
    double ResponseDays,
    int CalendarDayDiff);

public sealed record PlaceReviewAuditRow(
    DateTime? ReviewTimestampUtc,
    DateTime LastSeenUtc,
    DateTime? OwnerTimestampUtc,
    string? ReviewText,
    decimal? Rating,
    int? PhotosCount,
    bool HasOwnerResponse)
{
    public DateTime EffectiveSortUtc => ReviewTimestampUtc ?? LastSeenUtc;
}

public sealed record PlaceUpdateAuditRow(
    DateTime? EffectiveUpdateUtc);

public sealed record PlaceQuestionAnswerAuditRow(
    string? AnswerText,
    DateTime? AnswerTimestampUtc);

public sealed record PlaceAuditContext(
    string PlaceId,
    string? Description,
    string? WebsiteUri,
    WebsiteType WebsiteType,
    int? PhotoCount,
    int? StoredQuestionAnswerCount,
    string? OtherCategoriesJson,
    string? RegularOpeningHoursJson,
    decimal? LatestRating,
    int? LatestUserRatingCount,
    long? LastSourceSearchRunId,
    int ReviewCount,
    int RespondedReviewCount,
    int UpdateCount,
    int QaTableCount,
    IReadOnlyList<ReviewResponseTiming> ResponseTimings,
    IReadOnlyList<PlaceReviewAuditRow> Reviews,
    IReadOnlyList<PlaceUpdateAuditRow> Updates,
    IReadOnlyList<PlaceQuestionAnswerAuditRow> QuestionsAndAnswers)
{
    public int DescriptionLength => string.IsNullOrWhiteSpace(Description) ? 0 : Description.Trim().Length;

    public bool HasRealWebsite => WebsiteType == WebsiteType.RealWebsite;

    public bool HasSocialProfileOnlyWebsite => WebsiteType == WebsiteType.SocialProfile;

    public int QuestionAnswerCount => Math.Max(StoredQuestionAnswerCount.GetValueOrDefault(), QaTableCount);

    public int TotalReviewCount => Math.Max(LatestUserRatingCount.GetValueOrDefault(), ReviewCount);

    public bool HasRegularOpeningHours => HasJsonContent(RegularOpeningHoursJson);

    public int OtherCategoryCount
    {
        get
        {
            if (string.IsNullOrWhiteSpace(OtherCategoriesJson))
                return 0;

            try
            {
                var values = JsonSerializer.Deserialize<List<string>>(OtherCategoriesJson);
                return values?.Count(x => !string.IsNullOrWhiteSpace(x)) ?? 0;
            }
            catch
            {
                return 0;
            }
        }
    }

    private static bool HasJsonContent(string? rawJson)
    {
        var trimmed = (rawJson ?? string.Empty).Trim();
        if (trimmed.Length == 0 || trimmed is "[]" or "{}")
            return false;

        try
        {
            using var document = JsonDocument.Parse(trimmed);
            return document.RootElement.ValueKind switch
            {
                JsonValueKind.Array => document.RootElement.EnumerateArray().Any(element =>
                    element.ValueKind switch
                    {
                        JsonValueKind.String => !string.IsNullOrWhiteSpace(element.GetString()),
                        JsonValueKind.Null => false,
                        JsonValueKind.Undefined => false,
                        _ => true
                    }),
                JsonValueKind.Object => document.RootElement.EnumerateObject().Any(),
                JsonValueKind.String => !string.IsNullOrWhiteSpace(document.RootElement.GetString()),
                JsonValueKind.Null => false,
                JsonValueKind.Undefined => false,
                _ => true
            };
        }
        catch
        {
            return trimmed.Length > 0 && trimmed != "[]";
        }
    }
}
