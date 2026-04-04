namespace LocalSeo.Web.Models;

public static class SeoAuditRuleModes
{
    public const string Fixed = "Fixed";
    public const string Benchmark = "Benchmark";
    public const string CompetitorRelative = "CompetitorRelative";

    public static readonly IReadOnlyList<string> All =
    [
        Fixed,
        Benchmark,
        CompetitorRelative
    ];
}

public static class SeoAuditStatuses
{
    public const string Pass = "Pass";
    public const string Warning = "Warning";
    public const string Fail = "Fail";
    public const string NotApplicable = "NotApplicable";
}

public static class SeoAuditEntityTypes
{
    public const string GbpProfile = "GBP_PROFILE";
    public const string GbpReviews = "GBP_REVIEWS";
    public const string GbpCategories = "GBP_CATEGORIES";
    public const string GbpQa = "GBP_QA";
    public const string GbpUpdates = "GBP_UPDATES";
    public const string GbpPhotos = "GBP_PHOTOS";
    public const string Website = "WEBSITE";

    public static readonly IReadOnlyList<string> All =
    [
        GbpProfile,
        GbpReviews,
        GbpCategories,
        GbpQa,
        GbpUpdates,
        GbpPhotos,
        Website
    ];
}

public static class SeoAuditRuleTypes
{
    public const string MissingField = "MissingField";
    public const string MinLength = "MinLength";
    public const string MissingSecondaryCategories = "MissingSecondaryCategories";
    public const string NoResponses = "NoResponses";
    public const string ResponseCoverage = "ResponseCoverage";
    public const string ResponseTime = "ResponseTime";
    public const string MissingWebsite = "MissingWebsite";
    public const string MissingQa = "MissingQa";
    public const string MissingUpdates = "MissingUpdates";
    public const string MissingPhotos = "MissingPhotos";
    public const string RatingThreshold = "RatingThreshold";
    public const string ReviewRecency = "ReviewRecency";
    public const string ReviewCountThreshold = "ReviewCountThreshold";
    public const string TextReviewCoverage = "TextReviewCoverage";
    public const string RecentResponseCoverage = "RecentResponseCoverage";
    public const string PostRecency = "PostRecency";
    public const string PhotoCountThreshold = "PhotoCountThreshold";
    public const string QaAnswerCoverage = "QaAnswerCoverage";
    public const string ReviewVelocity = "ReviewVelocity";
    public const string ReviewBurstiness = "ReviewBurstiness";
    public const string RatingTrend = "RatingTrend";
    public const string LowRatingShare = "LowRatingShare";
    public const string MissingHours = "MissingHours";
    public const string TownCentreDistanceRank = "TownCentreDistanceRank";
    public const string BusinessTitleKeywordMatch = "BusinessTitleKeywordMatch";
    public const string PrimaryCategoryMatch = "PrimaryCategoryMatch";
    public const string PhysicalAddressInSearchTown = "PhysicalAddressInSearchTown";
    public const string HomepageNapMatch = "HomepageNapMatch";
    public const string HomepageTitleKeywordMatch = "HomepageTitleKeywordMatch";
    public const string HomepageHeadingKeywordMatch = "HomepageHeadingKeywordMatch";
    public const string HomepageNicheFocus = "HomepageNicheFocus";
    public const string HomepageTopicalKeywordRelevance = "HomepageTopicalKeywordRelevance";
    public const string HomepageInternalLinking = "HomepageInternalLinking";
    public const string HomepageAnchorTextKeywordMatch = "HomepageAnchorTextKeywordMatch";
    public const string HomepageHttpsDefault = "HomepageHttpsDefault";
    public const string DomainKeywordMatch = "DomainKeywordMatch";

    public static readonly IReadOnlyList<string> All =
    [
        MissingField,
        MinLength,
        MissingSecondaryCategories,
        NoResponses,
        ResponseCoverage,
        ResponseTime,
        MissingWebsite,
        MissingQa,
        MissingUpdates,
        MissingPhotos,
        RatingThreshold,
        ReviewRecency,
        ReviewCountThreshold,
        TextReviewCoverage,
        RecentResponseCoverage,
        PostRecency,
        PhotoCountThreshold,
        QaAnswerCoverage,
        ReviewVelocity,
        ReviewBurstiness,
        RatingTrend,
        LowRatingShare,
        MissingHours,
        TownCentreDistanceRank,
        BusinessTitleKeywordMatch,
        PrimaryCategoryMatch,
        PhysicalAddressInSearchTown,
        HomepageNapMatch,
        HomepageTitleKeywordMatch,
        HomepageHeadingKeywordMatch,
        HomepageNicheFocus,
        HomepageTopicalKeywordRelevance,
        HomepageInternalLinking,
        HomepageAnchorTextKeywordMatch,
        HomepageHttpsDefault,
        DomainKeywordMatch
    ];
}

public static class SeoAuditSeverityLevels
{
    public const string Info = "Info";
    public const string Opportunity = "Opportunity";
    public const string Warning = "Warning";
    public const string Critical = "Critical";

    public static readonly IReadOnlyList<string> All =
    [
        Info,
        Opportunity,
        Warning,
        Critical
    ];
}

public static class SeoAuditParameterValueTypes
{
    public const string String = "String";
    public const string Int = "Int";
    public const string Decimal = "Decimal";
    public const string Bool = "Bool";

    public static readonly IReadOnlyList<string> All =
    [
        String,
        Int,
        Decimal,
        Bool
    ];
}

public static class SeoAuditRuleKeys
{
    public const string MissingDescription = "MissingDescription";
    public const string DescriptionTooShort = "DescriptionTooShort";
    public const string MissingSecondaryCategories = "MissingSecondaryCategories";
    public const string NoResponsesToReviews = "NoResponsesToReviews";
    public const string NotAlwaysRespondingToReviews = "NotAlwaysRespondingToReviews";
    public const string TimeToLeaveReviewResponse = "TimeToLeaveReviewResponse";
    public const string NoWebsite = "NoWebsite";
    public const string NoQas = "NoQas";
    public const string NoUpdates = "NoUpdates";
    public const string NoPhotos = "NoPhotos";
    public const string OverallRatingBelow4 = "OverallRatingBelow4";
    public const string NoRecentReviews = "NoRecentReviews";
    public const string LowReviewCount = "LowReviewCount";
    public const string NoReviewsWithText = "NoReviewsWithText";
    public const string NoOwnerResponsesToRecentReviews = "NoOwnerResponsesToRecentReviews";
    public const string NoRecentPosts = "NoRecentPosts";
    public const string VeryFewPhotos = "VeryFewPhotos";
    public const string QasPresentButUnanswered = "QasPresentButUnanswered";
    public const string ReviewVelocity = "ReviewVelocity";
    public const string BurstyReviews = "BurstyReviews";
    public const string RatingTrendingDownward = "RatingTrendingDownward";
    public const string LowEngagementOnReviews = "LowEngagementOnReviews";
    public const string BusinessHoursMissing = "BusinessHoursMissing";
    public const string TownCentreDistanceRelative = "TownCentreDistanceRelative";
    public const string KeywordsInBusinessTitle = "KeywordsInBusinessTitle";
    public const string PrimaryCategoryMatchesRun = "PrimaryCategoryMatchesRun";
    public const string PhysicalAddressMatchesSearchTown = "PhysicalAddressMatchesSearchTown";
    public const string HtmlNapMatchesGbpNap = "HtmlNapMatchesGbpNap";
    public const string KeywordsInLandingPageTitleTag = "KeywordsInLandingPageTitleTag";
    public const string KeywordsInLandingPageHeadings = "KeywordsInLandingPageHeadings";
    public const string HomepageNicheFocus = "HomepageNicheFocus";
    public const string HomepageTopicalKeywordRelevance = "HomepageTopicalKeywordRelevance";
    public const string HomepageInternalLinking = "HomepageInternalLinking";
    public const string KeywordsInInternalLinkAnchorText = "KeywordsInInternalLinkAnchorText";
    public const string WebsiteUsesHttpsByDefault = "WebsiteUsesHttpsByDefault";
    public const string KeywordsInDomainName = "KeywordsInDomainName";
}

public sealed class SeoAuditRuleListRow
{
    public long SeoAuditRuleId { get; init; }
    public string RuleKey { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public string RuleGroup { get; init; } = string.Empty;
    public string RuleMode { get; init; } = SeoAuditRuleModes.Fixed;
    public string RuleType { get; init; } = string.Empty;
    public string EntityType { get; init; } = string.Empty;
    public string Severity { get; init; } = string.Empty;
    public int WarningScoreImpact { get; init; }
    public int FailScoreImpact { get; init; }
    public int SortOrder { get; init; }
    public bool IsActive { get; init; }
    public bool ShowInActionsTab { get; init; }
    public int ParameterCount { get; init; }
    public DateTime UpdatedAtUtc { get; init; }
}

public sealed class SeoAuditRuleParameterEditModel
{
    public long? SeoAuditRuleParameterId { get; set; }
    public string ParameterName { get; set; } = string.Empty;
    public string ParameterValue { get; set; } = string.Empty;
    public string ValueType { get; set; } = SeoAuditParameterValueTypes.String;
    public int SortOrder { get; set; }
    public bool IsActive { get; set; } = true;
}

public sealed class SeoAuditRuleEditModel
{
    public long? SeoAuditRuleId { get; set; }
    public string RuleKey { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string RuleGroup { get; set; } = string.Empty;
    public string RuleMode { get; set; } = SeoAuditRuleModes.Fixed;
    public string RuleType { get; set; } = string.Empty;
    public string EntityType { get; set; } = string.Empty;
    public string Severity { get; set; } = SeoAuditSeverityLevels.Warning;
    public int WarningScoreImpact { get; set; }
    public int FailScoreImpact { get; set; }
    public int SortOrder { get; set; }
    public bool IsActive { get; set; } = true;
    public bool ShowInActionsTab { get; set; } = true;
    public string WhyItMattersText { get; set; } = string.Empty;
    public string RecommendedActionText { get; set; } = string.Empty;
    public List<SeoAuditRuleParameterEditModel> Parameters { get; set; } = [];
}

public sealed class SeoAuditRuleListViewModel
{
    public IReadOnlyList<SeoAuditRuleListRow> Rows { get; init; } = [];
    public string? PlaceIdToRecalculate { get; init; }
}

public sealed class SeoAuditRuleEditViewModel
{
    public string Mode { get; init; } = "edit";
    public string? Message { get; init; }
    public SeoAuditRuleEditModel Rule { get; init; } = new();
}

public sealed class SeoAuditPlaceResultRow
{
    public long SeoAuditRuleId { get; init; }
    public string RuleKey { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public string Severity { get; init; } = string.Empty;
    public string Status { get; init; } = string.Empty;
    public int ScoreImpactApplied { get; init; }
    public int PossiblePoints { get; init; }
    public string? ActualValue { get; init; }
    public string? ExpectedValue { get; init; }
    public string? GapValue { get; init; }
    public string? SummaryText { get; init; }
    public string? WhyItMattersText { get; init; }
    public string? RecommendedActionText { get; init; }
    public int SortOrderSnapshot { get; init; }
    public long? LastSourceSearchRunId { get; init; }
    public DateTime LastEvaluatedAtUtc { get; init; }
}

public sealed class SeoAuditPlaceSummary
{
    public string PlaceId { get; init; } = string.Empty;
    public int ScorePercentage { get; init; }
    public DateTime? LastEvaluatedAtUtc { get; init; }
    public long? LastSourceSearchRunId { get; init; }
    public bool HasResults { get; init; }
    public IReadOnlyList<SeoAuditPlaceResultRow> ActionsNeeded { get; init; } = [];
    public IReadOnlyList<SeoAuditPlaceResultRow> AlreadyGood { get; init; } = [];
    public IReadOnlyList<SeoAuditPlaceResultRow> InformationOnly { get; init; } = [];
    public IReadOnlyList<SeoAuditPlaceResultRow> NotEvaluated { get; init; } = [];
}
