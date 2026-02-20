namespace LocalSeo.Web.Models;

public sealed record SearchRun(
    long SearchRunId,
    string CategoryId,
    long TownId,
    long CountyId,
    string SeedKeyword,
    string LocationName,
    decimal? CenterLat,
    decimal? CenterLng,
    int? RadiusMeters,
    int ResultLimit,
    bool FetchDetailedData,
    bool FetchGoogleReviews,
    bool FetchGoogleUpdates,
    bool FetchGoogleQuestionsAndAnswers,
    DateTime RanAtUtc);
public sealed record PlaceSnapshotRow(
    long PlaceSnapshotId,
    long SearchRunId,
    string PlaceId,
    int RankPosition,
    decimal? Rating,
    int? UserRatingCount,
    DateTime CapturedAtUtc,
    string? DisplayName,
    string? PrimaryCategory,
    int? PhotoCount,
    string? NationalPhoneNumber,
    decimal? Lat,
    decimal? Lng,
    string? FormattedAddress,
    string? WebsiteUri,
    int? QuestionAnswerCount,
    int? UpdateCount,
    int? DescriptionLength,
    bool? HasOtherCategories,
    int? ReviewsLast90,
    decimal? AvgPerMonth12m,
    decimal? Trend90Pct,
    int? DaysSinceLastReview,
    int? DaysSinceLastUpdate,
    string? UpdateStatusLabel,
    string? StatusLabel,
    int? MomentumScore)
{
    public int? AvgClicks { get; init; }
    public int? LastMonthClicks { get; init; }
}
public sealed record PlaceHistoryRow(
    long SearchRunId,
    int RankPosition,
    decimal? Rating,
    int? UserRatingCount,
    DateTime CapturedAtUtc,
    string? SeedKeyword,
    string? LocationName);
public sealed record PlaceReviewRow(
    string ReviewId,
    string? ProfileName,
    decimal? Rating,
    string? ReviewText,
    DateTime? ReviewTimestampUtc,
    string? TimeAgo,
    string? OwnerAnswer,
    DateTime? OwnerTimestampUtc,
    string? ReviewUrl,
    DateTime LastSeenUtc);
public sealed record DataForSeoTaskRow(
    long DataForSeoReviewTaskId,
    string DataForSeoTaskId,
    string TaskType,
    string PlaceId,
    string? LocationName,
    string Status,
    int? TaskStatusCode,
    string? TaskStatusMessage,
    string? Endpoint,
    DateTime CreatedAtUtc,
    DateTime? LastCheckedUtc,
    DateTime? ReadyAtUtc,
    DateTime? PopulatedAtUtc,
    DateTime? LastAttemptedPopulateUtc,
    int? LastPopulateReviewCount,
    DateTime? CallbackReceivedAtUtc,
    string? CallbackTaskId,
    string? LastError);
public sealed record RunDetailsViewModel(
    SearchRun Run,
    IReadOnlyList<PlaceSnapshotRow> Snapshots,
    IReadOnlyList<RunTaskProgressRow> TaskProgress)
{
    public RunKeyphraseTrafficSummary? KeyphraseTraffic { get; init; }
}

public sealed record RunKeyphraseTrafficSummary(
    string CategoryId,
    long TownId,
    IReadOnlyList<WeightedSearchVolumePoint> Last12Months,
    int WeightedAvgSearchVolume,
    int? WeightedLastMonthSearchVolume,
    int? LatestMonthYear,
    int? LatestMonthNumber,
    string KeyphrasesUrl);

public sealed record RunReviewComparisonViewModel(
    SearchRun Run,
    IReadOnlyList<RunReviewComparisonRow> Rows,
    IReadOnlyList<RunReviewComparisonSeries> Series);

public sealed record RunReviewComparisonRow(
    string PlaceId,
    int RankPosition,
    string DisplayName,
    decimal? AvgRating,
    int? UserRatingCount,
    int Reviews3m,
    int Reviews6m,
    int Reviews9m,
    int Reviews12m,
    decimal? AvgPerMonth12m,
    int? DaysSinceLastReview,
    int? LongestGapDays12m,
    decimal? RespondedPct12m,
    decimal? AvgOwnerResponseHours12m,
    string StatusLabel);

public sealed record RunReviewComparisonSeries(
    string PlaceId,
    string DisplayName,
    IReadOnlyList<RunReviewComparisonSeriesPoint> Points);

public sealed record RunReviewComparisonSeriesPoint(
    int Year,
    int Month,
    int TotalReviews);

public sealed record RunTaskProgressRow(
    string TaskType,
    string Label,
    int TotalPlaces,
    int DueCount,
    int ProcessingCount,
    int CompletedCount,
    int ErrorCount);

public sealed class PlaceDetailsViewModel
{
    public string PlaceId { get; init; } = string.Empty;
    public string? DisplayName { get; init; }
    public string? SearchLocationName { get; init; }
    public string? ContextSeedKeyword { get; init; }
    public string? ContextLocationName { get; init; }
    public string? FormattedAddress { get; init; }
    public string? PrimaryType { get; init; }
    public string? PrimaryCategory { get; init; }
    public string? NationalPhoneNumber { get; init; }
    public string? WebsiteUri { get; init; }
    public DateTime? OpeningDate { get; init; }
    public decimal? Lat { get; init; }
    public decimal? Lng { get; init; }
    public string? Description { get; init; }
    public int? PhotoCount { get; init; }
    public bool? IsServiceAreaBusiness { get; init; }
    public string? BusinessStatus { get; init; }
    public IReadOnlyList<string> RegularOpeningHours { get; init; } = [];
    public IReadOnlyList<string> OtherCategories { get; init; } = [];
    public IReadOnlyList<string> PlaceTopics { get; init; } = [];
    public long? ActiveRunId { get; init; }
    public int? ActiveRankPosition { get; init; }
    public decimal? ActiveRating { get; init; }
    public int? ActiveUserRatingCount { get; init; }
    public int? QuestionAnswerCount { get; init; }
    public DateTime? ActiveCapturedAtUtc { get; init; }
    public long? MapRunId { get; init; }
    public decimal? RunCenterLat { get; init; }
    public decimal? RunCenterLng { get; init; }
    public IReadOnlyList<PlaceReviewRow> Reviews { get; init; } = [];
    public int ReviewPage { get; init; } = 1;
    public int ReviewPageSize { get; init; } = 25;
    public int TotalReviewCount { get; init; }
    public int TotalReviewPages { get; init; }
    public IReadOnlyList<PlaceUpdateRow> Updates { get; init; } = [];
    public IReadOnlyList<PlaceQuestionAnswerRow> QuestionsAndAnswers { get; init; } = [];
    public IReadOnlyList<PlaceHistoryRow> History { get; init; } = [];
    public IReadOnlyList<PlaceDataTaskStatusRow> DataTaskStatuses { get; init; } = [];
    public PlaceReviewVelocityDetailsDto? ReviewVelocity { get; init; }
    public PlaceUpdateVelocityDetailsDto? UpdateVelocity { get; init; }
    public PlaceEstimatedTrafficSummary? EstimatedTraffic { get; init; }
    public bool ZohoLeadCreated { get; init; }
    public DateTime? ZohoLeadCreatedAtUtc { get; init; }
    public string? ZohoLeadId { get; init; }
    public DateTime? ZohoLastSyncAtUtc { get; init; }
    public string? ZohoLastError { get; init; }
}

public sealed record PlaceEstimatedTrafficSummary(
    int CurrentMapPosition,
    int EstimatedMonthlyMapVisits,
    int EstimatedVisitsAtPosition3,
    int EstimatedVisitsAtPosition1,
    int? OpportunityToPosition3,
    int? OpportunityToPosition1);

public sealed record PlaceVelocityListItemDto(
    string PlaceId,
    int? RankPosition,
    string? DisplayName,
    string? PrimaryCategory,
    decimal? Rating,
    int? UserRatingCount,
    int? ReviewsLast90,
    int? DaysSinceLastReview,
    string? StatusLabel,
    int? UpdateCount,
    int? DaysSinceLastUpdate,
    string? UpdateStatusLabel,
    bool? HasWebsite,
    int? DescriptionLength,
    bool? HasOtherCategories,
    int? PhotoCount,
    int? QuestionAnswerCount);

public sealed record PlacesRunFilterOptions(
    IReadOnlyList<string> Keywords,
    IReadOnlyList<string> Locations);

public sealed class PlacesIndexViewModel
{
    public IReadOnlyList<PlaceVelocityListItemDto> Rows { get; init; } = [];
    public IReadOnlyList<string> KeywordOptions { get; init; } = [];
    public IReadOnlyList<string> LocationOptions { get; init; } = [];
    public string? SelectedKeyword { get; init; }
    public string? SelectedLocation { get; init; }
    public string? PlaceNameQuery { get; init; }
    public string? Sort { get; init; }
    public string? Direction { get; init; }
}

public sealed record PlaceReviewVelocityDetailsDto(
    string PlaceId,
    DateTime? AsOfUtc,
    int? ReviewsLast90,
    int? ReviewsLast180,
    int? ReviewsLast270,
    int? ReviewsLast365,
    decimal? AvgPerMonth12m,
    int? Prev90,
    decimal? Trend90Pct,
    int? DaysSinceLastReview,
    DateTime? LastReviewTimestampUtc,
    int? LongestGapDays12m,
    decimal? RespondedPct12m,
    decimal? AvgOwnerResponseHours12m,
    int? MomentumScore,
    string? StatusLabel,
    IReadOnlyList<MonthlyReviewCountDto> MonthlySeries,
    IReadOnlyList<YearReviewBreakdownDto> YearBreakdown,
    CompetitorVelocityBlockDto? CompetitorBlock);

public sealed record MonthlyReviewCountDto(int Year, int Month, int ReviewCount);

public sealed record YearReviewBreakdownDto(int Year, int ReviewCount, decimal? AvgRating, decimal? RespondedPct);

public sealed record CompetitorVelocityBlockDto(
    IReadOnlyList<CompetitorVelocityItemDto> Competitors,
    decimal? CompetitorAverageReviewsPerMonth12m);

public sealed record CompetitorVelocityItemDto(
    string Name,
    int ReviewsLast365,
    decimal? AvgRating,
    int? DaysSinceLastReview);

public sealed class SearchFormModel
{
    public string CategoryId { get; set; } = string.Empty;
    public long? CountyId { get; set; }
    public long? TownId { get; set; }
    public int RadiusMeters { get; set; } = 5000;
    public int ResultLimit { get; set; } = 20;
    public bool FetchEnhancedGoogleData { get; set; }
    public bool FetchGoogleReviews { get; set; }
    public bool FetchGoogleUpdates { get; set; }
    public bool FetchGoogleQuestionsAndAnswers { get; set; }
    public IReadOnlyList<GoogleBusinessProfileCategoryLookupItem> CategoryOptions { get; set; } = [];
    public IReadOnlyList<GbCountyLookupItem> CountyOptions { get; set; } = [];
    public IReadOnlyList<GbTownLookupItem> TownOptions { get; set; } = [];
    public CategoryKeyphrasesViewModel? Keyphrases { get; set; }
    public string? KeyphrasesError { get; set; }
    public long? RerunSourceRunId { get; set; }
}

public sealed record PlaceUpdateRow(
    string UpdateKey,
    string? PostText,
    string? Url,
    IReadOnlyList<string> ImageUrls,
    DateTime? PostDateUtc,
    IReadOnlyList<PlaceUpdateLinkRow> Links,
    DateTime LastSeenUtc);

public sealed record PlaceUpdateLinkRow(string? Type, string? Title, string? Url);

public sealed record PlaceQuestionAnswerRow(
    string QaKey,
    string? QuestionText,
    DateTime? QuestionTimestampUtc,
    string? QuestionProfileName,
    string? AnswerText,
    DateTime? AnswerTimestampUtc,
    string? AnswerProfileName,
    DateTime LastSeenUtc);

public sealed record PlaceDataTaskStatusRow(
    string TaskType,
    string Label,
    string? Status,
    long? DataForSeoTaskRowId,
    DateTime? LastRunAtUtc,
    string LastRunAgeLabel,
    bool IsReady,
    bool CanRefresh,
    int RefreshThresholdHours);

public sealed record PlaceUpdateVelocityDetailsDto(
    string PlaceId,
    DateTime? AsOfUtc,
    int? UpdatesLast90,
    int? UpdatesLast180,
    int? UpdatesLast270,
    int? UpdatesLast365,
    int? Prev90,
    decimal? Trend90Pct,
    int? DaysSinceLastUpdate,
    DateTime? LastUpdateTimestampUtc,
    string? StatusLabel,
    IReadOnlyList<MonthlyUpdateCountDto> MonthlySeries,
    IReadOnlyList<YearUpdateBreakdownDto> YearBreakdown);

public sealed record MonthlyUpdateCountDto(int Year, int Month, int UpdateCount);

public sealed record YearUpdateBreakdownDto(int Year, int UpdateCount);

public sealed class AdminSettingsModel
{
    public int EnhancedGoogleDataRefreshHours { get; set; } = 24;
    public int GoogleReviewsRefreshHours { get; set; } = 24;
    public int GoogleUpdatesRefreshHours { get; set; } = 24;
    public int GoogleQuestionsAndAnswersRefreshHours { get; set; } = 24;
    public int SearchVolumeRefreshCooldownDays { get; set; } = 30;
    public int MapPackClickSharePercent { get; set; } = 50;
    public int MapPackCtrPosition1Percent { get; set; } = 38;
    public int MapPackCtrPosition2Percent { get; set; } = 23;
    public int MapPackCtrPosition3Percent { get; set; } = 16;
    public int MapPackCtrPosition4Percent { get; set; } = 7;
    public int MapPackCtrPosition5Percent { get; set; } = 5;
    public int MapPackCtrPosition6Percent { get; set; } = 4;
    public int MapPackCtrPosition7Percent { get; set; } = 3;
    public int MapPackCtrPosition8Percent { get; set; } = 2;
    public int MapPackCtrPosition9Percent { get; set; } = 1;
    public int MapPackCtrPosition10Percent { get; set; } = 1;
    public string ZohoLeadOwnerName { get; set; } = "Richard Howes";
    public string ZohoLeadOwnerId { get; set; } = "1108404000000068001";
    public string ZohoLeadNextAction { get; set; } = "Make first contact";
}

public sealed record GooglePlace(
    string Id,
    string? DisplayName,
    string? PrimaryType,
    string? PrimaryCategory,
    string TypesCsv,
    decimal? Rating,
    int? UserRatingCount,
    string? FormattedAddress,
    decimal? Lat,
    decimal? Lng,
    string? NationalPhoneNumber,
    string? WebsiteUri,
    string? Description,
    int? PhotoCount,
    bool? IsServiceAreaBusiness,
    string? BusinessStatus,
    IReadOnlyList<string> RegularOpeningHours);
