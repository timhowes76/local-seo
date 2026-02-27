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
    bool FetchGoogleSocialProfiles,
    bool FetchAppleBing,
    DateTime RanAtUtc);

public sealed record SearchRunProgressSnapshot(
    long SearchRunId,
    string Status,
    int? TotalApiCalls,
    int? CompletedApiCalls,
    int? PercentComplete,
    DateTime? StartedUtc,
    DateTime? LastUpdatedUtc,
    DateTime? CompletedUtc,
    string? ErrorMessage);

public sealed class SearchProgressPageModel
{
    public long RunId { get; init; }
    public string ProgressStreamUrl { get; init; } = string.Empty;
    public string CompletedRedirectUrl { get; init; } = string.Empty;
    public string RetryUrl { get; init; } = string.Empty;
    public SearchRunProgressSnapshot Initial { get; init; } = new(
        0,
        "Queued",
        null,
        0,
        0,
        null,
        null,
        null,
        null);
}
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
    int? MomentumScore,
    string? LogoUrl,
    bool HasFinancialInfo,
    bool IsZohoConnected)
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
    public string? LogoUrl { get; init; }
    public string? MainPhotoUrl { get; init; }
    public string? SearchLocationName { get; init; }
    public string? ContextSeedKeyword { get; init; }
    public string? ContextLocationName { get; init; }
    public string? FormattedAddress { get; init; }
    public string? PrimaryType { get; init; }
    public string? PrimaryCategory { get; init; }
    public string? NationalPhoneNumber { get; init; }
    public string? WebsiteUri { get; init; }
    public string? FacebookUrl { get; init; }
    public string? InstagramUrl { get; init; }
    public string? LinkedInUrl { get; init; }
    public string? XUrl { get; init; }
    public string? YouTubeUrl { get; init; }
    public string? TikTokUrl { get; init; }
    public string? PinterestUrl { get; init; }
    public string? BlueskyUrl { get; init; }
    public bool Apple { get; init; }
    public bool Bing { get; init; }
    public string? AppleUrl { get; init; }
    public string? BingUrl { get; init; }
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
    public PlaceFinancialInfo? FinancialInfo { get; init; }
    public IReadOnlyList<PlaceFinancialOfficerInfo> FinancialOfficers { get; init; } = [];
    public IReadOnlyList<PlaceFinancialPersonOfSignificantControlInfo> FinancialPersonsOfSignificantControl { get; init; } = [];
    public IReadOnlyList<PlaceFinancialAccountInfo> FinancialAccounts { get; init; } = [];
    public bool ZohoLeadCreated { get; init; }
    public DateTime? ZohoLeadCreatedAtUtc { get; init; }
    public string? ZohoLeadId { get; init; }
    public DateTime? ZohoLastSyncAtUtc { get; init; }
    public string? ZohoLastError { get; init; }
    public FirstContactReportAvailability? FirstContactReportAvailability { get; set; }
}

public sealed record PlaceFinancialInfo(
    string PlaceId,
    DateTime? DateOfCreation,
    string CompanyNumber,
    string? CompanyType,
    DateTime? LastAccountsFiled,
    DateTime? NextAccountsDue,
    string? CompanyStatus,
    bool HasLiquidated,
    bool HasCharges,
    bool HasInsolvencyHistory);

public sealed record PlaceFinancialInfoUpsert(
    DateTime? DateOfCreation,
    string CompanyNumber,
    string? CompanyType,
    DateTime? LastAccountsFiled,
    DateTime? NextAccountsDue,
    string? CompanyStatus,
    bool HasLiquidated,
    bool HasCharges,
    bool HasInsolvencyHistory,
    IReadOnlyList<PlaceFinancialOfficerUpsert> Officers,
    IReadOnlyList<PlaceFinancialPersonOfSignificantControlUpsert> PersonsWithSignificantControl);

public sealed record PlaceFinancialOfficerInfo(
    long Id,
    string PlaceId,
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    DateTime? DateOfBirth,
    string? Nationality,
    string? Role,
    DateTime? Appointed,
    DateTime? Resigned,
    bool IsPossiblePscMatch = false);

public sealed record PlaceFinancialOfficerUpsert(
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    DateTime? DateOfBirth,
    string? Nationality,
    string? Role,
    DateTime? Appointed,
    DateTime? Resigned);

public sealed record PlaceFinancialPersonOfSignificantControlInfo(
    long Id,
    string PlaceId,
    string CompanyNumber,
    string? PscItemKind,
    string? PscLinkSelf,
    string? PscId,
    string? NameRaw,
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    string? Nationality,
    byte? BirthMonth,
    int? BirthYear,
    DateTime? NotifiedOn,
    DateTime? CeasedOn,
    string? SourceEtag,
    DateTime RetrievedUtc,
    string? RawJson,
    IReadOnlyList<string> NatureCodes,
    string OwnershipRights,
    string VotingRights,
    bool CanAppointDirectors,
    bool HasSignificantControl,
    bool HasTrustControl,
    bool HasFirmControl,
    string LlpRights,
    bool IsPossibleOfficerMatch = false);

public sealed record PlaceFinancialPersonOfSignificantControlUpsert(
    string CompanyNumber,
    string? PscItemKind,
    string? PscLinkSelf,
    string? PscId,
    string? NameRaw,
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    string? Nationality,
    byte? BirthMonth,
    int? BirthYear,
    DateTime? NotifiedOn,
    DateTime? CeasedOn,
    string? SourceEtag,
    DateTime RetrievedUtc,
    string? RawJson,
    IReadOnlyList<string> NatureCodes);

public sealed record PlaceFinancialAccountInfo(
    long Id,
    string PlaceId,
    string CompanyNumber,
    string? TransactionId,
    DateTime? FilingDate,
    DateTime? MadeUpDate,
    string? AccountsType,
    string DocumentId,
    string DocumentMetadataUrl,
    string? ContentType,
    string? OriginalFileName,
    string LocalRelativePath,
    long? FileSizeBytes,
    DateTime RetrievedUtc,
    bool IsLatest,
    string? RawJson);

public sealed class PlaceSocialLinksEditModel
{
    public string PlaceId { get; set; } = string.Empty;
    public long? RunId { get; set; }
    public string? DisplayName { get; set; }
    public string? FacebookUrl { get; set; }
    public string? InstagramUrl { get; set; }
    public string? LinkedInUrl { get; set; }
    public string? XUrl { get; set; }
    public string? YouTubeUrl { get; set; }
    public string? TikTokUrl { get; set; }
    public string? PinterestUrl { get; set; }
    public string? BlueskyUrl { get; set; }
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
    string? LogoUrl,
    bool HasFinancialInfo,
    bool IsZohoConnected,
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
    public IReadOnlyList<int> TakeOptions { get; init; } = [25, 50, 100, 500, 1000];
    public int SelectedTake { get; init; } = 100;
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
    public bool FetchGoogleSocialProfiles { get; set; }
    public bool FetchAppleBing { get; set; }
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
    public int GoogleSocialProfilesRefreshHours { get; set; } = 24;
    public int AppleBingRefreshHours { get; set; } = 24;
    public int SearchVolumeRefreshCooldownDays { get; set; } = 30;
    public int MaxSuggestedKeyphrases { get; set; } = 20;
    public string OpenAiApiKeyProtected { get; set; } = string.Empty;
    public string OpenAiApiKey { get; set; } = string.Empty;
    public string OpenAiModel { get; set; } = "gpt-4.1-mini";
    public int OpenAiTimeoutSeconds { get; set; } = 20;
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
    public string SiteUrl { get; set; } = "https://briskly-viceless-kayleen.ngrok-free.dev/";
    public int MinimumPasswordLength { get; set; } = 12;
    public bool PasswordRequiresNumber { get; set; } = true;
    public bool PasswordRequiresCapitalLetter { get; set; } = true;
    public bool PasswordRequiresSpecialCharacter { get; set; } = true;
    public int LoginLockoutThreshold { get; set; } = 5;
    public int LoginLockoutMinutes { get; set; } = 15;
    public int EmailCodeCooldownSeconds { get; set; } = 60;
    public int EmailCodeMaxPerHourPerEmail { get; set; } = 10;
    public int EmailCodeMaxPerHourPerIp { get; set; } = 50;
    public int EmailCodeExpiryMinutes { get; set; } = 10;
    public int EmailCodeMaxFailedAttemptsPerCode { get; set; } = 5;
    public int InviteExpiryHours { get; set; } = 24;
    public int InviteOtpExpiryMinutes { get; set; } = 10;
    public int InviteOtpCooldownSeconds { get; set; } = 60;
    public int InviteOtpMaxPerHourPerInvite { get; set; } = 3;
    public int InviteOtpMaxPerHourPerIp { get; set; } = 25;
    public int InviteOtpMaxAttempts { get; set; } = 5;
    public int InviteOtpLockMinutes { get; set; } = 15;
    public int InviteMaxAttempts { get; set; } = 10;
    public int InviteLockMinutes { get; set; } = 15;
    public int ChangePasswordOtpExpiryMinutes { get; set; } = 10;
    public int ChangePasswordOtpCooldownSeconds { get; set; } = 60;
    public int ChangePasswordOtpMaxPerHourPerUser { get; set; } = 3;
    public int ChangePasswordOtpMaxPerHourPerIp { get; set; } = 25;
    public int ChangePasswordOtpMaxAttempts { get; set; } = 5;
    public int ChangePasswordOtpLockMinutes { get; set; } = 15;
    public bool BlockSearchEngines { get; set; }
}

public sealed class AdminSiteSettingsModel
{
    public string SiteUrl { get; set; } = "https://briskly-viceless-kayleen.ngrok-free.dev/";
    public string SendGridEventWebhookUrl { get; set; } = "https://briskly-viceless-kayleen.ngrok-free.dev/api/webhooks/sendgrid/events";
}

public sealed class AdminSearchSettingsModel
{
    public int MaxSuggestedKeyphrases { get; set; } = 20;
    public string OpenAiApiKey { get; set; } = string.Empty;
    public string OpenAiApiKeyMasked { get; set; } = string.Empty;
    public string OpenAiModel { get; set; } = "gpt-4.1-mini";
    public int OpenAiTimeoutSeconds { get; set; } = 20;
    public IReadOnlyList<string> OpenAiModelOptions { get; set; } = [];
}

public sealed class AdminEmailSignatureSettingsModel
{
    public string GlobalSignatureHtml { get; set; } = "<p>Kind regards,<br/>Local SEO Team</p>";
    public string WrapperViewPath { get; set; } = "_EmailWrapper.cshtml";
}

public sealed class AdminDataCollectionWindowsSettingsModel
{
    public int EnhancedGoogleDataRefreshHours { get; set; } = 24;
    public int GoogleReviewsRefreshHours { get; set; } = 24;
    public int GoogleUpdatesRefreshHours { get; set; } = 24;
    public int GoogleQuestionsAndAnswersRefreshHours { get; set; } = 24;
    public int GoogleSocialProfilesRefreshHours { get; set; } = 24;
    public int AppleBingRefreshHours { get; set; } = 24;
    public int SearchVolumeRefreshCooldownDays { get; set; } = 30;
}

public sealed class AdminMapPackCtrModelSettingsModel
{
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
}

public sealed class AdminZohoLeadDefaultsSettingsModel
{
    public string ZohoLeadOwnerName { get; set; } = "Richard Howes";
    public string ZohoLeadOwnerId { get; set; } = "1108404000000068001";
    public string ZohoLeadNextAction { get; set; } = "Make first contact";
}

public sealed class AdminSecuritySettingsModel
{
    public int MinimumPasswordLength { get; set; } = 12;
    public bool PasswordRequiresNumber { get; set; } = true;
    public bool PasswordRequiresCapitalLetter { get; set; } = true;
    public bool PasswordRequiresSpecialCharacter { get; set; } = true;
    public int LoginLockoutThreshold { get; set; } = 5;
    public int LoginLockoutMinutes { get; set; } = 15;
    public int EmailCodeCooldownSeconds { get; set; } = 60;
    public int EmailCodeMaxPerHourPerEmail { get; set; } = 10;
    public int EmailCodeMaxPerHourPerIp { get; set; } = 50;
    public int EmailCodeExpiryMinutes { get; set; } = 10;
    public int EmailCodeMaxFailedAttemptsPerCode { get; set; } = 5;
    public int InviteExpiryHours { get; set; } = 24;
    public int InviteOtpExpiryMinutes { get; set; } = 10;
    public int InviteOtpCooldownSeconds { get; set; } = 60;
    public int InviteOtpMaxPerHourPerInvite { get; set; } = 3;
    public int InviteOtpMaxPerHourPerIp { get; set; } = 25;
    public int InviteOtpMaxAttempts { get; set; } = 5;
    public int InviteOtpLockMinutes { get; set; } = 15;
    public int InviteMaxAttempts { get; set; } = 10;
    public int InviteLockMinutes { get; set; } = 15;
    public int ChangePasswordOtpExpiryMinutes { get; set; } = 10;
    public int ChangePasswordOtpCooldownSeconds { get; set; } = 60;
    public int ChangePasswordOtpMaxPerHourPerUser { get; set; } = 3;
    public int ChangePasswordOtpMaxPerHourPerIp { get; set; } = 25;
    public int ChangePasswordOtpMaxAttempts { get; set; } = 5;
    public int ChangePasswordOtpLockMinutes { get; set; } = 15;
    public bool BlockSearchEngines { get; set; }
}

public sealed class PasswordPolicyViewModel
{
    public int MinimumPasswordLength { get; init; } = 12;
    public bool RequiresNumber { get; init; } = true;
    public bool RequiresCapitalLetter { get; init; } = true;
    public bool RequiresSpecialCharacter { get; init; } = true;
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
