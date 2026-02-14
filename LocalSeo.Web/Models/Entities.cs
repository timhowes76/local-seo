namespace LocalSeo.Web.Models;

public sealed record SearchRun(long SearchRunId, string SeedKeyword, string LocationName, decimal? CenterLat, decimal? CenterLng, int? RadiusMeters, int ResultLimit, DateTime RanAtUtc);
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
    int? ReviewsLast90,
    decimal? AvgPerMonth12m,
    decimal? Trend90Pct,
    int? DaysSinceLastReview,
    string? StatusLabel,
    int? MomentumScore);
public sealed record PlaceHistoryRow(long SearchRunId, int RankPosition, decimal? Rating, int? UserRatingCount, DateTime CapturedAtUtc);
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
public sealed record RunDetailsViewModel(SearchRun Run, IReadOnlyList<PlaceSnapshotRow> Snapshots);

public sealed class PlaceDetailsViewModel
{
    public string PlaceId { get; init; } = string.Empty;
    public string? DisplayName { get; init; }
    public string? FormattedAddress { get; init; }
    public string? PrimaryType { get; init; }
    public string? PrimaryCategory { get; init; }
    public string? NationalPhoneNumber { get; init; }
    public string? WebsiteUri { get; init; }
    public decimal? Lat { get; init; }
    public decimal? Lng { get; init; }
    public string? Description { get; init; }
    public int? PhotoCount { get; init; }
    public bool? IsServiceAreaBusiness { get; init; }
    public string? BusinessStatus { get; init; }
    public IReadOnlyList<string> RegularOpeningHours { get; init; } = [];
    public IReadOnlyList<string> OtherCategories { get; init; } = [];
    public long? ActiveRunId { get; init; }
    public int? ActiveRankPosition { get; init; }
    public decimal? ActiveRating { get; init; }
    public int? ActiveUserRatingCount { get; init; }
    public DateTime? ActiveCapturedAtUtc { get; init; }
    public IReadOnlyList<PlaceReviewRow> Reviews { get; init; } = [];
    public IReadOnlyList<PlaceHistoryRow> History { get; init; } = [];
    public PlaceReviewVelocityDetailsDto? ReviewVelocity { get; init; }
}

public sealed record PlaceVelocityListItemDto(
    string PlaceId,
    string? DisplayName,
    decimal? Rating,
    int? UserRatingCount,
    int? ReviewsLast90,
    decimal? AvgPerMonth12m,
    decimal? Trend90Pct,
    int? DaysSinceLastReview,
    string? StatusLabel,
    int? MomentumScore);

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
    public string SeedKeyword { get; set; } = string.Empty;
    public string LocationName { get; set; } = string.Empty;
    public int RadiusMeters { get; set; } = 5000;
    public int ResultLimit { get; set; } = 20;
    public bool FetchReviews { get; set; }
}

public sealed class LoginEmailModel
{
    public string Email { get; set; } = string.Empty;
}

public sealed class LoginCodeModel
{
    public string Email { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
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
