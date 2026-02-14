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
    string? WebsiteUri);
public sealed record PlaceHistoryRow(long SearchRunId, int RankPosition, decimal? Rating, int? UserRatingCount, DateTime CapturedAtUtc);
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
    public IReadOnlyList<PlaceHistoryRow> History { get; init; } = [];
}

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
