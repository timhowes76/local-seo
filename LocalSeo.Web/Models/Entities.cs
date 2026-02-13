namespace LocalSeo.Web.Models;

public sealed record SearchRun(long SearchRunId, string SeedKeyword, string LocationName, decimal? CenterLat, decimal? CenterLng, int? RadiusMeters, int ResultLimit, DateTime RanAtUtc);
public sealed record PlaceSnapshotRow(long PlaceSnapshotId, long SearchRunId, string PlaceId, int RankPosition, decimal? Rating, int? UserRatingCount, DateTime CapturedAtUtc, string? DisplayName, string? FormattedAddress);

public sealed class SearchFormModel
{
    public string SeedKeyword { get; set; } = string.Empty;
    public string LocationName { get; set; } = string.Empty;
    public decimal? CenterLat { get; set; }
    public decimal? CenterLng { get; set; }
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

public sealed record GooglePlace(string Id, string? DisplayName, string? PrimaryType, string TypesCsv, decimal? Rating, int? UserRatingCount, string? FormattedAddress, decimal? Lat, decimal? Lng);
