namespace LocalSeo.Web.Models;

public sealed record GoogleBusinessProfileCategoryRow(
    string CategoryId,
    string DisplayName,
    string RegionCode,
    string LanguageCode,
    string Status,
    DateTime FirstSeenUtc,
    DateTime LastSeenUtc,
    DateTime LastSyncedUtc);

public sealed record GoogleBusinessProfileCategorySyncSummary(
    DateTime RanAtUtc,
    int AddedCount,
    int UpdatedCount,
    int MarkedInactiveCount);

public sealed record GoogleBusinessProfileCategorySyncRunResult(
    DateTime RanAtUtc,
    int AddedCount,
    int UpdatedCount,
    int MarkedInactiveCount,
    int PagesFetched,
    bool IsCycleComplete,
    bool WasRateLimited);

public sealed class GoogleBusinessProfileCategoryListViewModel
{
    public IReadOnlyList<GoogleBusinessProfileCategoryRow> Rows { get; init; } = [];
    public string Search { get; init; } = string.Empty;
    public string StatusFilter { get; init; } = "active";
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 50;
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
    public GoogleBusinessProfileCategorySyncSummary? LastUkSyncSummary { get; init; }
    public GoogleBusinessProfileCategoryCreateModel ManualAddForm { get; init; } = new();
}

public sealed record GoogleBusinessProfileCategoryListResult(
    IReadOnlyList<GoogleBusinessProfileCategoryRow> Rows,
    int TotalCount,
    int Page,
    int PageSize,
    int TotalPages);

public sealed class GoogleBusinessProfileCategoryCreateModel
{
    public string CategoryId { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string RegionCode { get; set; } = "GB";
    public string LanguageCode { get; set; } = "en-GB";
}

public sealed record GoogleBusinessProfileCategoryLookupItem(
    string CategoryId,
    string DisplayName,
    string RegionCode,
    string LanguageCode);

public sealed class GoogleBusinessProfileCategoryEditModel
{
    public string CategoryId { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string RegionCode { get; set; } = string.Empty;
    public string LanguageCode { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
}

public sealed record GoogleOAuthConnectionResult(
    bool Success,
    string Message,
    string? MaskedRefreshToken);

public sealed record GbCountyRow(
    long CountyId,
    string Name,
    string? Slug,
    bool IsActive,
    int? SortOrder,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record GbCountyListResult(
    IReadOnlyList<GbCountyRow> Rows,
    int TotalCount,
    int Page,
    int PageSize,
    int TotalPages);

public sealed class GbCountyCreateModel
{
    public string Name { get; set; } = string.Empty;
    public string? Slug { get; set; }
    public bool IsActive { get; set; } = true;
    public int? SortOrder { get; set; }
}

public sealed class GbCountyEditModel
{
    public long CountyId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Slug { get; set; }
    public bool IsActive { get; set; } = true;
    public int? SortOrder { get; set; }
}

public sealed class GbCountyListViewModel
{
    public IReadOnlyList<GbCountyRow> Rows { get; init; } = [];
    public string Search { get; init; } = string.Empty;
    public string StatusFilter { get; init; } = "active";
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 50;
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
    public GbCountyCreateModel ManualAddForm { get; init; } = new();
}

public sealed record GbCountyLookupItem(long CountyId, string Name, bool IsActive);

public sealed class GbTownLookupItem
{
    public long TownId { get; init; }
    public long CountyId { get; init; }
    public string Name { get; init; } = string.Empty;
    public string CountyName { get; init; } = string.Empty;
    public bool IsActive { get; init; }
    public decimal? Latitude { get; init; }
    public decimal? Longitude { get; init; }
}

public sealed record GbTownRow(
    long TownId,
    long CountyId,
    string CountyName,
    string Name,
    string? Slug,
    decimal? Latitude,
    decimal? Longitude,
    string? ExternalId,
    bool IsActive,
    int? SortOrder,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record GbTownListResult(
    IReadOnlyList<GbTownRow> Rows,
    int TotalCount,
    int Page,
    int PageSize,
    int TotalPages);

public sealed class GbTownCreateModel
{
    public long CountyId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Slug { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
    public string? ExternalId { get; set; }
    public bool IsActive { get; set; } = true;
    public int? SortOrder { get; set; }
    public IReadOnlyList<GbCountyLookupItem> CountyOptions { get; set; } = [];
}

public sealed class GbTownEditModel
{
    public long TownId { get; set; }
    public long CountyId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Slug { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
    public string? ExternalId { get; set; }
    public bool IsActive { get; set; } = true;
    public int? SortOrder { get; set; }
    public IReadOnlyList<GbCountyLookupItem> CountyOptions { get; set; } = [];
}

public sealed class GbTownListViewModel
{
    public IReadOnlyList<GbTownRow> Rows { get; init; } = [];
    public IReadOnlyList<GbCountyLookupItem> CountyOptions { get; init; } = [];
    public string Search { get; init; } = string.Empty;
    public string StatusFilter { get; init; } = "active";
    public long? CountyIdFilter { get; init; }
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 50;
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
    public GbTownCreateModel ManualAddForm { get; init; } = new();
}

public sealed class GbTownDetailsViewModel
{
    public GbTownEditModel Town { get; init; } = new();
    public string CountyName { get; init; } = string.Empty;
    public IReadOnlyList<SearchRun> Runs { get; init; } = [];
}

public sealed class GbCountyDetailsViewModel
{
    public GbCountyEditModel County { get; init; } = new();
    public IReadOnlyList<GbTownLookupItem> Towns { get; init; } = [];
    public IReadOnlyList<SearchRun> Runs { get; init; } = [];
}

public sealed class GbCountySortViewModel
{
    public IReadOnlyList<GbCountyRow> Rows { get; init; } = [];
}

public sealed class GbTownSortViewModel
{
    public IReadOnlyList<GbCountyLookupItem> CountyOptions { get; init; } = [];
    public long? SelectedCountyId { get; init; }
    public IReadOnlyList<GbTownRow> Rows { get; init; } = [];
}
