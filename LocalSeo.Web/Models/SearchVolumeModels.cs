namespace LocalSeo.Web.Models;

public static class CategoryLocationKeywordTypes
{
    public const int MainTerm = 1;
    public const int Synonym = 2;
    public const int Modifier = 3;
    public const int Adjacent = 4;
}

public sealed record LocationCategoryRow(
    string CategoryId,
    string CategoryDisplayName,
    int KeywordCount,
    int MainTermCount,
    DateTime? LastKeywordUpdatedUtc);

public sealed class LocationCategoryListViewModel
{
    public long LocationId { get; init; }
    public string LocationName { get; init; } = string.Empty;
    public string CountyName { get; init; } = string.Empty;
    public IReadOnlyList<LocationCategoryRow> Rows { get; init; } = [];
}

public sealed record SearchVolumePoint(int Year, int Month, int SearchVolume);
public sealed record WeightedSearchVolumePoint(int Year, int Month, decimal WeightedSearchVolume);

public sealed class CategoryLocationKeywordListItem
{
    public int Id { get; init; }
    public string Keyword { get; init; } = string.Empty;
    public int KeywordType { get; init; }
    public int? CanonicalKeywordId { get; init; }
    public string? SynonymOfKeyword { get; init; }
    public int? AvgSearchVolume { get; init; }
    public decimal? Cpc { get; init; }
    public string? Competition { get; init; }
    public int? CompetitionIndex { get; init; }
    public decimal? LowTopOfPageBid { get; init; }
    public decimal? HighTopOfPageBid { get; init; }
    public bool NoData { get; init; }
    public string? NoDataReason { get; init; }
    public DateTime? LastAttemptedUtc { get; init; }
    public DateTime? LastSucceededUtc { get; init; }
    public int? LastStatusCode { get; init; }
    public string? LastStatusMessage { get; init; }
    public DateTime? DataAsOfUtc { get; set; }
    public bool IsRefreshEligible { get; set; }
    public IReadOnlyList<SearchVolumePoint> Last12Months { get; set; } = [];
}

public sealed class CategoryLocationKeywordCreateModel
{
    public string Keyword { get; set; } = string.Empty;
    public int KeywordType { get; set; } = CategoryLocationKeywordTypes.Modifier;
}

public sealed class CategoryKeyphrasesViewModel
{
    public long LocationId { get; init; }
    public string LocationName { get; init; } = string.Empty;
    public string CountyName { get; init; } = string.Empty;
    public string CategoryId { get; init; } = string.Empty;
    public string CategoryDisplayName { get; init; } = string.Empty;
    public string ExpectedMainKeyword { get; init; } = string.Empty;
    public int SearchVolumeRefreshCooldownDays { get; init; } = 30;
    public IReadOnlyList<CategoryLocationKeywordListItem> Rows { get; init; } = [];
    public IReadOnlyList<WeightedSearchVolumePoint> WeightedTotalLast12Months { get; init; } = [];
    public CategoryLocationKeywordCreateModel AddForm { get; init; } = new();
}

public sealed record CategoryLocationKeywordRefreshSummary(
    int RequestedCount,
    int RefreshedCount,
    int SkippedCount,
    int ErrorCount);
