namespace LocalSeo.Web.Models;

public sealed class PlaceWebsiteTabViewModel
{
    public bool CanShowTab { get; init; }
    public bool IsEligibleWebsiteForHomepageFetch { get; init; }
    public bool CanFetchHomePage { get; init; }
    public string? FetchDisabledReason { get; init; }
    public string RecordedWebsiteUrl { get; init; } = string.Empty;
    public string? NormalizedWebsiteUrl { get; init; }
    public string? HostName { get; init; }
    public bool? IsHttps { get; init; }
    public string CurrentStatus { get; init; } = "NotFetched";
    public DateTime? LastCheckedUtc { get; init; }
    public DateTime? LastSuccessfulFetchUtc { get; init; }
    public PlaceWebsiteFetchSummaryViewModel? LastFetch { get; init; }
    public PlaceWebsiteHomepageAuditViewModel? LatestHomepageAudit { get; init; }
}

public sealed class PlaceWebsiteFetchSummaryViewModel
{
    public long PlaceWebsiteFetchId { get; init; }
    public DateTime FetchStartedUtc { get; init; }
    public DateTime? FetchCompletedUtc { get; init; }
    public bool Success { get; init; }
    public string RequestedUrl { get; init; } = string.Empty;
    public string? FinalUrl { get; init; }
    public int? HttpStatusCode { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ContentType { get; init; }
    public int? ResponseSizeBytes { get; init; }
    public int? RedirectCount { get; init; }
    public bool UsedWorker { get; init; }
    public string? WorkerKey { get; init; }
}

public sealed class PlaceWebsiteHomepageAuditViewModel
{
    public long PlaceWebsiteFetchId { get; init; }
    public string? TitleTag { get; init; }
    public int? TitleTagLength { get; init; }
    public string? MetaDescription { get; init; }
    public int? MetaDescriptionLength { get; init; }
    public string? CanonicalUrl { get; init; }
    public string? RobotsMeta { get; init; }
    public string? HtmlLang { get; init; }
    public string? H1Text { get; init; }
    public int? H1Count { get; init; }
    public int? H2Count { get; init; }
    public int? H3Count { get; init; }
    public IReadOnlyList<string> H2Texts { get; init; } = [];
    public IReadOnlyList<string> H3Texts { get; init; } = [];
    public int? VisibleWordCount { get; init; }
    public int? ParagraphCount { get; init; }
    public int? BulletListCount { get; init; }
    public int? ContentSectionCount { get; init; }
    public bool HasPhoneNumber { get; init; }
    public IReadOnlyList<string> PhoneNumbers { get; init; } = [];
    public bool HasPostalAddress { get; init; }
    public IReadOnlyList<string> PostalAddresses { get; init; } = [];
    public bool HasPostcode { get; init; }
    public IReadOnlyList<string> Postcodes { get; init; } = [];
    public bool HasCityName { get; init; }
    public IReadOnlyList<string> CityNames { get; init; } = [];
    public bool HasBusinessName { get; init; }
    public IReadOnlyList<string> BusinessNames { get; init; } = [];
    public IReadOnlyList<string> SchemaTypes { get; init; } = [];
    public bool HasLocalBusinessSchema { get; init; }
    public bool HasOrganizationSchema { get; init; }
    public bool HasProductSchema { get; init; }
    public bool HasFaqSchema { get; init; }
    public bool HasBreadcrumbSchema { get; init; }
    public bool HasNapInSchema { get; init; }
    public bool HasGeoCoordinatesInSchema { get; init; }
    public string? PageScheme { get; init; }
    public string? CanonicalScheme { get; init; }
    public bool? RedirectsToHttps { get; init; }
    public bool? HasMixedContent { get; init; }
    public int? InternalLinkCount { get; init; }
    public int? ServicePageLinkCount { get; init; }
    public IReadOnlyList<string> InternalAnchorTexts { get; init; } = [];
    public int? ImageCount { get; init; }
    public int? ImagesMissingAltCount { get; init; }
    public IReadOnlyList<string> ImageAltTexts { get; init; } = [];
    public IReadOnlyList<string> ImageFileNames { get; init; } = [];
    public string? DetectedCms { get; init; }
    public string? GeneratorMetaTag { get; init; }
    public bool HasViewportMeta { get; init; }
    public bool HasResponsiveIndicators { get; init; }
    public bool HasFavicon { get; init; }
    public bool HasCookieBanner { get; init; }
    public IReadOnlyList<string> ServiceKeywords { get; init; } = [];
    public IReadOnlyList<string> LocationKeywords { get; init; } = [];
    public IReadOnlyList<string> ServiceTownCombinations { get; init; } = [];
    public IReadOnlyList<string> BrandNames { get; init; } = [];
    public DateTime CreatedUtc { get; init; }
}

public sealed record PlaceWebsiteActionResult(
    bool Success,
    string Message);

public sealed record PlaceWebsiteBulkActionResult(
    int TotalCount,
    int SuccessCount,
    int FailedCount,
    IReadOnlyList<string> FailureMessages);
