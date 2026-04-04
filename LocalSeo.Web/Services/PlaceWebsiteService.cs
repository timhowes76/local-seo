using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IPlaceWebsiteService
{
    Task<PlaceWebsiteTabViewModel?> GetTabViewModelAsync(string placeId, CancellationToken ct);
    Task<PlaceWebsiteActionResult> FetchHomePageAsync(string placeId, CancellationToken ct);
    Task<PlaceWebsiteBulkActionResult> FetchHomePagesAsync(IReadOnlyCollection<string> placeIds, CancellationToken ct);
}

public sealed class PlaceWebsiteService(
    ISqlConnectionFactory connectionFactory,
    ICloudflareWorkerService cloudflareWorkerService,
    IHomePageFetchWorkerClient homePageFetchWorkerClient,
    IHomePageAuditParser homePageAuditParser,
    IWebsiteClassifier websiteClassifier,
    TimeProvider timeProvider,
    ILogger<PlaceWebsiteService> logger) : IPlaceWebsiteService
{
    private const string WorkerKey = "SalesLocalSeoHomePageFetch";
    private const string SchemaUnavailableMessage = "Website analysis schema is not available yet. Run the homepage analysis migration or startup schema bootstrap first.";
    private const string StatusNotFetched = "NotFetched";
    private const string StatusFetched = "Fetched";
    private const string StatusFailed = "Failed";
    private const string StatusBlocked = "Blocked";
    private const string StatusDisabled = "Disabled";
    private const string StatusIneligible = "Ineligible";
    private const string SourceTypePlaceRecord = "PlaceWebsiteUri";

    public async Task<PlaceWebsiteTabViewModel?> GetTabViewModelAsync(string placeId, CancellationToken ct)
    {
        var context = await GetPlaceContextAsync(placeId, ct);
        if (context is null)
            return null;

        var eligibility = DetermineEligibility(context.WebsiteUri, context.WebsiteType);
        if (!eligibility.CanShowTab)
        {
            return new PlaceWebsiteTabViewModel
            {
                CanShowTab = false,
                IsEligibleWebsiteForHomepageFetch = false,
                CanFetchHomePage = false,
                FetchDisabledReason = eligibility.Reason,
                RecordedWebsiteUrl = (context.WebsiteUri ?? string.Empty).Trim(),
                CurrentStatus = StatusIneligible
            };
        }

        if (!await WebsiteAnalysisSchemaIsAvailableAsync(ct))
        {
            return new PlaceWebsiteTabViewModel
            {
                CanShowTab = true,
                IsEligibleWebsiteForHomepageFetch = eligibility.IsEligible,
                CanFetchHomePage = false,
                FetchDisabledReason = SchemaUnavailableMessage,
                RecordedWebsiteUrl = (context.WebsiteUri ?? string.Empty).Trim(),
                CurrentStatus = StatusDisabled
            };
        }

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var placeWebsite = await GetPlaceWebsiteRowAsync(conn, placeId, ct);
        var lastFetch = placeWebsite is null
            ? null
            : await GetLatestFetchRowAsync(conn, placeWebsite.PlaceWebsiteId, ct);
        var latestAudit = placeWebsite is null
            ? null
            : await GetLatestSuccessfulAuditRowAsync(conn, placeWebsite.PlaceWebsiteId, ct);
        var worker = await cloudflareWorkerService.GetByKeyAsync(WorkerKey, ct);
        var hasPendingFetch = lastFetch is not null && !lastFetch.FetchCompletedUtc.HasValue;
        var workerEnabled = cloudflareWorkerService.IsWorkerEnabled(worker);
        var fetchDisabledReason = !eligibility.IsEligible
            ? eligibility.Reason
            : hasPendingFetch
                ? "A homepage fetch is already in progress."
                : workerEnabled
                    ? null
                    : "Cloudflare worker is not configured or enabled.";

        var currentStatus = NormalizeStatus(placeWebsite?.Status, workerEnabled);
        return new PlaceWebsiteTabViewModel
        {
            CanShowTab = true,
            IsEligibleWebsiteForHomepageFetch = eligibility.IsEligible,
            CanFetchHomePage = eligibility.IsEligible && workerEnabled && !hasPendingFetch,
            FetchDisabledReason = fetchDisabledReason,
            RecordedWebsiteUrl = placeWebsite?.WebsiteUrl ?? (context.WebsiteUri ?? string.Empty).Trim(),
            NormalizedWebsiteUrl = placeWebsite?.NormalizedWebsiteUrl,
            HostName = placeWebsite?.HostName,
            IsHttps = placeWebsite?.IsHttps,
            CurrentStatus = currentStatus,
            LastCheckedUtc = placeWebsite?.LastCheckedUtc,
            LastSuccessfulFetchUtc = placeWebsite?.LastSuccessfulFetchUtc,
            LastFetch = lastFetch is null ? null : MapFetch(lastFetch),
            LatestHomepageAudit = latestAudit is null ? null : MapAudit(latestAudit)
        };
    }

    public async Task<PlaceWebsiteActionResult> FetchHomePageAsync(string placeId, CancellationToken ct)
    {
        var context = await GetPlaceContextAsync(placeId, ct);
        if (context is null)
            return new PlaceWebsiteActionResult(false, "Place not found.");

        var eligibility = DetermineEligibility(context.WebsiteUri, context.WebsiteType);
        if (!eligibility.IsEligible || string.IsNullOrWhiteSpace(context.WebsiteUri))
            return new PlaceWebsiteActionResult(false, eligibility.Reason ?? "This place does not have an eligible website.");

        if (!await WebsiteAnalysisSchemaIsAvailableAsync(ct))
            return new PlaceWebsiteActionResult(false, SchemaUnavailableMessage);

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var requestedUrl = context.WebsiteUri.Trim();
        var normalizedUrl = NormalizeWebsiteUrl(requestedUrl);
        var resolvedHostName = TryGetHost(normalizedUrl);
        var isHttps = TryGetScheme(normalizedUrl) is { } scheme && string.Equals(scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

        int placeWebsiteId;
        await using (var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        {
            placeWebsiteId = await EnsurePlaceWebsiteAsync(conn, context.PlaceId, requestedUrl, normalizedUrl, resolvedHostName, isHttps, nowUtc, ct);
            var hasPendingFetch = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.PlaceWebsiteFetch
WHERE PlaceWebsiteId = @PlaceWebsiteId
  AND FetchCompletedUtc IS NULL;",
                new { PlaceWebsiteId = placeWebsiteId },
                cancellationToken: ct));
            if (hasPendingFetch > 0)
                return new PlaceWebsiteActionResult(false, "A homepage fetch is already in progress for this place.");
        }

        long fetchId;
        await using (var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        {
            fetchId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.PlaceWebsiteFetch(
  PlaceWebsiteId,
  FetchStartedUtc,
  FetchCompletedUtc,
  Success,
  RequestedUrl,
  UsedWorker,
  WorkerKey,
  CreatedUtc)
OUTPUT INSERTED.PlaceWebsiteFetchId
VALUES(
  @PlaceWebsiteId,
  @FetchStartedUtc,
  NULL,
  0,
  @RequestedUrl,
  1,
  @WorkerKey,
  @CreatedUtc);",
                new
                {
                    PlaceWebsiteId = placeWebsiteId,
                    FetchStartedUtc = nowUtc,
                    RequestedUrl = requestedUrl,
                    WorkerKey,
                    CreatedUtc = nowUtc
                },
                cancellationToken: ct));
        }

        HomePageFetchWorkerResult workerResult;
        try
        {
            workerResult = await homePageFetchWorkerClient.FetchAsync(WorkerKey, requestedUrl, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Homepage fetch worker call failed for place {PlaceId}.", placeId);
            workerResult = new HomePageFetchWorkerResult(
                WorkerKey,
                false,
                requestedUrl,
                null,
                null,
                null,
                null,
                null,
                "WorkerRequestFailed",
                ex.Message,
                true);
        }

        var completedUtc = timeProvider.GetUtcNow().UtcDateTime;
        var returnedHtml = (workerResult.Html ?? string.Empty).Trim();
        if (!workerResult.Success || returnedHtml.Length == 0)
        {
            var failedStatus = DetermineFailedStatus(workerResult);
            await PersistFailedFetchAsync(
                placeWebsiteId,
                fetchId,
                requestedUrl,
                normalizedUrl,
                resolvedHostName,
                isHttps,
                workerResult,
                completedUtc,
                failedStatus,
                ct);

            var message = BuildFailureMessage(workerResult, failedStatus);
            return new PlaceWebsiteActionResult(false, message);
        }

        var finalUrl = NormalizeWebsiteUrl(workerResult.FinalUrl) ?? normalizedUrl;
        var finalHostName = TryGetHost(finalUrl) ?? resolvedHostName;
        var finalIsHttps = TryGetScheme(finalUrl) is { } finalScheme && string.Equals(finalScheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
        var parsed = homePageAuditParser.Parse(new HomePageAuditParseRequest
        {
            Html = returnedHtml,
            RequestedUrl = workerResult.RequestedUrl,
            FinalUrl = finalUrl,
            DisplayName = context.DisplayName,
            FormattedAddress = context.FormattedAddress,
            PrimaryCategory = context.PrimaryCategory,
            SearchLocationName = context.SearchLocationName
        });

        await PersistSuccessfulFetchAsync(
            placeWebsiteId,
            fetchId,
            requestedUrl,
            finalUrl,
            finalHostName,
            finalIsHttps,
            returnedHtml,
            workerResult,
            parsed,
            completedUtc,
            ct);

        return new PlaceWebsiteActionResult(true, "Homepage analysis fetched and stored.");
    }

    public async Task<PlaceWebsiteBulkActionResult> FetchHomePagesAsync(IReadOnlyCollection<string> placeIds, CancellationToken ct)
    {
        var normalizedPlaceIds = placeIds
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (normalizedPlaceIds.Count == 0)
            return new PlaceWebsiteBulkActionResult(0, 0, 0, []);

        var successCount = 0;
        var failedCount = 0;
        var failureMessages = new List<string>();
        foreach (var placeId in normalizedPlaceIds)
        {
            ct.ThrowIfCancellationRequested();

            var result = await FetchHomePageAsync(placeId, ct);
            if (result.Success)
            {
                successCount++;
                continue;
            }

            failedCount++;
            if (!string.IsNullOrWhiteSpace(result.Message) && failureMessages.Count < 5)
                failureMessages.Add(result.Message);
        }

        return new PlaceWebsiteBulkActionResult(
            normalizedPlaceIds.Count,
            successCount,
            failedCount,
            failureMessages);
    }

    private async Task PersistFailedFetchAsync(
        int placeWebsiteId,
        long fetchId,
        string requestedUrl,
        string? normalizedUrl,
        string? hostName,
        bool? isHttps,
        HomePageFetchWorkerResult workerResult,
        DateTime completedUtc,
        string failedStatus,
        CancellationToken ct)
    {
        var failureMessage = BuildFailureMessage(workerResult, failedStatus);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceWebsiteFetch
SET
  FetchCompletedUtc = @FetchCompletedUtc,
  Success = 0,
  RequestedUrl = @RequestedUrl,
  FinalUrl = @FinalUrl,
  HttpStatusCode = @HttpStatusCode,
  ErrorCode = @ErrorCode,
  ErrorMessage = @ErrorMessage,
  ContentType = @ContentType,
  ResponseSizeBytes = NULL,
  RedirectCount = CASE WHEN @RequestedUrl <> COALESCE(@FinalUrl, @RequestedUrl) THEN 1 ELSE 0 END,
  UsedWorker = 1,
  WorkerKey = @WorkerKey,
  HtmlHash = NULL
WHERE PlaceWebsiteFetchId = @PlaceWebsiteFetchId;",
            new
            {
                PlaceWebsiteFetchId = fetchId,
                FetchCompletedUtc = completedUtc,
                RequestedUrl = requestedUrl,
                FinalUrl = NormalizeWebsiteUrl(workerResult.FinalUrl),
                HttpStatusCode = workerResult.StatusCode,
                workerResult.ErrorCode,
                ErrorMessage = NormalizeDbString(failureMessage, 2000),
                ContentType = NormalizeDbString(workerResult.ContentType, 200),
                WorkerKey = NormalizeDbString(workerResult.WorkerName, 200)
            },
            cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceWebsite
SET
  WebsiteUrl = @WebsiteUrl,
  NormalizedWebsiteUrl = @NormalizedWebsiteUrl,
  HostName = @HostName,
  IsHttps = @IsHttps,
  SourceType = @SourceType,
  [Status] = @Status,
  LastCheckedUtc = @LastCheckedUtc,
  UpdatedUtc = @UpdatedUtc
WHERE PlaceWebsiteId = @PlaceWebsiteId;",
            new
            {
                PlaceWebsiteId = placeWebsiteId,
                WebsiteUrl = NormalizeDbString(requestedUrl, 1000) ?? string.Empty,
                NormalizedWebsiteUrl = NormalizeDbString(normalizedUrl, 1000),
                HostName = NormalizeDbString(hostName, 255),
                IsHttps = isHttps,
                SourceType = SourceTypePlaceRecord,
                Status = failedStatus,
                LastCheckedUtc = completedUtc,
                UpdatedUtc = completedUtc
            },
            cancellationToken: ct));
    }

    private async Task PersistSuccessfulFetchAsync(
        int placeWebsiteId,
        long fetchId,
        string requestedUrl,
        string? finalUrl,
        string? hostName,
        bool? isHttps,
        string html,
        HomePageFetchWorkerResult workerResult,
        HomePageAuditParseResult parsed,
        DateTime completedUtc,
        CancellationToken ct)
    {
        var responseSizeBytes = Encoding.UTF8.GetByteCount(html);
        var htmlHash = ComputeSha256(html);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceWebsiteFetch
SET
  FetchCompletedUtc = @FetchCompletedUtc,
  Success = 1,
  RequestedUrl = @RequestedUrl,
  FinalUrl = @FinalUrl,
  HttpStatusCode = @HttpStatusCode,
  ErrorCode = NULL,
  ErrorMessage = NULL,
  ContentType = @ContentType,
  ResponseSizeBytes = @ResponseSizeBytes,
  RedirectCount = CASE WHEN @RequestedUrl <> COALESCE(@FinalUrl, @RequestedUrl) THEN 1 ELSE 0 END,
  UsedWorker = 1,
  WorkerKey = @WorkerKey,
  HtmlHash = @HtmlHash
WHERE PlaceWebsiteFetchId = @PlaceWebsiteFetchId;",
            new
            {
                PlaceWebsiteFetchId = fetchId,
                FetchCompletedUtc = completedUtc,
                RequestedUrl = requestedUrl,
                FinalUrl = NormalizeDbString(finalUrl, 1000),
                HttpStatusCode = workerResult.StatusCode,
                ContentType = NormalizeDbString(workerResult.ContentType, 200) ?? "text/html",
                ResponseSizeBytes = responseSizeBytes,
                WorkerKey = NormalizeDbString(workerResult.WorkerName, 200),
                HtmlHash = htmlHash
            },
            cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceWebsiteHomepageAudit(
  PlaceWebsiteFetchId,
  TitleTag,
  TitleTagLength,
  MetaDescription,
  MetaDescriptionLength,
  CanonicalUrl,
  RobotsMeta,
  HtmlLang,
  H1Text,
  H1Count,
  H2Count,
  H3Count,
  H2TextsJson,
  H3TextsJson,
  VisibleWordCount,
  ParagraphCount,
  BulletListCount,
  ContentSectionCount,
  HasPhoneNumber,
  PhoneNumbersJson,
  HasPostalAddress,
  PostalAddressesJson,
  HasPostcode,
  PostcodesJson,
  HasCityName,
  CityNamesJson,
  HasBusinessName,
  BusinessNamesJson,
  SchemaTypesJson,
  HasLocalBusinessSchema,
  HasOrganizationSchema,
  HasProductSchema,
  HasFaqSchema,
  HasBreadcrumbSchema,
  HasNapInSchema,
  HasGeoCoordinatesInSchema,
  PageScheme,
  CanonicalScheme,
  RedirectsToHttps,
  HasMixedContent,
  InternalLinkCount,
  ServicePageLinkCount,
  InternalAnchorTextsJson,
  ImageCount,
  ImagesMissingAltCount,
  ImageAltTextsJson,
  ImageFileNamesJson,
  DetectedCms,
  GeneratorMetaTag,
  HasViewportMeta,
  HasResponsiveIndicators,
  HasFavicon,
  HasCookieBanner,
  ServiceKeywordsJson,
  LocationKeywordsJson,
  ServiceTownCombinationsJson,
  BrandNamesJson,
  CreatedUtc)
VALUES(
  @PlaceWebsiteFetchId,
  @TitleTag,
  @TitleTagLength,
  @MetaDescription,
  @MetaDescriptionLength,
  @CanonicalUrl,
  @RobotsMeta,
  @HtmlLang,
  @H1Text,
  @H1Count,
  @H2Count,
  @H3Count,
  @H2TextsJson,
  @H3TextsJson,
  @VisibleWordCount,
  @ParagraphCount,
  @BulletListCount,
  @ContentSectionCount,
  @HasPhoneNumber,
  @PhoneNumbersJson,
  @HasPostalAddress,
  @PostalAddressesJson,
  @HasPostcode,
  @PostcodesJson,
  @HasCityName,
  @CityNamesJson,
  @HasBusinessName,
  @BusinessNamesJson,
  @SchemaTypesJson,
  @HasLocalBusinessSchema,
  @HasOrganizationSchema,
  @HasProductSchema,
  @HasFaqSchema,
  @HasBreadcrumbSchema,
  @HasNapInSchema,
  @HasGeoCoordinatesInSchema,
  @PageScheme,
  @CanonicalScheme,
  @RedirectsToHttps,
  @HasMixedContent,
  @InternalLinkCount,
  @ServicePageLinkCount,
  @InternalAnchorTextsJson,
  @ImageCount,
  @ImagesMissingAltCount,
  @ImageAltTextsJson,
  @ImageFileNamesJson,
  @DetectedCms,
  @GeneratorMetaTag,
  @HasViewportMeta,
  @HasResponsiveIndicators,
  @HasFavicon,
  @HasCookieBanner,
  @ServiceKeywordsJson,
  @LocationKeywordsJson,
  @ServiceTownCombinationsJson,
  @BrandNamesJson,
  @CreatedUtc);",
            BuildAuditParameters(fetchId, parsed, completedUtc),
            cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceWebsite
SET
  WebsiteUrl = @WebsiteUrl,
  NormalizedWebsiteUrl = @NormalizedWebsiteUrl,
  HostName = @HostName,
  IsHttps = @IsHttps,
  SourceType = @SourceType,
  [Status] = @Status,
  LastCheckedUtc = @LastCheckedUtc,
  LastSuccessfulFetchUtc = @LastSuccessfulFetchUtc,
  UpdatedUtc = @UpdatedUtc
WHERE PlaceWebsiteId = @PlaceWebsiteId;",
            new
            {
                PlaceWebsiteId = placeWebsiteId,
                WebsiteUrl = NormalizeDbString(requestedUrl, 1000) ?? string.Empty,
                NormalizedWebsiteUrl = NormalizeDbString(finalUrl, 1000),
                HostName = NormalizeDbString(hostName, 255),
                IsHttps = isHttps,
                SourceType = SourceTypePlaceRecord,
                Status = StatusFetched,
                LastCheckedUtc = completedUtc,
                LastSuccessfulFetchUtc = completedUtc,
                UpdatedUtc = completedUtc
            },
            cancellationToken: ct));
    }

    private async Task<int> EnsurePlaceWebsiteAsync(SqlConnection conn, string placeId, string requestedUrl, string? normalizedUrl, string? hostName, bool? isHttps, DateTime nowUtc, CancellationToken ct)
    {
        var existing = await GetPlaceWebsiteRowAsync(conn, placeId, ct);
        if (existing is not null)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceWebsite
SET
  WebsiteUrl = @WebsiteUrl,
  NormalizedWebsiteUrl = @NormalizedWebsiteUrl,
  HostName = @HostName,
  IsHttps = @IsHttps,
  SourceType = @SourceType,
  UpdatedUtc = @UpdatedUtc
WHERE PlaceWebsiteId = @PlaceWebsiteId;",
                new
                {
                    PlaceWebsiteId = existing.PlaceWebsiteId,
                    WebsiteUrl = NormalizeDbString(requestedUrl, 1000) ?? string.Empty,
                    NormalizedWebsiteUrl = NormalizeDbString(normalizedUrl, 1000),
                    HostName = NormalizeDbString(hostName, 255),
                    IsHttps = isHttps,
                    SourceType = SourceTypePlaceRecord,
                    UpdatedUtc = nowUtc
                },
                cancellationToken: ct));
            return existing.PlaceWebsiteId;
        }

        return await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.PlaceWebsite(
  PlaceId,
  WebsiteUrl,
  NormalizedWebsiteUrl,
  HostName,
  IsHttps,
  SourceType,
  [Status],
  FirstDiscoveredUtc,
  CreatedUtc,
  UpdatedUtc)
OUTPUT INSERTED.PlaceWebsiteId
VALUES(
  @PlaceId,
  @WebsiteUrl,
  @NormalizedWebsiteUrl,
  @HostName,
  @IsHttps,
  @SourceType,
  @Status,
  @FirstDiscoveredUtc,
  @CreatedUtc,
  @UpdatedUtc);",
            new
            {
                PlaceId = placeId,
                WebsiteUrl = NormalizeDbString(requestedUrl, 1000) ?? string.Empty,
                NormalizedWebsiteUrl = NormalizeDbString(normalizedUrl, 1000),
                HostName = NormalizeDbString(hostName, 255),
                IsHttps = isHttps,
                SourceType = SourceTypePlaceRecord,
                Status = StatusNotFetched,
                FirstDiscoveredUtc = nowUtc,
                CreatedUtc = nowUtc,
                UpdatedUtc = nowUtc
            },
            cancellationToken: ct));
    }

    private async Task<PlaceContextRow?> GetPlaceContextAsync(string placeId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(placeId))
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<PlaceContextRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  DisplayName,
  WebsiteUri,
  WebsiteType,
  FormattedAddress,
  PrimaryCategory,
  SearchLocationName
FROM dbo.Place
WHERE PlaceId = @PlaceId;",
            new { PlaceId = placeId },
            cancellationToken: ct));
    }

    private static async Task<PlaceWebsiteRow?> GetPlaceWebsiteRowAsync(SqlConnection conn, string placeId, CancellationToken ct)
        => await conn.QuerySingleOrDefaultAsync<PlaceWebsiteRow>(new CommandDefinition(@"
SELECT
  PlaceWebsiteId,
  PlaceId,
  WebsiteUrl,
  NormalizedWebsiteUrl,
  HostName,
  IsHttps,
  SourceType,
  [Status],
  FirstDiscoveredUtc,
  LastCheckedUtc,
  LastSuccessfulFetchUtc,
  CreatedUtc,
  UpdatedUtc
FROM dbo.PlaceWebsite
WHERE PlaceId = @PlaceId;",
            new { PlaceId = placeId },
            cancellationToken: ct));

    private static async Task<PlaceWebsiteFetchRow?> GetLatestFetchRowAsync(SqlConnection conn, int placeWebsiteId, CancellationToken ct)
        => await conn.QuerySingleOrDefaultAsync<PlaceWebsiteFetchRow>(new CommandDefinition(@"
SELECT TOP 1
  PlaceWebsiteFetchId,
  PlaceWebsiteId,
  FetchStartedUtc,
  FetchCompletedUtc,
  Success,
  RequestedUrl,
  FinalUrl,
  HttpStatusCode,
  ErrorCode,
  ErrorMessage,
  ContentType,
  ResponseSizeBytes,
  RedirectCount,
  UsedWorker,
  WorkerKey,
  HtmlHash,
  CreatedUtc
FROM dbo.PlaceWebsiteFetch
WHERE PlaceWebsiteId = @PlaceWebsiteId
ORDER BY FetchStartedUtc DESC, PlaceWebsiteFetchId DESC;",
            new { PlaceWebsiteId = placeWebsiteId },
            cancellationToken: ct));

    private static async Task<PlaceWebsiteAuditRow?> GetLatestSuccessfulAuditRowAsync(SqlConnection conn, int placeWebsiteId, CancellationToken ct)
        => await conn.QuerySingleOrDefaultAsync<PlaceWebsiteAuditRow>(new CommandDefinition(@"
SELECT TOP 1
  a.PlaceWebsiteHomepageAuditId,
  a.PlaceWebsiteFetchId,
  a.TitleTag,
  a.TitleTagLength,
  a.MetaDescription,
  a.MetaDescriptionLength,
  a.CanonicalUrl,
  a.RobotsMeta,
  a.HtmlLang,
  a.H1Text,
  a.H1Count,
  a.H2Count,
  a.H3Count,
  a.H2TextsJson,
  a.H3TextsJson,
  a.VisibleWordCount,
  a.ParagraphCount,
  a.BulletListCount,
  a.ContentSectionCount,
  a.HasPhoneNumber,
  a.PhoneNumbersJson,
  a.HasPostalAddress,
  a.PostalAddressesJson,
  a.HasPostcode,
  a.PostcodesJson,
  a.HasCityName,
  a.CityNamesJson,
  a.HasBusinessName,
  a.BusinessNamesJson,
  a.SchemaTypesJson,
  a.HasLocalBusinessSchema,
  a.HasOrganizationSchema,
  a.HasProductSchema,
  a.HasFaqSchema,
  a.HasBreadcrumbSchema,
  a.HasNapInSchema,
  a.HasGeoCoordinatesInSchema,
  a.PageScheme,
  a.CanonicalScheme,
  a.RedirectsToHttps,
  a.HasMixedContent,
  a.InternalLinkCount,
  a.ServicePageLinkCount,
  a.InternalAnchorTextsJson,
  a.ImageCount,
  a.ImagesMissingAltCount,
  a.ImageAltTextsJson,
  a.ImageFileNamesJson,
  a.DetectedCms,
  a.GeneratorMetaTag,
  a.HasViewportMeta,
  a.HasResponsiveIndicators,
  a.HasFavicon,
  a.HasCookieBanner,
  a.ServiceKeywordsJson,
  a.LocationKeywordsJson,
  a.ServiceTownCombinationsJson,
  a.BrandNamesJson,
  a.CreatedUtc
FROM dbo.PlaceWebsiteHomepageAudit a
JOIN dbo.PlaceWebsiteFetch f ON f.PlaceWebsiteFetchId = a.PlaceWebsiteFetchId
WHERE f.PlaceWebsiteId = @PlaceWebsiteId
  AND f.Success = 1
ORDER BY f.FetchStartedUtc DESC, a.PlaceWebsiteHomepageAuditId DESC;",
            new { PlaceWebsiteId = placeWebsiteId },
            cancellationToken: ct));

    private static object BuildAuditParameters(long fetchId, HomePageAuditParseResult parsed, DateTime createdUtc)
        => new
        {
            PlaceWebsiteFetchId = fetchId,
            TitleTag = NormalizeDbString(parsed.TitleTag, 1000),
            parsed.TitleTagLength,
            MetaDescription = NormalizeDbString(parsed.MetaDescription, 2000),
            parsed.MetaDescriptionLength,
            CanonicalUrl = NormalizeDbString(parsed.CanonicalUrl, 1000),
            RobotsMeta = NormalizeDbString(parsed.RobotsMeta, 500),
            HtmlLang = NormalizeDbString(parsed.HtmlLang, 50),
            H1Text = NormalizeDbString(parsed.H1Text, 1000),
            parsed.H1Count,
            parsed.H2Count,
            parsed.H3Count,
            H2TextsJson = SerializeJson(parsed.H2Texts),
            H3TextsJson = SerializeJson(parsed.H3Texts),
            parsed.VisibleWordCount,
            parsed.ParagraphCount,
            parsed.BulletListCount,
            parsed.ContentSectionCount,
            parsed.HasPhoneNumber,
            PhoneNumbersJson = SerializeJson(parsed.PhoneNumbers),
            parsed.HasPostalAddress,
            PostalAddressesJson = SerializeJson(parsed.PostalAddresses),
            parsed.HasPostcode,
            PostcodesJson = SerializeJson(parsed.Postcodes),
            parsed.HasCityName,
            CityNamesJson = SerializeJson(parsed.CityNames),
            parsed.HasBusinessName,
            BusinessNamesJson = SerializeJson(parsed.BusinessNames),
            SchemaTypesJson = SerializeJson(parsed.SchemaTypes),
            parsed.HasLocalBusinessSchema,
            parsed.HasOrganizationSchema,
            parsed.HasProductSchema,
            parsed.HasFaqSchema,
            parsed.HasBreadcrumbSchema,
            parsed.HasNapInSchema,
            parsed.HasGeoCoordinatesInSchema,
            PageScheme = NormalizeDbString(parsed.PageScheme, 10),
            CanonicalScheme = NormalizeDbString(parsed.CanonicalScheme, 10),
            parsed.RedirectsToHttps,
            parsed.HasMixedContent,
            parsed.InternalLinkCount,
            parsed.ServicePageLinkCount,
            InternalAnchorTextsJson = SerializeJson(parsed.InternalAnchorTexts),
            parsed.ImageCount,
            parsed.ImagesMissingAltCount,
            ImageAltTextsJson = SerializeJson(parsed.ImageAltTexts),
            ImageFileNamesJson = SerializeJson(parsed.ImageFileNames),
            DetectedCms = NormalizeDbString(parsed.DetectedCms, 100),
            GeneratorMetaTag = NormalizeDbString(parsed.GeneratorMetaTag, 255),
            parsed.HasViewportMeta,
            parsed.HasResponsiveIndicators,
            parsed.HasFavicon,
            parsed.HasCookieBanner,
            ServiceKeywordsJson = SerializeJson(parsed.ServiceKeywords),
            LocationKeywordsJson = SerializeJson(parsed.LocationKeywords),
            ServiceTownCombinationsJson = SerializeJson(parsed.ServiceTownCombinations),
            BrandNamesJson = SerializeJson(parsed.BrandNames),
            CreatedUtc = createdUtc
        };

    private WebsiteEligibility DetermineEligibility(string? websiteUrl, WebsiteType storedWebsiteType)
    {
        var normalizedUrl = NormalizeWebsiteUrl(websiteUrl);
        if (string.IsNullOrWhiteSpace(normalizedUrl))
            return new WebsiteEligibility(false, false, StatusIneligible, "No website URL is recorded for this place.");

        var websiteType = Enum.IsDefined(storedWebsiteType) && storedWebsiteType != WebsiteType.None
            ? storedWebsiteType
            : websiteClassifier.Classify(normalizedUrl);
        if (websiteType == WebsiteType.SocialProfile)
        {
            return new WebsiteEligibility(
                false,
                false,
                StatusIneligible,
                "The recorded website is classified as a social media profile.");
        }

        return new WebsiteEligibility(true, true, StatusNotFetched, null);
    }

    private static PlaceWebsiteFetchSummaryViewModel MapFetch(PlaceWebsiteFetchRow row)
        => new()
        {
            PlaceWebsiteFetchId = row.PlaceWebsiteFetchId,
            FetchStartedUtc = row.FetchStartedUtc,
            FetchCompletedUtc = row.FetchCompletedUtc,
            Success = row.Success,
            RequestedUrl = row.RequestedUrl,
            FinalUrl = row.FinalUrl,
            HttpStatusCode = row.HttpStatusCode,
            ErrorCode = row.ErrorCode,
            ErrorMessage = row.ErrorMessage,
            ContentType = row.ContentType,
            ResponseSizeBytes = row.ResponseSizeBytes,
            RedirectCount = row.RedirectCount,
            UsedWorker = row.UsedWorker,
            WorkerKey = row.WorkerKey
        };

    private static PlaceWebsiteHomepageAuditViewModel MapAudit(PlaceWebsiteAuditRow row)
        => new()
        {
            PlaceWebsiteFetchId = row.PlaceWebsiteFetchId,
            TitleTag = row.TitleTag,
            TitleTagLength = row.TitleTagLength,
            MetaDescription = row.MetaDescription,
            MetaDescriptionLength = row.MetaDescriptionLength,
            CanonicalUrl = row.CanonicalUrl,
            RobotsMeta = row.RobotsMeta,
            HtmlLang = row.HtmlLang,
            H1Text = row.H1Text,
            H1Count = row.H1Count,
            H2Count = row.H2Count,
            H3Count = row.H3Count,
            H2Texts = DeserializeJsonList(row.H2TextsJson),
            H3Texts = DeserializeJsonList(row.H3TextsJson),
            VisibleWordCount = row.VisibleWordCount,
            ParagraphCount = row.ParagraphCount,
            BulletListCount = row.BulletListCount,
            ContentSectionCount = row.ContentSectionCount,
            HasPhoneNumber = row.HasPhoneNumber,
            PhoneNumbers = DeserializeJsonList(row.PhoneNumbersJson),
            HasPostalAddress = row.HasPostalAddress,
            PostalAddresses = DeserializeJsonList(row.PostalAddressesJson),
            HasPostcode = row.HasPostcode,
            Postcodes = DeserializeJsonList(row.PostcodesJson),
            HasCityName = row.HasCityName,
            CityNames = DeserializeJsonList(row.CityNamesJson),
            HasBusinessName = row.HasBusinessName,
            BusinessNames = DeserializeJsonList(row.BusinessNamesJson),
            SchemaTypes = DeserializeJsonList(row.SchemaTypesJson),
            HasLocalBusinessSchema = row.HasLocalBusinessSchema,
            HasOrganizationSchema = row.HasOrganizationSchema,
            HasProductSchema = row.HasProductSchema,
            HasFaqSchema = row.HasFaqSchema,
            HasBreadcrumbSchema = row.HasBreadcrumbSchema,
            HasNapInSchema = row.HasNapInSchema,
            HasGeoCoordinatesInSchema = row.HasGeoCoordinatesInSchema,
            PageScheme = row.PageScheme,
            CanonicalScheme = row.CanonicalScheme,
            RedirectsToHttps = row.RedirectsToHttps,
            HasMixedContent = row.HasMixedContent,
            InternalLinkCount = row.InternalLinkCount,
            ServicePageLinkCount = row.ServicePageLinkCount,
            InternalAnchorTexts = DeserializeJsonList(row.InternalAnchorTextsJson),
            ImageCount = row.ImageCount,
            ImagesMissingAltCount = row.ImagesMissingAltCount,
            ImageAltTexts = DeserializeJsonList(row.ImageAltTextsJson),
            ImageFileNames = DeserializeJsonList(row.ImageFileNamesJson),
            DetectedCms = row.DetectedCms,
            GeneratorMetaTag = row.GeneratorMetaTag,
            HasViewportMeta = row.HasViewportMeta,
            HasResponsiveIndicators = row.HasResponsiveIndicators,
            HasFavicon = row.HasFavicon,
            HasCookieBanner = row.HasCookieBanner,
            ServiceKeywords = DeserializeJsonList(row.ServiceKeywordsJson),
            LocationKeywords = DeserializeJsonList(row.LocationKeywordsJson),
            ServiceTownCombinations = DeserializeJsonList(row.ServiceTownCombinationsJson),
            BrandNames = DeserializeJsonList(row.BrandNamesJson),
            CreatedUtc = row.CreatedUtc
        };

    private static string NormalizeStatus(string? currentStatus, bool workerEnabled)
    {
        var normalized = NormalizeDbString(currentStatus, 50);
        if (!string.IsNullOrWhiteSpace(normalized))
            return normalized;
        return workerEnabled ? StatusNotFetched : StatusDisabled;
    }

    private static string DetermineFailedStatus(HomePageFetchWorkerResult workerResult)
    {
        if (!workerResult.WorkerWasConfigured)
            return StatusDisabled;

        return IsBlockedStatusCode(workerResult.StatusCode)
            ? StatusBlocked
            : StatusFailed;
    }

    private static bool IsBlockedStatusCode(int? statusCode)
        => statusCode is 401 or 403 or 406 or 429;

    private static string BuildFailureMessage(HomePageFetchWorkerResult workerResult, string failedStatus)
    {
        if (string.Equals(failedStatus, StatusDisabled, StringComparison.OrdinalIgnoreCase))
        {
            return string.IsNullOrWhiteSpace(workerResult.ErrorMessage)
                ? "Cloudflare worker is not configured or enabled."
                : workerResult.ErrorMessage!;
        }

        if (string.Equals(failedStatus, StatusBlocked, StringComparison.OrdinalIgnoreCase))
        {
            var codeSuffix = workerResult.StatusCode.HasValue
                ? $" (HTTP {workerResult.StatusCode.Value})."
                : ".";
            return $"Target site blocked the homepage fetch{codeSuffix}";
        }

        return string.IsNullOrWhiteSpace(workerResult.ErrorMessage)
            ? "Homepage fetch failed."
            : workerResult.ErrorMessage!;
    }

    private static string? NormalizeWebsiteUrl(string? value)
    {
        var trimmed = NormalizeDbString(value, 1000);
        if (trimmed is null)
            return null;

        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var absolute)
            && (absolute.Scheme == Uri.UriSchemeHttp || absolute.Scheme == Uri.UriSchemeHttps))
        {
            return absolute.ToString();
        }

        if (Uri.TryCreate("https://" + trimmed, UriKind.Absolute, out var withHttps))
            return withHttps.ToString();

        return trimmed;
    }

    private static string? TryGetHost(string? value)
        => Uri.TryCreate(value, UriKind.Absolute, out var uri) ? NormalizeDbString(uri.Host, 255) : null;

    private static string? TryGetScheme(string? value)
        => Uri.TryCreate(value, UriKind.Absolute, out var uri) ? uri.Scheme : null;

    private static string? NormalizeDbString(string? value, int maxLength)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return null;
        return normalized.Length <= maxLength ? normalized : normalized[..maxLength];
    }

    private static string? SerializeJson(IReadOnlyList<string> values)
        => values.Count == 0 ? null : JsonSerializer.Serialize(values);

    private static IReadOnlyList<string> DeserializeJsonList(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        try
        {
            return JsonSerializer.Deserialize<List<string>>(json) ?? [];
        }
        catch
        {
            return [];
        }
    }

    private static string ComputeSha256(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private async Task<bool> WebsiteAnalysisSchemaIsAvailableAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT CASE
  WHEN OBJECT_ID(N'dbo.CloudflareWorker', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.PlaceWebsite', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.PlaceWebsiteFetch', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.PlaceWebsiteHomepageAudit', N'U') IS NOT NULL
  THEN 1 ELSE 0 END;",
            cancellationToken: ct)) == 1;
    }

    private sealed record WebsiteEligibility(
        bool CanShowTab,
        bool IsEligible,
        string Status,
        string? Reason);

    private sealed record PlaceContextRow(
        string PlaceId,
        string? DisplayName,
        string? WebsiteUri,
        WebsiteType WebsiteType,
        string? FormattedAddress,
        string? PrimaryCategory,
        string? SearchLocationName);

    private sealed record PlaceWebsiteRow(
        int PlaceWebsiteId,
        string PlaceId,
        string WebsiteUrl,
        string? NormalizedWebsiteUrl,
        string? HostName,
        bool? IsHttps,
        string? SourceType,
        string? Status,
        DateTime FirstDiscoveredUtc,
        DateTime? LastCheckedUtc,
        DateTime? LastSuccessfulFetchUtc,
        DateTime CreatedUtc,
        DateTime UpdatedUtc);

    private sealed record PlaceWebsiteFetchRow(
        long PlaceWebsiteFetchId,
        int PlaceWebsiteId,
        DateTime FetchStartedUtc,
        DateTime? FetchCompletedUtc,
        bool Success,
        string RequestedUrl,
        string? FinalUrl,
        int? HttpStatusCode,
        string? ErrorCode,
        string? ErrorMessage,
        string? ContentType,
        int? ResponseSizeBytes,
        int? RedirectCount,
        bool UsedWorker,
        string? WorkerKey,
        string? HtmlHash,
        DateTime CreatedUtc);

    private sealed record PlaceWebsiteAuditRow(
        long PlaceWebsiteHomepageAuditId,
        long PlaceWebsiteFetchId,
        string? TitleTag,
        int? TitleTagLength,
        string? MetaDescription,
        int? MetaDescriptionLength,
        string? CanonicalUrl,
        string? RobotsMeta,
        string? HtmlLang,
        string? H1Text,
        int? H1Count,
        int? H2Count,
        int? H3Count,
        string? H2TextsJson,
        string? H3TextsJson,
        int? VisibleWordCount,
        int? ParagraphCount,
        int? BulletListCount,
        int? ContentSectionCount,
        bool HasPhoneNumber,
        string? PhoneNumbersJson,
        bool HasPostalAddress,
        string? PostalAddressesJson,
        bool HasPostcode,
        string? PostcodesJson,
        bool HasCityName,
        string? CityNamesJson,
        bool HasBusinessName,
        string? BusinessNamesJson,
        string? SchemaTypesJson,
        bool HasLocalBusinessSchema,
        bool HasOrganizationSchema,
        bool HasProductSchema,
        bool HasFaqSchema,
        bool HasBreadcrumbSchema,
        bool HasNapInSchema,
        bool HasGeoCoordinatesInSchema,
        string? PageScheme,
        string? CanonicalScheme,
        bool? RedirectsToHttps,
        bool? HasMixedContent,
        int? InternalLinkCount,
        int? ServicePageLinkCount,
        string? InternalAnchorTextsJson,
        int? ImageCount,
        int? ImagesMissingAltCount,
        string? ImageAltTextsJson,
        string? ImageFileNamesJson,
        string? DetectedCms,
        string? GeneratorMetaTag,
        bool HasViewportMeta,
        bool HasResponsiveIndicators,
        bool HasFavicon,
        bool HasCookieBanner,
        string? ServiceKeywordsJson,
        string? LocationKeywordsJson,
        string? ServiceTownCombinationsJson,
        string? BrandNamesJson,
        DateTime CreatedUtc);
}
