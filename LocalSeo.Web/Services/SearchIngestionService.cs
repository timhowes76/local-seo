using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface ISearchIngestionService
{
    Task<long> RunAsync(SearchFormModel model, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct);
    Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct);
    Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct);
    Task<RunKeyphraseTrafficSummary?> GetRunKeyphraseTrafficSummaryAsync(long runId, CancellationToken ct);
    Task<IReadOnlyList<RunTaskProgressRow>> GetRunTaskProgressAsync(SearchRun run, CancellationToken ct);
    Task<RunReviewComparisonViewModel?> GetRunReviewComparisonAsync(long runId, CancellationToken ct);
    Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct, int reviewPage = 1, int reviewPageSize = 25);
    Task<PlaceSocialLinksEditModel?> GetPlaceSocialLinksForEditAsync(string placeId, long? runId, CancellationToken ct);
    Task<bool> UpdatePlaceSocialLinksAsync(PlaceSocialLinksEditModel model, CancellationToken ct);
    Task<bool> SavePlaceFinancialAsync(string placeId, PlaceFinancialInfoUpsert financialInfo, CancellationToken ct);
    Task<PlaceFinancialInfo?> GetPlaceFinancialAsync(string placeId, CancellationToken ct);
}


public sealed class SearchIngestionService(
    ISqlConnectionFactory connectionFactory,
    IGooglePlacesClient google,
    IOptions<PlacesOptions> placesOptions,
    IReviewsProviderResolver reviewsProviderResolver,
    DataForSeoReviewsProvider dataForSeoReviewsProvider,
    IAdminSettingsService adminSettingsService,
    IReviewVelocityService reviewVelocityService,
    ILogger<SearchIngestionService> logger) : ISearchIngestionService
{
    public async Task<long> RunAsync(SearchFormModel model, CancellationToken ct)
    {
        var normalizedCategoryId = (model.CategoryId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Category is required.");
        if (!model.TownId.HasValue || model.TownId.Value <= 0)
            throw new InvalidOperationException("Town is required.");

        SearchRunSelectionRow? selection;
        await using (var lookupConn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        {
            selection = await lookupConn.QuerySingleOrDefaultAsync<SearchRunSelectionRow>(new CommandDefinition(@"
SELECT TOP 1
  c.CategoryId,
  c.DisplayName AS CategoryDisplayName,
  t.TownId,
  t.CountyId,
  t.Name AS TownName,
  county.Name AS CountyName,
  t.Latitude AS TownLat,
  t.Longitude AS TownLng
FROM dbo.GoogleBusinessProfileCategory c
JOIN dbo.GbTown t ON t.TownId = @TownId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE c.CategoryId = @CategoryId
  AND c.Status = 'Active'
  AND t.IsActive = 1
  AND county.IsActive = 1
  AND (@CountyId IS NULL OR t.CountyId = @CountyId);", new
            {
                CategoryId = normalizedCategoryId,
                TownId = model.TownId.Value,
                CountyId = model.CountyId
            }, cancellationToken: ct));
        }
        if (selection is null)
            throw new InvalidOperationException("Selected category or town is invalid, inactive, or no longer available.");

        model.CountyId = selection.CountyId;
        model.TownId = selection.TownId;

        decimal centerLat;
        decimal centerLng;
        var locationQuery = $"{selection.TownName}, {selection.CountyName}";
        string? canonicalLocationName = locationQuery;
        var shouldPersistTownCoordinates = false;

        if (selection.TownLat.HasValue && selection.TownLng.HasValue)
        {
            centerLat = selection.TownLat.Value;
            centerLng = selection.TownLng.Value;
        }
        else
        {
            var center = await google.GeocodeAsync(locationQuery, placesOptions.Value.GeocodeCountryCode, ct);
            if (center is null)
                throw new InvalidOperationException($"Could not determine coordinates for '{locationQuery}'.");

            centerLat = center.Value.Lat;
            centerLng = center.Value.Lng;
            canonicalLocationName = center.Value.CanonicalLocationName;
            shouldPersistTownCoordinates = true;
        }

        var effectiveLocationName = !string.IsNullOrWhiteSpace(canonicalLocationName)
            ? canonicalLocationName
            : locationQuery;
        var places = await google.SearchAsync(selection.CategoryDisplayName, locationQuery, centerLat, centerLng, model.RadiusMeters, model.ResultLimit, ct);

        var normalizedLocationByPlaceId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var place in places)
        {
            var normalizedLocation = effectiveLocationName;
            if (place.Lat.HasValue && place.Lng.HasValue)
            {
                try
                {
                    var reverseCanonical = await google.ReverseGeocodeCanonicalLocationAsync(
                        place.Lat.Value,
                        place.Lng.Value,
                        placesOptions.Value.GeocodeCountryCode,
                        ct);
                    if (!string.IsNullOrWhiteSpace(reverseCanonical))
                        normalizedLocation = reverseCanonical;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Reverse geocode canonicalization failed for place {PlaceId}. Using fallback location name.", place.Id);
                }
            }

            normalizedLocationByPlaceId[place.Id] = normalizedLocation;
        }

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        if (shouldPersistTownCoordinates)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbTown
SET
  Latitude = COALESCE(Latitude, @Latitude),
  Longitude = COALESCE(Longitude, @Longitude),
  UpdatedUtc = CASE
    WHEN Latitude IS NULL OR Longitude IS NULL THEN SYSUTCDATETIME()
    ELSE UpdatedUtc
  END
WHERE TownId = @TownId;", new
            {
                Latitude = centerLat,
                Longitude = centerLng,
                TownId = selection.TownId
            }, tx, cancellationToken: ct));
        }

        var runId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SearchRun(CategoryId,TownId,RadiusMeters,ResultLimit,FetchDetailedData,FetchGoogleReviews,FetchGoogleUpdates,FetchGoogleQuestionsAndAnswers,FetchGoogleSocialProfiles)
OUTPUT INSERTED.SearchRunId
VALUES(@CategoryId,@TownId,@RadiusMeters,@ResultLimit,@FetchDetailedData,@FetchGoogleReviews,@FetchGoogleUpdates,@FetchGoogleQuestionsAndAnswers,@FetchGoogleSocialProfiles)",
            new
            {
                CategoryId = selection.CategoryId,
                TownId = selection.TownId,
                model.RadiusMeters,
                model.ResultLimit,
                FetchDetailedData = model.FetchEnhancedGoogleData,
                FetchGoogleReviews = model.FetchGoogleReviews,
                FetchGoogleUpdates = model.FetchGoogleUpdates,
                FetchGoogleQuestionsAndAnswers = model.FetchGoogleQuestionsAndAnswers,
                FetchGoogleSocialProfiles = model.FetchGoogleSocialProfiles
            }, tx, cancellationToken: ct));

        var requestGoogleReviews = model.FetchGoogleReviews;
        var requestMyBusinessInfo = model.FetchEnhancedGoogleData;
        var requestGoogleUpdates = model.FetchGoogleUpdates;
        var requestGoogleQuestionsAndAnswers = model.FetchGoogleQuestionsAndAnswers;
        var requestGoogleSocialProfiles = model.FetchGoogleSocialProfiles;
        var shouldFetchAnyDataForSeo = requestGoogleReviews || requestMyBusinessInfo || requestGoogleUpdates || requestGoogleQuestionsAndAnswers || requestGoogleSocialProfiles;

        IReviewsProvider? provider = null;
        var providerName = string.Empty;
        if (shouldFetchAnyDataForSeo)
        {
            provider = reviewsProviderResolver.Resolve(out providerName);
            if (provider is NullReviewsProvider)
            {
                provider = dataForSeoReviewsProvider;
                providerName = "DataForSeo";
                logger.LogWarning("Data enrichment was requested with provider '{ProviderName}'. Falling back to DataForSeo.", placesOptions.Value.ReviewsProvider);
            }
            if (providerName.Equals("SerpApi", StringComparison.OrdinalIgnoreCase))
                logger.LogWarning("Reviews provider selected as SerpApi, but implementation is pending.");
        }

        var reviewRequests = shouldFetchAnyDataForSeo
            ? new List<ReviewTaskRequest>()
            : null;

        for (var i = 0; i < places.Count; i++)
        {
            var p = places[i];
            var placeLocationName = normalizedLocationByPlaceId.TryGetValue(p.Id, out var normalizedLocation)
                ? normalizedLocation
                : effectiveLocationName;
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.Place AS target
USING (SELECT @PlaceId AS PlaceId) AS source
ON target.PlaceId = source.PlaceId
WHEN MATCHED THEN UPDATE SET
  DisplayName=@DisplayName,
  PrimaryType=@PrimaryType,
  PrimaryCategory=@PrimaryCategory,
  TypesCsv=@TypesCsv,
  FormattedAddress=@FormattedAddress,
  Lat=@Lat,
  Lng=@Lng,
  NationalPhoneNumber=@NationalPhoneNumber,
  WebsiteUri=@WebsiteUri,
  IsServiceAreaBusiness=@IsServiceAreaBusiness,
  BusinessStatus=@BusinessStatus,
  SearchLocationName=@SearchLocationName,
  RegularOpeningHoursJson=@RegularOpeningHoursJson,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT(
  PlaceId,DisplayName,PrimaryType,PrimaryCategory,TypesCsv,FormattedAddress,Lat,Lng,NationalPhoneNumber,WebsiteUri,IsServiceAreaBusiness,BusinessStatus,SearchLocationName,RegularOpeningHoursJson
)
VALUES(
  @PlaceId,@DisplayName,@PrimaryType,@PrimaryCategory,@TypesCsv,@FormattedAddress,@Lat,@Lng,@NationalPhoneNumber,@WebsiteUri,@IsServiceAreaBusiness,@BusinessStatus,@SearchLocationName,@RegularOpeningHoursJson
);",
                new
                {
                    PlaceId = p.Id,
                    p.DisplayName,
                    p.PrimaryType,
                    p.PrimaryCategory,
                    p.TypesCsv,
                    p.FormattedAddress,
                    p.Lat,
                    p.Lng,
                    p.NationalPhoneNumber,
                    p.WebsiteUri,
                    p.IsServiceAreaBusiness,
                    p.BusinessStatus,
                    SearchLocationName = placeLocationName,
                    RegularOpeningHoursJson = p.RegularOpeningHours.Count == 0 ? null : JsonSerializer.Serialize(p.RegularOpeningHours)
                }, tx, cancellationToken: ct));

            await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceSnapshot(SearchRunId,PlaceId,RankPosition,Rating,UserRatingCount)
VALUES(@SearchRunId,@PlaceId,@RankPosition,@Rating,@UserRatingCount)",
                new { SearchRunId = runId, PlaceId = p.Id, RankPosition = i + 1, p.Rating, p.UserRatingCount }, tx, cancellationToken: ct));

            if (reviewRequests is not null)
                reviewRequests.Add(new ReviewTaskRequest(p.Id, p.UserRatingCount, placeLocationName, p.Lat, p.Lng));
        }

        await tx.CommitAsync(ct);

        foreach (var place in places)
        {
            try
            {
                await reviewVelocityService.RecomputePlaceStatsAsync(place.Id, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Failed to recompute review velocity stats for place {PlaceId} after capture.", place.Id);
            }
        }

        if (reviewRequests is not null && provider is not null)
        {
            var settings = await adminSettingsService.GetAsync(ct);
            var enhancedHours = Math.Max(0, settings.EnhancedGoogleDataRefreshHours);
            var reviewsHours = Math.Max(0, settings.GoogleReviewsRefreshHours);
            var updatesHours = Math.Max(0, settings.GoogleUpdatesRefreshHours);
            var qasHours = Math.Max(0, settings.GoogleQuestionsAndAnswersRefreshHours);
            var socialProfilesHours = Math.Max(0, settings.GoogleSocialProfilesRefreshHours);
            var nowUtc = DateTime.UtcNow;

            var latestRunsByKey = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
            if (reviewRequests.Count > 0)
            {
                await using var latestConn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
                var placeIds = reviewRequests.Select(x => x.PlaceId).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
                var taskTypes = new List<string>();
                if (requestGoogleReviews) taskTypes.Add("reviews");
                if (requestMyBusinessInfo) taskTypes.Add("my_business_info");
                if (requestGoogleUpdates) taskTypes.Add("my_business_updates");
                if (requestGoogleQuestionsAndAnswers) taskTypes.Add("questions_and_answers");
                if (requestGoogleSocialProfiles) taskTypes.Add("social_profiles");

                if (taskTypes.Count > 0)
                {
                    var latestRows = await latestConn.QueryAsync<LatestTaskRunByPlaceRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  COALESCE(TaskType, 'reviews') AS TaskType,
  MAX(CreatedAtUtc) AS LastCreatedAtUtc
FROM dbo.DataForSeoReviewTask
WHERE PlaceId IN @PlaceIds
  AND COALESCE(TaskType, 'reviews') IN @TaskTypes
GROUP BY PlaceId, COALESCE(TaskType, 'reviews');",
                        new { PlaceIds = placeIds, TaskTypes = taskTypes }, cancellationToken: ct));

                    foreach (var row in latestRows)
                    {
                        latestRunsByKey[$"{row.PlaceId}|{row.TaskType}"] = row.LastCreatedAtUtc;
                    }
                }
            }

            logger.LogInformation(
                "Detailed data requested. Provider {ProviderName}. Creating DataForSEO tasks for {PlaceCount} places. Reviews={Reviews}, MyBusinessInfo={MyBusinessInfo}, Updates={Updates}, QAs={QAs}, SocialProfiles={SocialProfiles}.",
                providerName,
                reviewRequests.Count,
                requestGoogleReviews,
                requestMyBusinessInfo,
                requestGoogleUpdates,
                requestGoogleQuestionsAndAnswers,
                requestGoogleSocialProfiles);
            foreach (var reviewRequest in reviewRequests)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var reviewsDue = requestGoogleReviews && IsTaskDue(reviewRequest.PlaceId, "reviews", reviewsHours, nowUtc, latestRunsByKey);
                    var infoDue = requestMyBusinessInfo && IsTaskDue(reviewRequest.PlaceId, "my_business_info", enhancedHours, nowUtc, latestRunsByKey);
                    var updatesDue = requestGoogleUpdates && IsTaskDue(reviewRequest.PlaceId, "my_business_updates", updatesHours, nowUtc, latestRunsByKey);
                    var qasDue = requestGoogleQuestionsAndAnswers && IsTaskDue(reviewRequest.PlaceId, "questions_and_answers", qasHours, nowUtc, latestRunsByKey);
                    var socialDue = requestGoogleSocialProfiles && IsTaskDue(reviewRequest.PlaceId, "social_profiles", socialProfilesHours, nowUtc, latestRunsByKey);

                    if (!reviewsDue && requestGoogleReviews)
                        logger.LogInformation("Skipping reviews task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, reviewsHours);
                    if (!infoDue && requestMyBusinessInfo)
                        logger.LogInformation("Skipping my_business_info task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, enhancedHours);
                    if (!updatesDue && requestGoogleUpdates)
                        logger.LogInformation("Skipping my_business_updates task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, updatesHours);
                    if (!qasDue && requestGoogleQuestionsAndAnswers)
                        logger.LogInformation("Skipping questions_and_answers task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, qasHours);
                    if (!socialDue && requestGoogleSocialProfiles)
                        logger.LogInformation("Skipping social_profiles task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, socialProfilesHours);

                    if (!reviewsDue && !infoDue && !updatesDue && !qasDue && !socialDue)
                        continue;

                    await provider.FetchAndStoreReviewsAsync(
                        reviewRequest.PlaceId,
                        reviewRequest.ReviewCount,
                        reviewRequest.LocationName,
                        reviewRequest.Lat ?? centerLat,
                        reviewRequest.Lng ?? centerLng,
                        model.RadiusMeters,
                        reviewsDue,
                        infoDue,
                        updatesDue,
                        qasDue,
                        socialDue,
                        ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Review fetch failed for place {PlaceId} using provider {ProviderName}.", reviewRequest.PlaceId, providerName);
                }
            }
        }

        return runId;
    }

    private static bool IsTaskDue(
        string placeId,
        string taskType,
        int thresholdHours,
        DateTime nowUtc,
        IReadOnlyDictionary<string, DateTime> latestRunsByKey)
    {
        var key = $"{placeId}|{taskType}";
        if (!latestRunsByKey.TryGetValue(key, out var lastRunUtc))
            return true;

        return (nowUtc - lastRunUtc) >= TimeSpan.FromHours(Math.Max(0, thresholdHours));
    }

    public async Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT TOP (@Take)
  r.SearchRunId,
  r.CategoryId,
  r.TownId,
  t.CountyId,
  c.DisplayName AS SeedKeyword,
  CONCAT(t.Name, N', ', county.Name) AS LocationName,
  t.Latitude AS CenterLat,
  t.Longitude AS CenterLng,
  r.RadiusMeters,
  r.ResultLimit,
  r.FetchDetailedData,
  r.FetchGoogleReviews,
  r.FetchGoogleUpdates,
  r.FetchGoogleQuestionsAndAnswers,
  r.FetchGoogleSocialProfiles,
  r.RanAtUtc
FROM dbo.SearchRun r
JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
JOIN dbo.GbTown t ON t.TownId = r.TownId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
ORDER BY r.SearchRunId DESC", new { Take = take }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<SearchRun>(new CommandDefinition(@"
SELECT
  r.SearchRunId,
  r.CategoryId,
  r.TownId,
  t.CountyId,
  c.DisplayName AS SeedKeyword,
  CONCAT(t.Name, N', ', county.Name) AS LocationName,
  t.Latitude AS CenterLat,
  t.Longitude AS CenterLng,
  r.RadiusMeters,
  r.ResultLimit,
  r.FetchDetailedData,
  r.FetchGoogleReviews,
  r.FetchGoogleUpdates,
  r.FetchGoogleQuestionsAndAnswers,
  r.FetchGoogleSocialProfiles,
  r.RanAtUtc
FROM dbo.SearchRun r
JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
JOIN dbo.GbTown t ON t.TownId = r.TownId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE r.SearchRunId=@RunId", new { RunId = runId }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = (await conn.QueryAsync<PlaceSnapshotRow>(new CommandDefinition(@"
SELECT s.PlaceSnapshotId, s.SearchRunId, s.PlaceId, s.RankPosition, s.Rating, s.UserRatingCount, s.CapturedAtUtc,
       p.DisplayName, p.PrimaryCategory, p.PhotoCount, p.NationalPhoneNumber, p.Lat, p.Lng, p.FormattedAddress, p.WebsiteUri, p.QuestionAnswerCount,
       COALESCE(updCount.UpdateCount, 0) AS UpdateCount,
       LEN(COALESCE(p.Description, N'')) AS DescriptionLength,
       CASE
         WHEN p.OtherCategoriesJson IS NULL THEN CAST(0 AS bit)
         WHEN LTRIM(RTRIM(p.OtherCategoriesJson)) IN (N'', N'[]', N'{}') THEN CAST(0 AS bit)
         ELSE CAST(1 AS bit)
       END AS HasOtherCategories,
       v.ReviewsLast90, v.AvgPerMonth12m, v.Trend90Pct, v.DaysSinceLastReview,
       CASE WHEN upd.LastUpdateDate IS NULL THEN NULL ELSE DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) END AS DaysSinceLastUpdate,
       CASE
         WHEN latestUpdateTask.Status IN ('NoData','CompletedNoUpdates','CompletedNoData') THEN 'No Data'
         WHEN upd.LastUpdateDate IS NULL THEN 'NoUpdates'
         WHEN DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) > 60 THEN 'Stalled'
         WHEN updTrend.Trend90Pct <= -30 THEN 'Slowing'
         WHEN updTrend.Trend90Pct >= 30 AND DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) <= 30 THEN 'Accelerating'
         ELSE 'Healthy'
       END AS UpdateStatusLabel,
       v.StatusLabel, v.MomentumScore,
       p.LogoUrl,
       CAST(CASE WHEN pf.PlaceId IS NULL THEN 0 ELSE 1 END AS bit) AS HasFinancialInfo,
       CAST(COALESCE(p.ZohoLeadCreated, 0) AS bit) AS IsZohoConnected
FROM dbo.PlaceSnapshot s
JOIN dbo.Place p ON p.PlaceId=s.PlaceId
LEFT JOIN dbo.PlacesFinancial pf ON pf.PlaceId=s.PlaceId
LEFT JOIN dbo.PlaceReviewVelocityStats v ON v.PlaceId=s.PlaceId
OUTER APPLY (
  SELECT
    MAX(CAST(effective.EffectiveUpdateUtc AS date)) AS LastUpdateDate,
    SUM(CASE WHEN effective.EffectiveUpdateUtc >= DATEADD(day,-90,SYSUTCDATETIME()) THEN 1 ELSE 0 END) AS UpdatesLast90,
    SUM(CASE WHEN effective.EffectiveUpdateUtc >= DATEADD(day,-180,SYSUTCDATETIME()) AND effective.EffectiveUpdateUtc < DATEADD(day,-90,SYSUTCDATETIME()) THEN 1 ELSE 0 END) AS PrevUpdates90
  FROM dbo.PlaceUpdate u
  CROSS APPLY (
    SELECT COALESCE(
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.timestamp') AS datetimeoffset(0)) AS datetime2(0)),
      CASE
        WHEN TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) BETWEEN 1000000000 AND 9999999999
          THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp'))), CAST('1970-01-01T00:00:00' AS datetime2(0)))
        WHEN TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) BETWEEN 1000000000000 AND 9999999999999
          THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) / 1000), CAST('1970-01-01T00:00:00' AS datetime2(0)))
        ELSE NULL
      END,
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.post_date') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.date_posted') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.posted_at') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.date') AS datetimeoffset(0)) AS datetime2(0)),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.timestamp'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.post_date'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.date_posted'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.posted_at'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.date'), 112),
      u.PostDateUtc,
      u.FirstSeenUtc
    ) AS EffectiveUpdateUtc
  ) effective
  WHERE u.PlaceId=s.PlaceId
    AND effective.EffectiveUpdateUtc IS NOT NULL
) upd
OUTER APPLY (
  SELECT CAST(CASE
    WHEN COALESCE(upd.PrevUpdates90, 0) = 0 AND COALESCE(upd.UpdatesLast90, 0) = 0 THEN 0
    WHEN COALESCE(upd.PrevUpdates90, 0) = 0 AND COALESCE(upd.UpdatesLast90, 0) > 0 THEN 100
    ELSE ((COALESCE(upd.UpdatesLast90, 0) - COALESCE(upd.PrevUpdates90, 0)) * 100.0) / NULLIF(COALESCE(upd.PrevUpdates90, 0), 0)
  END AS decimal(9,2)) AS Trend90Pct
) updTrend
OUTER APPLY (
  SELECT TOP 1 Status
  FROM dbo.DataForSeoReviewTask t
  WHERE t.PlaceId=s.PlaceId
    AND COALESCE(t.TaskType, 'reviews')='my_business_updates'
  ORDER BY t.CreatedAtUtc DESC, t.DataForSeoReviewTaskId DESC
) latestUpdateTask
OUTER APPLY (
  SELECT COUNT(1) AS UpdateCount
  FROM dbo.PlaceUpdate u2
  WHERE u2.PlaceId=s.PlaceId
) updCount
WHERE s.SearchRunId=@RunId
ORDER BY s.RankPosition", new { RunId = runId }, cancellationToken: ct))).ToList();

        var keyphraseTraffic = await GetRunKeyphraseTrafficSummaryCoreAsync(conn, runId, ct);
        if (keyphraseTraffic is null)
            return rows;

        var settings = await adminSettingsService.GetAsync(ct);
        var weightedAvg = Math.Max(0, keyphraseTraffic.WeightedAvgSearchVolume);
        var weightedLastMonth = keyphraseTraffic.WeightedLastMonthSearchVolume;

        return rows
            .Select(row => row with
            {
                AvgClicks = CalculateEstimatedMapPackClicks(row.RankPosition, weightedAvg, settings),
                LastMonthClicks = weightedLastMonth.HasValue
                    ? CalculateEstimatedMapPackClicks(row.RankPosition, weightedLastMonth.Value, settings)
                    : null
            })
            .ToList();
    }

    public async Task<RunKeyphraseTrafficSummary?> GetRunKeyphraseTrafficSummaryAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await GetRunKeyphraseTrafficSummaryCoreAsync(conn, runId, ct);
    }

    public async Task<IReadOnlyList<RunTaskProgressRow>> GetRunTaskProgressAsync(SearchRun run, CancellationToken ct)
    {
        var selectedTaskTypes = new List<(string TaskType, string Label)>();
        if (run.FetchDetailedData)
            selectedTaskTypes.Add(("my_business_info", "Google Enhanced Data"));
        if (run.FetchGoogleReviews)
            selectedTaskTypes.Add(("reviews", "Google Reviews"));
        if (run.FetchGoogleUpdates)
            selectedTaskTypes.Add(("my_business_updates", "Google Updates"));
        if (run.FetchGoogleQuestionsAndAnswers)
            selectedTaskTypes.Add(("questions_and_answers", "Google Question & Answers"));
        if (run.FetchGoogleSocialProfiles)
            selectedTaskTypes.Add(("social_profiles", "Google Social Profiles"));

        if (selectedTaskTypes.Count == 0)
            return [];

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalPlaces = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(DISTINCT PlaceId)
FROM dbo.PlaceSnapshot
WHERE SearchRunId=@RunId;", new { RunId = run.SearchRunId }, cancellationToken: ct));

        var types = selectedTaskTypes.Select(x => x.TaskType).ToArray();
        var statusRows = (await conn.QueryAsync<RunTaskProgressStatusRow>(new CommandDefinition(@"
WITH run_places AS (
  SELECT DISTINCT PlaceId
  FROM dbo.PlaceSnapshot
  WHERE SearchRunId=@RunId
),
latest AS (
  SELECT
    t.PlaceId,
    COALESCE(t.TaskType, 'reviews') AS TaskType,
    t.Status,
    ROW_NUMBER() OVER (
      PARTITION BY t.PlaceId, COALESCE(t.TaskType, 'reviews')
      ORDER BY t.CreatedAtUtc DESC, t.DataForSeoReviewTaskId DESC
    ) AS rn
  FROM dbo.DataForSeoReviewTask t
  JOIN run_places rp ON rp.PlaceId=t.PlaceId
  WHERE t.CreatedAtUtc >= @RanAtUtc
    AND COALESCE(t.TaskType, 'reviews') IN @TaskTypes
)
SELECT
  TaskType,
  SUM(CASE WHEN Status IN ('Pending','Ready','Created') THEN 1 ELSE 0 END) AS ProcessingCount,
  SUM(CASE WHEN Status IN ('Populated','NoData','CompletedNoReviews','CompletedNoData','CompletedNoUpdates') THEN 1 ELSE 0 END) AS CompletedCount,
  SUM(CASE WHEN Status='Error' THEN 1 ELSE 0 END) AS ErrorCount,
  COUNT(1) AS DueCount
FROM latest
WHERE rn = 1
GROUP BY TaskType;",
            new
            {
                RunId = run.SearchRunId,
                RanAtUtc = run.RanAtUtc,
                TaskTypes = types
            }, cancellationToken: ct))).ToList();

        var byType = statusRows.ToDictionary(x => x.TaskType, StringComparer.OrdinalIgnoreCase);
        var result = new List<RunTaskProgressRow>(selectedTaskTypes.Count);
        foreach (var selected in selectedTaskTypes)
        {
            byType.TryGetValue(selected.TaskType, out var row);
            result.Add(new RunTaskProgressRow(
                selected.TaskType,
                selected.Label,
                totalPlaces,
                row?.DueCount ?? 0,
                row?.ProcessingCount ?? 0,
                row?.CompletedCount ?? 0,
                row?.ErrorCount ?? 0));
        }

        return result;
    }

    public async Task<RunReviewComparisonViewModel?> GetRunReviewComparisonAsync(long runId, CancellationToken ct)
    {
        var run = await GetRunAsync(runId, ct);
        if (run is null)
            return null;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var rowData = (await conn.QueryAsync<RunReviewComparisonRowData>(new CommandDefinition(@"
WITH run_places AS (
  SELECT
    s.PlaceId,
    MIN(s.RankPosition) AS RankPosition
  FROM dbo.PlaceSnapshot s
  WHERE s.SearchRunId=@RunId
  GROUP BY s.PlaceId
)
SELECT
  rp.PlaceId,
  rp.RankPosition,
  COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), rp.PlaceId) AS DisplayName,
  latestSnapshot.Rating AS AvgRating,
  latestSnapshot.UserRatingCount,
  COALESCE(v.ReviewsLast90, 0) AS ReviewsLast90,
  COALESCE(v.ReviewsLast180, 0) AS ReviewsLast180,
  COALESCE(v.ReviewsLast270, 0) AS ReviewsLast270,
  COALESCE(v.ReviewsLast365, 0) AS ReviewsLast365,
  v.AvgPerMonth12m,
  v.DaysSinceLastReview,
  v.LongestGapDays12m,
  v.RespondedPct12m,
  v.AvgOwnerResponseHours12m,
  COALESCE(NULLIF(LTRIM(RTRIM(v.StatusLabel)), N''), N'NoReviews') AS StatusLabel
FROM run_places rp
LEFT JOIN dbo.Place p ON p.PlaceId=rp.PlaceId
OUTER APPLY (
  SELECT TOP 1
    s.Rating,
    s.UserRatingCount
  FROM dbo.PlaceSnapshot s
  WHERE s.SearchRunId=@RunId
    AND s.PlaceId=rp.PlaceId
  ORDER BY s.RankPosition ASC, s.CapturedAtUtc DESC, s.PlaceSnapshotId DESC
) latestSnapshot
LEFT JOIN dbo.PlaceReviewVelocityStats v ON v.PlaceId=rp.PlaceId
ORDER BY rp.RankPosition, COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), rp.PlaceId);",
            new { RunId = runId }, cancellationToken: ct))).ToList();

        var rows = rowData
            .Select(x => new RunReviewComparisonRow(
                x.PlaceId,
                x.RankPosition,
                x.DisplayName,
                x.AvgRating,
                x.UserRatingCount,
                Math.Max(0, x.ReviewsLast90),
                Math.Max(0, x.ReviewsLast180 - x.ReviewsLast90),
                Math.Max(0, x.ReviewsLast270 - x.ReviewsLast180),
                Math.Max(0, x.ReviewsLast365 - x.ReviewsLast270),
                x.AvgPerMonth12m,
                x.DaysSinceLastReview,
                x.LongestGapDays12m,
                x.RespondedPct12m,
                x.AvgOwnerResponseHours12m,
                string.IsNullOrWhiteSpace(x.StatusLabel) ? "NoReviews" : x.StatusLabel))
            .ToList();

        var monthlyCounts = (await conn.QueryAsync<RunReviewMonthlyCountRow>(new CommandDefinition(@"
WITH run_places AS (
  SELECT DISTINCT PlaceId
  FROM dbo.PlaceSnapshot
  WHERE SearchRunId=@RunId
),
review_points AS (
  SELECT
    rp.PlaceId,
    COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), rp.PlaceId) AS DisplayName,
    COALESCE(r.ReviewTimestampUtc, r.LastSeenUtc) AS EventUtc
  FROM run_places rp
  LEFT JOIN dbo.Place p ON p.PlaceId=rp.PlaceId
  LEFT JOIN dbo.PlaceReview r ON r.PlaceId=rp.PlaceId
)
SELECT
  PlaceId,
  DisplayName,
  YEAR(EventUtc) AS [Year],
  MONTH(EventUtc) AS [Month],
  COUNT(1) AS ReviewCount
FROM review_points
WHERE EventUtc IS NOT NULL
GROUP BY PlaceId, DisplayName, YEAR(EventUtc), MONTH(EventUtc)
ORDER BY PlaceId, [Year], [Month];",
            new { RunId = runId }, cancellationToken: ct))).ToList();

        var displayNameByPlaceId = rows.ToDictionary(x => x.PlaceId, x => x.DisplayName, StringComparer.OrdinalIgnoreCase);
        var rankByPlaceId = rows.ToDictionary(x => x.PlaceId, x => x.RankPosition, StringComparer.OrdinalIgnoreCase);

        var series = monthlyCounts
            .GroupBy(x => x.PlaceId, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var ordered = group
                    .OrderBy(x => x.Year)
                    .ThenBy(x => x.Month)
                    .ToList();

                var runningTotal = 0;
                var points = ordered
                    .Select(x =>
                    {
                        runningTotal += x.ReviewCount;
                        return new RunReviewComparisonSeriesPoint(x.Year, x.Month, runningTotal);
                    })
                    .ToList();

                displayNameByPlaceId.TryGetValue(group.Key, out var displayName);
                return new
                {
                    PlaceId = group.Key,
                    DisplayName = displayName ?? group.First().DisplayName,
                    Points = (IReadOnlyList<RunReviewComparisonSeriesPoint>)points
                };
            })
            .OrderBy(x => rankByPlaceId.TryGetValue(x.PlaceId, out var rank) ? rank : int.MaxValue)
            .ThenBy(x => x.DisplayName)
            .Select(x => new RunReviewComparisonSeries(x.PlaceId, x.DisplayName, x.Points))
            .ToList();

        return new RunReviewComparisonViewModel(run, rows, series);
    }

    public async Task<bool> UpdatePlaceSocialLinksAsync(PlaceSocialLinksEditModel model, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(model);

        var placeId = (model.PlaceId ?? string.Empty).Trim();
        if (placeId.Length == 0)
            throw new InvalidOperationException("Place ID is required.");

        var facebookUrl = NormalizeSocialUrl(model.FacebookUrl, "Facebook URL");
        var instagramUrl = NormalizeSocialUrl(model.InstagramUrl, "Instagram URL");
        var linkedInUrl = NormalizeSocialUrl(model.LinkedInUrl, "LinkedIn URL");
        var xUrl = NormalizeSocialUrl(model.XUrl, "X URL");
        var youTubeUrl = NormalizeSocialUrl(model.YouTubeUrl, "YouTube URL");
        var tikTokUrl = NormalizeSocialUrl(model.TikTokUrl, "TikTok URL");
        var pinterestUrl = NormalizeSocialUrl(model.PinterestUrl, "Pinterest URL");
        var blueskyUrl = NormalizeSocialUrl(model.BlueskyUrl, "Bluesky URL");

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var touched = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Place
SET
  FacebookUrl = @FacebookUrl,
  InstagramUrl = @InstagramUrl,
  LinkedInUrl = @LinkedInUrl,
  XUrl = @XUrl,
  YouTubeUrl = @YouTubeUrl,
  TikTokUrl = @TikTokUrl,
  PinterestUrl = @PinterestUrl,
  BlueskyUrl = @BlueskyUrl,
  LastSeenUtc = SYSUTCDATETIME()
WHERE PlaceId = @PlaceId;", new
        {
            PlaceId = placeId,
            FacebookUrl = facebookUrl,
            InstagramUrl = instagramUrl,
            LinkedInUrl = linkedInUrl,
            XUrl = xUrl,
            YouTubeUrl = youTubeUrl,
            TikTokUrl = tikTokUrl,
            PinterestUrl = pinterestUrl,
            BlueskyUrl = blueskyUrl
        }, cancellationToken: ct));

        return touched > 0;
    }

    public async Task<PlaceSocialLinksEditModel?> GetPlaceSocialLinksForEditAsync(string placeId, long? runId, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            throw new InvalidOperationException("Place ID is required.");

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<PlaceSocialLinksEditRow>(new CommandDefinition(@"
SELECT TOP 1
  PlaceId,
  DisplayName,
  FacebookUrl,
  InstagramUrl,
  LinkedInUrl,
  XUrl,
  YouTubeUrl,
  TikTokUrl,
  PinterestUrl,
  BlueskyUrl
FROM dbo.Place
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct));
        if (row is null)
            return null;

        return new PlaceSocialLinksEditModel
        {
            PlaceId = row.PlaceId,
            RunId = runId,
            DisplayName = row.DisplayName,
            FacebookUrl = row.FacebookUrl,
            InstagramUrl = row.InstagramUrl,
            LinkedInUrl = row.LinkedInUrl,
            XUrl = row.XUrl,
            YouTubeUrl = row.YouTubeUrl,
            TikTokUrl = row.TikTokUrl,
            PinterestUrl = row.PinterestUrl,
            BlueskyUrl = row.BlueskyUrl
        };
    }

    public async Task<bool> SavePlaceFinancialAsync(string placeId, PlaceFinancialInfoUpsert financialInfo, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(financialInfo);

        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            throw new InvalidOperationException("Place ID is required.");

        var normalizedCompanyNumber = (financialInfo.CompanyNumber ?? string.Empty).Trim();
        if (normalizedCompanyNumber.Length == 0)
            throw new InvalidOperationException("Company number is required.");
        if (normalizedCompanyNumber.Length > 32)
            normalizedCompanyNumber = normalizedCompanyNumber[..32];

        var normalizedCompanyType = string.IsNullOrWhiteSpace(financialInfo.CompanyType)
            ? null
            : financialInfo.CompanyType.Trim();
        if (normalizedCompanyType is not null && normalizedCompanyType.Length > 80)
            normalizedCompanyType = normalizedCompanyType[..80];

        var normalizedCompanyStatus = string.IsNullOrWhiteSpace(financialInfo.CompanyStatus)
            ? null
            : financialInfo.CompanyStatus.Trim();
        if (normalizedCompanyStatus is not null && normalizedCompanyStatus.Length > 80)
            normalizedCompanyStatus = normalizedCompanyStatus[..80];

        var officers = (financialInfo.Officers ?? [])
            .Select(x => new
            {
                FirstNames = NormalizeAndTrim(x.FirstNames, 200),
                LastName = NormalizeAndTrim(x.LastName, 200),
                CountryOfResidence = NormalizeAndTrim(x.CountryOfResidence, 100),
                DateOfBirth = x.DateOfBirth?.Date,
                Nationality = NormalizeAndTrim(x.Nationality, 100),
                Role = NormalizeAndTrim(x.Role, 80),
                Appointed = x.Appointed?.Date,
                Resigned = x.Resigned?.Date
            })
            .Where(x =>
                x.FirstNames is not null
                || x.LastName is not null
                || x.CountryOfResidence is not null
                || x.DateOfBirth.HasValue
                || x.Nationality is not null
                || x.Role is not null
                || x.Appointed.HasValue
                || x.Resigned.HasValue)
            .ToList();

        var pscEntries = (financialInfo.PersonsWithSignificantControl ?? [])
            .Select(x =>
            {
                var normalizedPscCompanyNumber = NormalizeAndTrim(x.CompanyNumber, 32) ?? normalizedCompanyNumber;
                var normalizedPscId = NormalizeAndTrim(x.PscId, 120);
                var normalizedNameRaw = NormalizeAndTrim(x.NameRaw, 300);
                var normalizedFirstNames = NormalizeAndTrim(x.FirstNames, 150);
                var normalizedLastName = NormalizeAndTrim(x.LastName, 150);
                var normalizedCountryOfResidence = NormalizeAndTrim(x.CountryOfResidence, 100);
                var normalizedNationality = NormalizeAndTrim(x.Nationality, 100);
                var normalizedItemKind = NormalizeAndTrim(x.PscItemKind, 120);
                var normalizedLinkSelf = NormalizeAndTrim(x.PscLinkSelf, 500);
                var normalizedSourceEtag = NormalizeAndTrim(x.SourceEtag, 100);
                var normalizedRawJson = string.IsNullOrWhiteSpace(x.RawJson) ? null : x.RawJson.Trim();
                var normalizedBirthMonth = x.BirthMonth is >= 1 and <= 12 ? x.BirthMonth : null;
                var normalizedBirthYear = x.BirthYear is >= 1 and <= 9999 ? x.BirthYear : null;
                var normalizedRetrievedUtc = x.RetrievedUtc == default ? DateTime.UtcNow : x.RetrievedUtc;
                var normalizedNatureCodes = (x.NatureCodes ?? [])
                    .Select(code => NormalizeAndTrim(code, 200))
                    .Where(code => code is not null)
                    .Cast<string>()
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
                return new NormalizedPscUpsertRow(
                    normalizedPscCompanyNumber,
                    normalizedItemKind,
                    normalizedLinkSelf,
                    normalizedPscId,
                    normalizedNameRaw,
                    normalizedFirstNames,
                    normalizedLastName,
                    normalizedCountryOfResidence,
                    normalizedNationality,
                    normalizedBirthMonth,
                    normalizedBirthYear,
                    x.NotifiedOn?.Date,
                    x.CeasedOn?.Date,
                    normalizedSourceEtag,
                    normalizedRetrievedUtc,
                    normalizedRawJson,
                    normalizedNatureCodes);
            })
            .Where(x =>
                x.PscItemKind is not null
                || x.PscLinkSelf is not null
                || x.PscId is not null
                || x.NameRaw is not null
                || x.FirstNames is not null
                || x.LastName is not null
                || x.CountryOfResidence is not null
                || x.Nationality is not null
                || x.BirthMonth.HasValue
                || x.BirthYear.HasValue
                || x.NotifiedOn.HasValue
                || x.CeasedOn.HasValue
                || x.SourceEtag is not null
                || x.NatureCodes.Count > 0)
            .ToList();

        var pscDedupKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var dedupedPscEntries = new List<NormalizedPscUpsertRow>();
        foreach (var psc in pscEntries)
        {
            var fallbackKey = string.Concat(
                "fallback|",
                psc.NameRaw ?? string.Empty,
                "|",
                psc.BirthMonth?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                "|",
                psc.BirthYear?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                "|",
                psc.CeasedOn?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty);
            var dedupKey = psc.PscId is not null
                ? $"id|{psc.PscItemKind ?? string.Empty}|{psc.PscId}"
                : fallbackKey;
            if (!pscDedupKeys.Add(dedupKey))
                continue;
            dedupedPscEntries.Add(psc);
        }

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var placeExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.Place
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct));
        if (placeExists <= 0)
            return false;

        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
IF EXISTS (SELECT 1 FROM dbo.PlacesFinancial WHERE PlaceId = @PlaceId)
BEGIN
  UPDATE dbo.PlacesFinancial
  SET
    DateOfCreation = @DateOfCreation,
    CompanyNumber = @CompanyNumber,
    CompanyType = @CompanyType,
    LastAccountsFiled = @LastAccountsFiled,
    NextAccountsDue = @NextAccountsDue,
    CompanyStatus = @CompanyStatus,
    HasLiquidated = @HasLiquidated,
    HasCharges = @HasCharges,
    HasInsolvencyHistory = @HasInsolvencyHistory,
    LastUpdatedUtc = SYSUTCDATETIME()
  WHERE PlaceId = @PlaceId;
END
ELSE
BEGIN
  INSERT INTO dbo.PlacesFinancial(
    PlaceId,
    DateOfCreation,
    CompanyNumber,
    CompanyType,
    LastAccountsFiled,
    NextAccountsDue,
    CompanyStatus,
    HasLiquidated,
    HasCharges,
    HasInsolvencyHistory
  )
  VALUES(
    @PlaceId,
    @DateOfCreation,
    @CompanyNumber,
    @CompanyType,
    @LastAccountsFiled,
    @NextAccountsDue,
    @CompanyStatus,
    @HasLiquidated,
    @HasCharges,
    @HasInsolvencyHistory
  );
END;", new
        {
            PlaceId = normalizedPlaceId,
            DateOfCreation = financialInfo.DateOfCreation?.Date,
            CompanyNumber = normalizedCompanyNumber,
            CompanyType = normalizedCompanyType,
            LastAccountsFiled = financialInfo.LastAccountsFiled?.Date,
            NextAccountsDue = financialInfo.NextAccountsDue?.Date,
            CompanyStatus = normalizedCompanyStatus,
            HasLiquidated = financialInfo.HasLiquidated,
            HasCharges = financialInfo.HasCharges,
            HasInsolvencyHistory = financialInfo.HasInsolvencyHistory
        }, transaction: tx, cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.PlacesFinancialOfficers
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, transaction: tx, cancellationToken: ct));

        if (officers.Count > 0)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlacesFinancialOfficers(
  PlaceId,
  FirstNames,
  LastName,
  CountryOfResidence,
  DateOfBirth,
  Nationality,
  Role,
  Appointed,
  Resigned
)
VALUES(
  @PlaceId,
  @FirstNames,
  @LastName,
  @CountryOfResidence,
  @DateOfBirth,
  @Nationality,
  @Role,
  @Appointed,
  @Resigned
);",
                officers.Select(x => new
                {
                    PlaceId = normalizedPlaceId,
                    x.FirstNames,
                    x.LastName,
                    x.CountryOfResidence,
                    x.DateOfBirth,
                    x.Nationality,
                    x.Role,
                    x.Appointed,
                    x.Resigned
                }),
                transaction: tx,
                cancellationToken: ct));
        }

        await conn.ExecuteAsync(new CommandDefinition(@"
DELETE n
FROM dbo.PlaceFinancialPSC_NatureOfControl n
JOIN dbo.PlaceFinancialPersonsOfSignificantControl p ON p.Id = n.PSCId
WHERE p.PlaceId = @PlaceId;

DELETE FROM dbo.PlaceFinancialPersonsOfSignificantControl
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, transaction: tx, cancellationToken: ct));

        foreach (var psc in dedupedPscEntries)
        {
            var pscDbId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.PlaceFinancialPersonsOfSignificantControl(
  PlaceId,
  CompanyNumber,
  PscItemKind,
  PscLinkSelf,
  PscId,
  NameRaw,
  FirstNames,
  LastName,
  CountryOfResidence,
  Nationality,
  BirthMonth,
  BirthYear,
  NotifiedOn,
  CeasedOn,
  SourceEtag,
  RetrievedUtc,
  RawJson
)
VALUES(
  @PlaceId,
  @CompanyNumber,
  @PscItemKind,
  @PscLinkSelf,
  @PscId,
  @NameRaw,
  @FirstNames,
  @LastName,
  @CountryOfResidence,
  @Nationality,
  @BirthMonth,
  @BirthYear,
  @NotifiedOn,
  @CeasedOn,
  @SourceEtag,
  @RetrievedUtc,
  @RawJson
);
SELECT CAST(SCOPE_IDENTITY() AS bigint);", new
            {
                PlaceId = normalizedPlaceId,
                psc.CompanyNumber,
                psc.PscItemKind,
                psc.PscLinkSelf,
                psc.PscId,
                psc.NameRaw,
                psc.FirstNames,
                psc.LastName,
                psc.CountryOfResidence,
                psc.Nationality,
                psc.BirthMonth,
                psc.BirthYear,
                psc.NotifiedOn,
                psc.CeasedOn,
                psc.SourceEtag,
                psc.RetrievedUtc,
                psc.RawJson
            }, transaction: tx, cancellationToken: ct));

            if (psc.NatureCodes.Count <= 0)
                continue;

            await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceFinancialPSC_NatureOfControl(
  PSCId,
  NatureCode
)
VALUES(
  @PSCId,
  @NatureCode
);", psc.NatureCodes.Select(code => new
            {
                PSCId = pscDbId,
                NatureCode = code
            }), transaction: tx, cancellationToken: ct));
        }

        await tx.CommitAsync(ct);

        return true;
    }

    public async Task<PlaceFinancialInfo?> GetPlaceFinancialAsync(string placeId, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            throw new InvalidOperationException("Place ID is required.");

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<PlaceFinancialRow>(new CommandDefinition(@"
SELECT TOP 1
  PlaceId,
  DateOfCreation,
  CompanyNumber,
  CompanyType,
  LastAccountsFiled,
  NextAccountsDue,
  CompanyStatus,
  HasLiquidated,
  HasCharges,
  HasInsolvencyHistory
FROM dbo.PlacesFinancial
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct));

        if (row is null || string.IsNullOrWhiteSpace(row.CompanyNumber))
            return null;

        return new PlaceFinancialInfo(
            row.PlaceId,
            row.DateOfCreation,
            row.CompanyNumber,
            row.CompanyType,
            row.LastAccountsFiled,
            row.NextAccountsDue,
            row.CompanyStatus,
            row.HasLiquidated,
            row.HasCharges,
            row.HasInsolvencyHistory);
    }

    public async Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct, int reviewPage = 1, int reviewPageSize = 25)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var effectiveReviewPageSize = Math.Clamp(reviewPageSize, 10, 100);
        var effectiveReviewPage = Math.Max(1, reviewPage);

        var place = await conn.QuerySingleOrDefaultAsync<PlaceDetailsRow>(new CommandDefinition(@"
SELECT
  p.PlaceId,
  p.DisplayName,
  p.LogoUrl,
  p.MainPhotoUrl,
  p.FormattedAddress,
  p.PrimaryType,
  p.PrimaryCategory,
  p.TypesCsv,
  p.NationalPhoneNumber,
  p.WebsiteUri,
  p.FacebookUrl,
  p.InstagramUrl,
  p.LinkedInUrl,
  p.XUrl,
  p.YouTubeUrl,
  p.TikTokUrl,
  p.PinterestUrl,
  p.BlueskyUrl,
  p.OpeningDate,
  p.SearchLocationName,
  p.QuestionAnswerCount,
  p.Lat,
  p.Lng,
  p.Description,
  p.PhotoCount,
  p.OtherCategoriesJson,
  p.PlaceTopicsJson,
  p.IsServiceAreaBusiness,
  p.BusinessStatus,
  p.RegularOpeningHoursJson,
  p.ZohoLeadCreated,
  p.ZohoLeadCreatedAtUtc,
  p.ZohoLeadId,
  p.ZohoLastSyncAtUtc,
  p.ZohoLastError,
  pf.DateOfCreation AS FinancialDateOfCreation,
  pf.CompanyNumber AS FinancialCompanyNumber,
  pf.CompanyType AS FinancialCompanyType,
  pf.LastAccountsFiled AS FinancialLastAccountsFiled,
  pf.NextAccountsDue AS FinancialNextAccountsDue,
  pf.CompanyStatus AS FinancialCompanyStatus,
  pf.HasLiquidated AS FinancialHasLiquidated,
  pf.HasCharges AS FinancialHasCharges,
  pf.HasInsolvencyHistory AS FinancialHasInsolvencyHistory
FROM dbo.Place p
LEFT JOIN dbo.PlacesFinancial pf ON pf.PlaceId = p.PlaceId
WHERE p.PlaceId=@PlaceId", new { PlaceId = placeId }, cancellationToken: ct));

        if (place is null)
            return null;

        var liveDetails = await google.GetPlaceDetailsAsync(placeId, ct);

        PlaceSnapshotContextRow? active = null;
        if (runId.HasValue)
        {
            active = await conn.QuerySingleOrDefaultAsync<PlaceSnapshotContextRow>(new CommandDefinition(@"
SELECT TOP 1 SearchRunId, RankPosition, Rating, UserRatingCount, CapturedAtUtc
FROM dbo.PlaceSnapshot
WHERE PlaceId=@PlaceId AND SearchRunId=@RunId
ORDER BY CapturedAtUtc DESC", new { PlaceId = placeId, RunId = runId.Value }, cancellationToken: ct));
        }

        active ??= await conn.QuerySingleOrDefaultAsync<PlaceSnapshotContextRow>(new CommandDefinition(@"
SELECT TOP 1 SearchRunId, RankPosition, Rating, UserRatingCount, CapturedAtUtc
FROM dbo.PlaceSnapshot
WHERE PlaceId=@PlaceId
ORDER BY CapturedAtUtc DESC", new { PlaceId = placeId }, cancellationToken: ct));

        var mapRunId = runId ?? active?.SearchRunId;
        SearchRunCenterRow? runCenter = null;
        if (mapRunId.HasValue)
        {
            runCenter = await conn.QuerySingleOrDefaultAsync<SearchRunCenterRow>(new CommandDefinition(@"
SELECT TOP 1
  r.SearchRunId,
  t.Latitude AS CenterLat,
  t.Longitude AS CenterLng
FROM dbo.SearchRun r
JOIN dbo.GbTown t ON t.TownId = r.TownId
WHERE r.SearchRunId=@SearchRunId;",
                new { SearchRunId = mapRunId.Value }, cancellationToken: ct));
        }

        var contextRunId = runId ?? active?.SearchRunId;
        SearchRunContextRow? runContext = null;
        if (contextRunId.HasValue)
        {
            runContext = await conn.QuerySingleOrDefaultAsync<SearchRunContextRow>(new CommandDefinition(@"
SELECT TOP 1
  r.SearchRunId,
  c.DisplayName AS SeedKeyword,
  CONCAT(t.Name, N', ', county.Name) AS LocationName
FROM dbo.SearchRun r
JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
JOIN dbo.GbTown t ON t.TownId = r.TownId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE r.SearchRunId=@SearchRunId;",
                new { SearchRunId = contextRunId.Value }, cancellationToken: ct));
        }

        var settings = await adminSettingsService.GetAsync(ct);
        var latestTaskRows = (await conn.QueryAsync<LatestTaskStatusRow>(new CommandDefinition(@"
WITH cte AS (
  SELECT
    DataForSeoReviewTaskId,
    COALESCE(TaskType, 'reviews') AS TaskType,
    Status,
    CreatedAtUtc,
    ROW_NUMBER() OVER(PARTITION BY COALESCE(TaskType, 'reviews') ORDER BY CreatedAtUtc DESC, DataForSeoReviewTaskId DESC) AS rn
  FROM dbo.DataForSeoReviewTask
  WHERE PlaceId=@PlaceId
)
SELECT
  DataForSeoReviewTaskId,
  TaskType,
  Status,
  CreatedAtUtc
FROM cte
WHERE rn = 1;", new { PlaceId = placeId }, cancellationToken: ct))).ToList();
        var taskStatuses = BuildPlaceTaskStatuses(latestTaskRows, settings);

        var history = (await conn.QueryAsync<PlaceHistoryRow>(new CommandDefinition(@"
SELECT TOP 20
  s.SearchRunId,
  s.RankPosition,
  s.Rating,
  s.UserRatingCount,
  s.CapturedAtUtc,
  c.DisplayName AS SeedKeyword,
  CONCAT(t.Name, N', ', county.Name) AS LocationName
FROM dbo.PlaceSnapshot s
LEFT JOIN dbo.SearchRun r ON r.SearchRunId = s.SearchRunId
LEFT JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
LEFT JOIN dbo.GbTown t ON t.TownId = r.TownId
LEFT JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE s.PlaceId=@PlaceId
ORDER BY s.CapturedAtUtc DESC", new { PlaceId = placeId }, cancellationToken: ct))).ToList();

        var totalReviewCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.PlaceReview
WHERE PlaceId=@PlaceId;", new { PlaceId = placeId }, cancellationToken: ct));
        var totalReviewPages = Math.Max(1, (int)Math.Ceiling(totalReviewCount / (double)effectiveReviewPageSize));
        if (effectiveReviewPage > totalReviewPages)
            effectiveReviewPage = totalReviewPages;
        var reviewOffset = (effectiveReviewPage - 1) * effectiveReviewPageSize;

        var reviews = (await conn.QueryAsync<PlaceReviewRow>(new CommandDefinition(@"
SELECT
  ReviewId,
  ProfileName,
  Rating,
  ReviewText,
  ReviewTimestampUtc,
  TimeAgo,
  OwnerAnswer,
  OwnerTimestampUtc,
  ReviewUrl,
  LastSeenUtc
FROM dbo.PlaceReview
WHERE PlaceId=@PlaceId
ORDER BY COALESCE(ReviewTimestampUtc, LastSeenUtc) DESC, PlaceReviewId DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;", new
        {
            PlaceId = placeId,
            Offset = reviewOffset,
            PageSize = effectiveReviewPageSize
        }, cancellationToken: ct))).ToList();

        var updates = (await conn.QueryAsync<PlaceUpdateDataRow>(new CommandDefinition(@"
SELECT TOP 200
  UpdateKey,
  PostText,
  Url,
  ImagesUrlJson,
  PostDateUtc,
  LinksJson,
  FirstSeenUtc,
  LastSeenUtc
FROM dbo.PlaceUpdate
WHERE PlaceId=@PlaceId
ORDER BY COALESCE(PostDateUtc, FirstSeenUtc) DESC, PlaceUpdateId DESC", new { PlaceId = placeId }, cancellationToken: ct)))
            .Select(x => new PlaceUpdateRow(
                x.UpdateKey,
                x.PostText,
                x.Url,
                ParseJsonStringArray(x.ImagesUrlJson),
                x.PostDateUtc,
                ParseUpdateLinks(x.LinksJson),
                x.PostDateUtc ?? x.FirstSeenUtc))
            .ToList();

        var questionsAndAnswers = (await conn.QueryAsync<PlaceQuestionAnswerDataRow>(new CommandDefinition(@"
SELECT TOP 500
  QaKey,
  QuestionText,
  QuestionTimestampUtc,
  QuestionProfileName,
  AnswerText,
  AnswerTimestampUtc,
  AnswerProfileName,
  LastSeenUtc
FROM dbo.PlaceQuestionAnswer
WHERE PlaceId=@PlaceId
ORDER BY COALESCE(AnswerTimestampUtc, QuestionTimestampUtc, LastSeenUtc) DESC, PlaceQuestionAnswerId DESC;",
            new { PlaceId = placeId }, cancellationToken: ct)))
            .Select(x => new PlaceQuestionAnswerRow(
                x.QaKey,
                x.QuestionText,
                x.QuestionTimestampUtc,
                x.QuestionProfileName,
                x.AnswerText,
                x.AnswerTimestampUtc,
                x.AnswerProfileName,
                x.LastSeenUtc))
            .ToList();

        var financialOfficers = (await conn.QueryAsync<PlaceFinancialOfficerRow>(new CommandDefinition(@"
SELECT
  Id,
  PlaceId,
  FirstNames,
  LastName,
  CountryOfResidence,
  DateOfBirth,
  Nationality,
  Role,
  Appointed,
  Resigned
FROM dbo.PlacesFinancialOfficers
WHERE PlaceId = @PlaceId;", new { PlaceId = placeId }, cancellationToken: ct)))
            .Select(x => new PlaceFinancialOfficerInfo(
                x.Id,
                x.PlaceId,
                x.FirstNames,
                x.LastName,
                x.CountryOfResidence,
                x.DateOfBirth,
                x.Nationality,
                x.Role,
                x.Appointed,
                x.Resigned))
            .OrderBy(x => x.Resigned.HasValue ? 1 : 0)
            .ThenBy(x => x.Resigned.HasValue ? DateTime.MaxValue : (x.Appointed ?? DateTime.MaxValue))
            .ThenByDescending(x => x.Resigned ?? DateTime.MinValue)
            .ThenBy(x => x.Id)
            .ToList();

        var financialPscRows = (await conn.QueryAsync<PlaceFinancialPscRow>(new CommandDefinition(@"
SELECT
  Id,
  PlaceId,
  CompanyNumber,
  PscItemKind,
  PscLinkSelf,
  PscId,
  NameRaw,
  FirstNames,
  LastName,
  CountryOfResidence,
  Nationality,
  BirthMonth,
  BirthYear,
  NotifiedOn,
  CeasedOn,
  SourceEtag,
  RetrievedUtc,
  RawJson
FROM dbo.PlaceFinancialPersonsOfSignificantControl
WHERE PlaceId = @PlaceId;", new { PlaceId = placeId }, cancellationToken: ct))).ToList();

        IReadOnlyList<PlaceFinancialPscNatureRow> financialPscNatureRows = [];
        if (financialPscRows.Count > 0)
        {
            var pscIds = financialPscRows.Select(x => x.Id).ToArray();
            financialPscNatureRows = (await conn.QueryAsync<PlaceFinancialPscNatureRow>(new CommandDefinition(@"
SELECT
  PSCId,
  NatureCode
FROM dbo.PlaceFinancialPSC_NatureOfControl
WHERE PSCId IN @PSCIds;", new { PSCIds = pscIds }, cancellationToken: ct))).ToList();
        }

        var natureCodesByPscId = financialPscNatureRows
            .GroupBy(x => x.PSCId)
            .ToDictionary(
                g => g.Key,
                g => (IReadOnlyList<string>)g
                    .Select(x => x.NatureCode)
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList());

        var financialPscs = financialPscRows
            .Select(x =>
            {
                var natureCodes = natureCodesByPscId.TryGetValue(x.Id, out var values)
                    ? values
                    : [];

                return new PlaceFinancialPersonOfSignificantControlInfo(
                    x.Id,
                    x.PlaceId,
                    x.CompanyNumber,
                    x.PscItemKind,
                    x.PscLinkSelf,
                    x.PscId,
                    x.NameRaw,
                    x.FirstNames,
                    x.LastName,
                    x.CountryOfResidence,
                    x.Nationality,
                    x.BirthMonth,
                    x.BirthYear,
                    x.NotifiedOn,
                    x.CeasedOn,
                    x.SourceEtag,
                    x.RetrievedUtc,
                    x.RawJson,
                    natureCodes,
                    BuildOwnershipRightsDisplay(natureCodes),
                    BuildVotingRightsDisplay(natureCodes),
                    HasNatureCode(natureCodes, "right-to-appoint-and-remove-directors"),
                    HasNatureCode(natureCodes, "significant-influence-or-control"),
                    NatureCodesContain(natureCodes, "as-trust"),
                    NatureCodesContain(natureCodes, "as-firm"),
                    BuildLlpRightsDisplay(natureCodes));
            })
            .OrderBy(x => x.CeasedOn.HasValue ? 1 : 0)
            .ThenBy(x => x.CeasedOn ?? DateTime.MaxValue)
            .ThenBy(x => x.LastName)
            .ThenBy(x => x.FirstNames)
            .ToList();

        var matchedFinancialData = ApplyFinancialOfficerPscMatches(financialOfficers, financialPscs);
        financialOfficers = matchedFinancialData.Officers.ToList();
        financialPscs = matchedFinancialData.Pscs.ToList();

        var financialAccounts = (await conn.QueryAsync<PlaceFinancialAccountRow>(new CommandDefinition(@"
SELECT
  Id,
  PlaceId,
  CompanyNumber,
  TransactionId,
  FilingDate,
  MadeUpDate,
  AccountsType,
  DocumentId,
  DocumentMetadataUrl,
  ContentType,
  OriginalFileName,
  LocalRelativePath,
  FileSizeBytes,
  RetrievedUtc,
  IsLatest,
  RawJson
FROM dbo.PlaceFinancialAccounts
WHERE PlaceId = @PlaceId
ORDER BY
  CASE WHEN MadeUpDate IS NULL THEN 1 ELSE 0 END,
  MadeUpDate DESC,
  CASE WHEN FilingDate IS NULL THEN 1 ELSE 0 END,
  FilingDate DESC,
  RetrievedUtc DESC,
  Id DESC;", new { PlaceId = placeId }, cancellationToken: ct)))
            .Select(x => new PlaceFinancialAccountInfo(
                x.Id,
                x.PlaceId,
                x.CompanyNumber,
                x.TransactionId,
                x.FilingDate,
                x.MadeUpDate,
                x.AccountsType,
                x.DocumentId,
                x.DocumentMetadataUrl,
                x.ContentType,
                x.OriginalFileName,
                x.LocalRelativePath,
                x.FileSizeBytes,
                x.RetrievedUtc,
                x.IsLatest,
                x.RawJson))
            .ToList();

        var effectiveTypesCsv = ChooseBestTypesCsv(place.TypesCsv, liveDetails?.TypesCsv);
        var primaryType = SelectBestPrimaryType(
            PreferSpecificType(place.PrimaryType, liveDetails?.PrimaryType),
            effectiveTypesCsv);
        var primaryCategory = SelectPrimaryCategoryLabel(
            PreferSpecificCategory(place.PrimaryCategory, liveDetails?.PrimaryCategory),
            primaryType);
        var regularOpeningHours = ParseJsonStringArray(place.RegularOpeningHoursJson);
        if (regularOpeningHours.Count == 0 && liveDetails is { RegularOpeningHours.Count: > 0 })
            regularOpeningHours = liveDetails.RegularOpeningHours;

        var otherCategories = ParseJsonStringArray(place.OtherCategoriesJson);
        var placeTopics = ParseJsonStringArray(place.PlaceTopicsJson);
        var estimatedTraffic = await BuildPlaceEstimatedTrafficSummaryAsync(conn, contextRunId, active?.RankPosition, settings, ct);

        return new PlaceDetailsViewModel
        {
            PlaceId = place.PlaceId,
            DisplayName = PreferNonEmpty(place.DisplayName, liveDetails?.DisplayName),
            LogoUrl = place.LogoUrl,
            MainPhotoUrl = place.MainPhotoUrl,
            ContextSeedKeyword = runContext?.SeedKeyword,
            ContextLocationName = runContext?.LocationName,
            FormattedAddress = PreferNonEmpty(place.FormattedAddress, liveDetails?.FormattedAddress),
            PrimaryType = primaryType,
            PrimaryCategory = primaryCategory,
            NationalPhoneNumber = PreferNonEmpty(place.NationalPhoneNumber, liveDetails?.NationalPhoneNumber),
            WebsiteUri = PreferNonEmpty(place.WebsiteUri, liveDetails?.WebsiteUri),
            FacebookUrl = place.FacebookUrl,
            InstagramUrl = place.InstagramUrl,
            LinkedInUrl = place.LinkedInUrl,
            XUrl = place.XUrl,
            YouTubeUrl = place.YouTubeUrl,
            TikTokUrl = place.TikTokUrl,
            PinterestUrl = place.PinterestUrl,
            BlueskyUrl = place.BlueskyUrl,
            OpeningDate = place.OpeningDate,
            SearchLocationName = place.SearchLocationName,
            QuestionAnswerCount = place.QuestionAnswerCount,
            Lat = place.Lat ?? liveDetails?.Lat,
            Lng = place.Lng ?? liveDetails?.Lng,
            Description = place.Description,
            PhotoCount = place.PhotoCount,
            IsServiceAreaBusiness = place.IsServiceAreaBusiness ?? liveDetails?.IsServiceAreaBusiness,
            BusinessStatus = HumanizeType(PreferNonEmpty(place.BusinessStatus, liveDetails?.BusinessStatus)),
            RegularOpeningHours = regularOpeningHours,
            OtherCategories = otherCategories,
            PlaceTopics = placeTopics,
            ActiveRunId = active?.SearchRunId,
            ActiveRankPosition = active?.RankPosition,
            ActiveRating = active?.Rating,
            ActiveUserRatingCount = active?.UserRatingCount,
            ActiveCapturedAtUtc = active?.CapturedAtUtc,
            MapRunId = runCenter?.SearchRunId,
            RunCenterLat = runCenter?.CenterLat,
            RunCenterLng = runCenter?.CenterLng,
            Reviews = reviews,
            ReviewPage = effectiveReviewPage,
            ReviewPageSize = effectiveReviewPageSize,
            TotalReviewCount = totalReviewCount,
            TotalReviewPages = totalReviewPages,
            Updates = updates,
            QuestionsAndAnswers = questionsAndAnswers,
            History = history,
            DataTaskStatuses = taskStatuses,
            FinancialOfficers = financialOfficers,
            FinancialPersonsOfSignificantControl = financialPscs,
            FinancialAccounts = financialAccounts,
            ReviewVelocity = await reviewVelocityService.GetPlaceReviewVelocityAsync(placeId, ct),
            UpdateVelocity = await reviewVelocityService.GetPlaceUpdateVelocityAsync(placeId, ct),
            EstimatedTraffic = estimatedTraffic,
            FinancialInfo = string.IsNullOrWhiteSpace(place.FinancialCompanyNumber)
                ? null
                : new PlaceFinancialInfo(
                    place.PlaceId,
                    place.FinancialDateOfCreation,
                    place.FinancialCompanyNumber!,
                    place.FinancialCompanyType,
                    place.FinancialLastAccountsFiled,
                    place.FinancialNextAccountsDue,
                    place.FinancialCompanyStatus,
                    place.FinancialHasLiquidated ?? false,
                    place.FinancialHasCharges ?? false,
                    place.FinancialHasInsolvencyHistory ?? false),
            ZohoLeadCreated = place.ZohoLeadCreated,
            ZohoLeadCreatedAtUtc = place.ZohoLeadCreatedAtUtc,
            ZohoLeadId = place.ZohoLeadId,
            ZohoLastSyncAtUtc = place.ZohoLastSyncAtUtc,
            ZohoLastError = place.ZohoLastError
        };
    }

    private async Task<RunKeyphraseTrafficSummary?> GetRunKeyphraseTrafficSummaryCoreAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        long runId,
        CancellationToken ct)
    {
        var runContext = await conn.QuerySingleOrDefaultAsync<RunKeywordTrafficContextRow>(new CommandDefinition(@"
SELECT TOP 1
  CategoryId,
  TownId
FROM dbo.SearchRun
WHERE SearchRunId = @RunId;", new { RunId = runId }, cancellationToken: ct));
        if (runContext is null)
            return null;

        var aggregate = await BuildKeywordTrafficAggregateAsync(conn, runContext.CategoryId, runContext.TownId, ct);
        if (aggregate is null)
            return null;

        return new RunKeyphraseTrafficSummary(
            runContext.CategoryId,
            runContext.TownId,
            aggregate.Last12Months,
            aggregate.WeightedAvgSearchVolume,
            aggregate.WeightedLastMonthSearchVolume,
            aggregate.LatestMonthYear,
            aggregate.LatestMonthNumber,
            BuildAdminKeyphrasesUrl(runContext.TownId, runContext.CategoryId));
    }

    private async Task<PlaceEstimatedTrafficSummary?> BuildPlaceEstimatedTrafficSummaryAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        long? contextRunId,
        int? activeRankPosition,
        AdminSettingsModel settings,
        CancellationToken ct)
    {
        if (!contextRunId.HasValue || !activeRankPosition.HasValue || activeRankPosition.Value <= 0)
            return null;

        var runContext = await conn.QuerySingleOrDefaultAsync<RunKeywordTrafficContextRow>(new CommandDefinition(@"
SELECT TOP 1
  CategoryId,
  TownId
FROM dbo.SearchRun
WHERE SearchRunId = @RunId;", new { RunId = contextRunId.Value }, cancellationToken: ct));
        if (runContext is null)
            return null;

        var aggregate = await BuildKeywordTrafficAggregateAsync(conn, runContext.CategoryId, runContext.TownId, ct);
        if (aggregate is null)
            return null;

        var weightedAvgVolume = Math.Max(0, aggregate.WeightedAvgSearchVolume);
        var currentPosition = activeRankPosition.Value;
        var currentVisits = CalculateEstimatedMapPackClicks(currentPosition, weightedAvgVolume, settings);
        var visitsAt3 = CalculateEstimatedMapPackClicks(3, weightedAvgVolume, settings);
        var visitsAt1 = CalculateEstimatedMapPackClicks(1, weightedAvgVolume, settings);

        int? opportunityTo3 = null;
        if (currentPosition > 3)
            opportunityTo3 = Math.Max(0, visitsAt3 - currentVisits);

        int? opportunityTo1 = null;
        if (currentPosition > 1)
            opportunityTo1 = Math.Max(0, visitsAt1 - currentVisits);

        return new PlaceEstimatedTrafficSummary(
            currentPosition,
            currentVisits,
            visitsAt3,
            visitsAt1,
            opportunityTo3,
            opportunityTo1);
    }

    private async Task<KeywordTrafficAggregate?> BuildKeywordTrafficAggregateAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        string categoryId,
        long townId,
        CancellationToken ct)
    {
        var keywordCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @TownId;", new
        {
            CategoryId = categoryId,
            TownId = townId
        }, cancellationToken: ct));
        if (keywordCount <= 0)
            return null;

        var monthly = (await conn.QueryAsync<WeightedMonthRow>(new CommandDefinition(@"
SELECT TOP 12
  m.[Year],
  m.[Month],
  CAST(SUM(
    CAST(m.SearchVolume AS decimal(18, 4)) *
    CASE
      WHEN k.KeywordType = 1 THEN 1.0
      WHEN k.KeywordType IN (3, 4) THEN 0.7
      ELSE 0.0
    END
  ) AS decimal(18, 4)) AS WeightedSearchVolume
FROM dbo.CategoryLocationSearchVolume m
JOIN dbo.CategoryLocationKeyword k ON k.Id = m.CategoryLocationKeywordId
WHERE k.CategoryId = @CategoryId
  AND k.LocationId = @TownId
  AND k.NoData = 0
  AND k.KeywordType IN (1, 3, 4)
GROUP BY m.[Year], m.[Month]
ORDER BY m.[Year] DESC, m.[Month] DESC;", new
        {
            CategoryId = categoryId,
            TownId = townId
        }, cancellationToken: ct))).ToList();

        var last12Months = monthly
            .OrderBy(x => x.Year)
            .ThenBy(x => x.Month)
            .Select(x => new WeightedSearchVolumePoint(
                x.Year,
                x.Month,
                decimal.Round(x.WeightedSearchVolume, 2, MidpointRounding.AwayFromZero)))
            .ToList();

        var weightedAvg = last12Months.Count == 0
            ? 0
            : (int)Math.Round(last12Months.Average(x => x.WeightedSearchVolume), MidpointRounding.AwayFromZero);
        var latest = monthly
            .OrderByDescending(x => x.Year)
            .ThenByDescending(x => x.Month)
            .FirstOrDefault();
        var weightedLastMonth = latest is null
            ? (int?)null
            : (int)Math.Round(latest.WeightedSearchVolume, MidpointRounding.AwayFromZero);

        return new KeywordTrafficAggregate(
            last12Months,
            Math.Max(0, weightedAvg),
            weightedLastMonth.HasValue ? Math.Max(0, weightedLastMonth.Value) : null,
            latest?.Year,
            latest?.Month);
    }

    private static int CalculateEstimatedMapPackClicks(int rankPosition, int baselineSearchVolume, AdminSettingsModel settings)
    {
        if (rankPosition < 1 || rankPosition > 10 || baselineSearchVolume <= 0)
            return 0;

        var mapPackShareRatio = Math.Clamp(settings.MapPackClickSharePercent, 0, 100) / 100m;
        if (mapPackShareRatio <= 0m)
            return 0;

        var positionCtrRatio = rankPosition switch
        {
            1 => Math.Clamp(settings.MapPackCtrPosition1Percent, 0, 100) / 100m,
            2 => Math.Clamp(settings.MapPackCtrPosition2Percent, 0, 100) / 100m,
            3 => Math.Clamp(settings.MapPackCtrPosition3Percent, 0, 100) / 100m,
            4 => Math.Clamp(settings.MapPackCtrPosition4Percent, 0, 100) / 100m,
            5 => Math.Clamp(settings.MapPackCtrPosition5Percent, 0, 100) / 100m,
            6 => Math.Clamp(settings.MapPackCtrPosition6Percent, 0, 100) / 100m,
            7 => Math.Clamp(settings.MapPackCtrPosition7Percent, 0, 100) / 100m,
            8 => Math.Clamp(settings.MapPackCtrPosition8Percent, 0, 100) / 100m,
            9 => Math.Clamp(settings.MapPackCtrPosition9Percent, 0, 100) / 100m,
            10 => Math.Clamp(settings.MapPackCtrPosition10Percent, 0, 100) / 100m,
            _ => 0m
        };
        if (positionCtrRatio <= 0m)
            return 0;

        var clicks = baselineSearchVolume * mapPackShareRatio * positionCtrRatio;
        return Math.Max(0, (int)Math.Round(clicks, MidpointRounding.AwayFromZero));
    }

    private static string BuildAdminKeyphrasesUrl(long locationId, string categoryId)
    {
        var encodedCategoryId = Convert.ToBase64String(Encoding.UTF8.GetBytes(categoryId))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        return $"/admin/location/{locationId}/category/{encodedCategoryId}/keyphrases";
    }

    private static IReadOnlyList<PlaceDataTaskStatusRow> BuildPlaceTaskStatuses(
        IReadOnlyList<LatestTaskStatusRow> latestTaskRows,
        AdminSettingsModel settings)
    {
        var nowUtc = DateTime.UtcNow;

        PlaceDataTaskStatusRow Build(string taskType, string label, int refreshThresholdHours)
        {
            var row = latestTaskRows.FirstOrDefault(x => string.Equals(x.TaskType, taskType, StringComparison.OrdinalIgnoreCase));
            if (row is null)
            {
                return new PlaceDataTaskStatusRow(
                    taskType,
                    label,
                    "Never",
                    null,
                    null,
                    "Never run",
                    false,
                    true,
                    refreshThresholdHours);
            }

            var lastRunAtUtc = row.CreatedAtUtc;
            var ageLabel = FormatElapsedLabel(nowUtc - lastRunAtUtc);
            var canRefresh = (nowUtc - lastRunAtUtc) >= TimeSpan.FromHours(Math.Max(0, refreshThresholdHours));
            var status = string.IsNullOrWhiteSpace(row.Status) ? "Unknown" : row.Status;

            return new PlaceDataTaskStatusRow(
                taskType,
                label,
                status,
                row.DataForSeoReviewTaskId,
                lastRunAtUtc,
                ageLabel,
                string.Equals(status, "Ready", StringComparison.OrdinalIgnoreCase),
                canRefresh,
                refreshThresholdHours);
        }

        return new List<PlaceDataTaskStatusRow>
        {
            Build("my_business_info", "Google Enhanced Data collection", settings.EnhancedGoogleDataRefreshHours),
            Build("reviews", "Google Review collection", settings.GoogleReviewsRefreshHours),
            Build("my_business_updates", "Google Updates collection", settings.GoogleUpdatesRefreshHours),
            Build("questions_and_answers", "Google Question & Answers collection", settings.GoogleQuestionsAndAnswersRefreshHours),
            Build("social_profiles", "Google Social Profile collection", settings.GoogleSocialProfilesRefreshHours)
        };
    }

    private static string FormatElapsedLabel(TimeSpan elapsed)
    {
        if (elapsed < TimeSpan.Zero)
            elapsed = TimeSpan.Zero;

        var days = (int)Math.Floor(elapsed.TotalDays);
        var hours = elapsed.Hours;
        if (days <= 0 && hours <= 0)
            return "less than 1 hour ago";
        if (days <= 0)
            return $"{hours}h ago";
        return $"{days}d {hours}h ago";
    }

    private static string BuildOwnershipRightsDisplay(IReadOnlyList<string> natureCodes)
        => BuildBracketedRightsDisplay(natureCodes, code => code.StartsWith("ownership-of-shares-", StringComparison.OrdinalIgnoreCase));

    private static string BuildVotingRightsDisplay(IReadOnlyList<string> natureCodes)
        => BuildBracketedRightsDisplay(
            natureCodes,
            code => code.StartsWith("voting-rights-", StringComparison.OrdinalIgnoreCase)
                && code.IndexOf("as-llp-member", StringComparison.OrdinalIgnoreCase) < 0);

    private static string BuildBracketedRightsDisplay(IReadOnlyList<string> natureCodes, Func<string, bool> include)
    {
        if (natureCodes.Count == 0)
            return "N/A";

        var ranges = natureCodes
            .Where(include)
            .Select(MapNatureCodeToBracketRange)
            .Where(x => x is not null)
            .Cast<string>()
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (ranges.Count == 0)
            return "N/A";
        return string.Join(", ", ranges);
    }

    private static bool HasNatureCode(IReadOnlyList<string> natureCodes, string targetCode)
    {
        return natureCodes.Any(code => string.Equals(code, targetCode, StringComparison.OrdinalIgnoreCase));
    }

    private static bool NatureCodesContain(IReadOnlyList<string> natureCodes, string fragment)
    {
        return natureCodes.Any(code => code.IndexOf(fragment, StringComparison.OrdinalIgnoreCase) >= 0);
    }

    private static string BuildLlpRightsDisplay(IReadOnlyList<string> natureCodes)
    {
        if (natureCodes.Count == 0)
            return "N/A";

        var matches = natureCodes
            .Where(code =>
                code.IndexOf("as-llp-member", StringComparison.OrdinalIgnoreCase) >= 0
                || code.IndexOf("right-to-appoint-and-remove-members", StringComparison.OrdinalIgnoreCase) >= 0
                || code.StartsWith("right-to-share-surplus-assets-", StringComparison.OrdinalIgnoreCase))
            .Select(HumanizeNatureCode)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (matches.Count == 0)
            return "N/A";
        return string.Join(", ", matches);
    }

    private static string? MapNatureCodeToBracketRange(string code)
    {
        var normalized = NormalizeKey(code).Replace('_', '-');
        if (normalized.Contains("25-to-50-percent", StringComparison.Ordinal))
            return "25%-50%";
        if (normalized.Contains("50-to-75-percent", StringComparison.Ordinal))
            return "50%-75%";
        if (normalized.Contains("75-to-100-percent", StringComparison.Ordinal))
            return "75%-100%";

        if (normalized.Contains("more-than-25-percent-but-not-more-than-50-percent", StringComparison.Ordinal))
            return "25%-50%";
        if (normalized.Contains("more-than-50-percent-but-less-than-75-percent", StringComparison.Ordinal))
            return "50%-75%";
        if (normalized.Contains("75-percent-or-more", StringComparison.Ordinal))
            return "75%-100%";

        return null;
    }

    private static string HumanizeNatureCode(string code)
    {
        var tokens = code
            .Split(['-', '_', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(token =>
            {
                if (string.Equals(token, "llp", StringComparison.OrdinalIgnoreCase))
                    return "LLP";
                if (token.All(char.IsDigit))
                    return token;
                return char.ToUpperInvariant(token[0]) + token[1..].ToLowerInvariant();
            });
        return string.Join(' ', tokens);
    }

    private static (IReadOnlyList<PlaceFinancialOfficerInfo> Officers, IReadOnlyList<PlaceFinancialPersonOfSignificantControlInfo> Pscs)
        ApplyFinancialOfficerPscMatches(
            IReadOnlyList<PlaceFinancialOfficerInfo> officers,
            IReadOnlyList<PlaceFinancialPersonOfSignificantControlInfo> pscs)
    {
        if (officers.Count == 0 || pscs.Count == 0)
            return (officers, pscs);

        var matchedOfficerIndices = new HashSet<int>();
        var matchedPscIndices = new HashSet<int>();

        var officerKeys = officers
            .Select(BuildOfficerMatchKeys)
            .ToList();
        var pscKeys = pscs
            .Select(BuildPscMatchKeys)
            .ToList();

        for (var pscIndex = 0; pscIndex < pscs.Count; pscIndex++)
        {
            if (pscs[pscIndex].CeasedOn.HasValue)
                continue;

            var pscCandidates = pscKeys[pscIndex];
            if (pscCandidates.Count == 0)
                continue;

            for (var officerIndex = 0; officerIndex < officers.Count; officerIndex++)
            {
                if (officers[officerIndex].Resigned.HasValue)
                    continue;

                var officerCandidates = officerKeys[officerIndex];
                if (officerCandidates.Count == 0)
                    continue;

                if (!HasFinancialMatch(officerCandidates, pscCandidates))
                    continue;

                matchedPscIndices.Add(pscIndex);
                matchedOfficerIndices.Add(officerIndex);
            }
        }

        var updatedOfficers = officers
            .Select((officer, index) => officer with { IsPossiblePscMatch = matchedOfficerIndices.Contains(index) })
            .ToList();
        var updatedPscs = pscs
            .Select((psc, index) => psc with { IsPossibleOfficerMatch = matchedPscIndices.Contains(index) })
            .ToList();

        return (updatedOfficers, updatedPscs);
    }

    private static bool HasFinancialMatch(
        IReadOnlyList<FinancialMatchKey> officerCandidates,
        IReadOnlyList<FinancialMatchKey> pscCandidates)
    {
        foreach (var officerKey in officerCandidates)
        {
            foreach (var pscKey in pscCandidates)
            {
                if (!string.Equals(pscKey.LastName, officerKey.LastName, StringComparison.Ordinal))
                    continue;
                if (pscKey.BirthMonth != officerKey.BirthMonth || pscKey.BirthYear != officerKey.BirthYear)
                    continue;

                var exactFirstNameMatch = string.Equals(pscKey.FirstName, officerKey.FirstName, StringComparison.Ordinal);
                var initialMatch = pscKey.FirstInitial == officerKey.FirstInitial;
                if (!exactFirstNameMatch && !initialMatch)
                    continue;

                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<FinancialMatchKey> BuildOfficerMatchKeys(PlaceFinancialOfficerInfo officer)
    {
        if (!officer.DateOfBirth.HasValue)
            return [];

        return BuildFinancialMatchKeys(
            officer.FirstNames,
            officer.LastName,
            officer.DateOfBirth.Value.Month,
            officer.DateOfBirth.Value.Year);
    }

    private static IReadOnlyList<FinancialMatchKey> BuildPscMatchKeys(PlaceFinancialPersonOfSignificantControlInfo psc)
    {
        if (!psc.BirthMonth.HasValue || !psc.BirthYear.HasValue)
            return [];

        return BuildFinancialMatchKeys(
            psc.FirstNames,
            psc.LastName,
            psc.BirthMonth.Value,
            psc.BirthYear.Value);
    }

    private static IReadOnlyList<FinancialMatchKey> BuildFinancialMatchKeys(
        string? firstNamesValue,
        string? lastNameValue,
        int birthMonth,
        int birthYear)
    {
        var candidates = new List<FinancialMatchKey>();
        foreach (var pair in BuildNameCandidatePairs(firstNamesValue, lastNameValue))
        {
            var key = CreateFinancialMatchKey(pair.FirstName, pair.LastName, birthMonth, birthYear);
            if (key is null)
                continue;
            candidates.Add(key);
        }

        return candidates
            .Distinct()
            .ToList();
    }

    private static IReadOnlyList<(string? FirstName, string? LastName)> BuildNameCandidatePairs(
        string? firstNamesValue,
        string? lastNameValue)
    {
        var pairs = new List<(string? FirstName, string? LastName)>
        {
            (firstNamesValue, lastNameValue)
        };

        if (!string.IsNullOrWhiteSpace(firstNamesValue) && !string.IsNullOrWhiteSpace(lastNameValue))
        {
            var first = firstNamesValue.Trim();
            var last = lastNameValue.Trim();
            if (!string.Equals(first, last, StringComparison.OrdinalIgnoreCase))
                pairs.Add((lastNameValue, firstNamesValue));
        }

        return pairs;
    }

    private static FinancialMatchKey? CreateFinancialMatchKey(
        string? firstNamesValue,
        string? lastNameValue,
        int birthMonth,
        int birthYear)
    {
        if (birthMonth is < 1 or > 12 || birthYear is < 1 or > 9999)
            return null;

        var lastName = NormalizeNameForMatch(lastNameValue);
        var firstName = NormalizeNameForMatch(ExtractFirstNameToken(firstNamesValue));
        if (lastName is null || firstName is null)
            return null;

        return new FinancialMatchKey(lastName, firstName, firstName[0], birthMonth, birthYear);
    }

    private static string? ExtractFirstNameToken(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        var separators = new[] { ' ', ',', '.', '-', '/', '\\' };
        var token = trimmed
            .Split(separators, StringSplitOptions.RemoveEmptyEntries)
            .FirstOrDefault();
        return token;
    }

    private static string? NormalizeNameForMatch(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var chars = value
            .Trim()
            .Where(char.IsLetterOrDigit)
            .Select(char.ToUpperInvariant)
            .ToArray();
        if (chars.Length == 0)
            return null;

        return new string(chars);
    }

    private static string? NormalizeSocialUrl(string? value, string label)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return null;

        if (!Uri.TryCreate(normalized, UriKind.Absolute, out var uri) ||
            (!string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) &&
             !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"{label} must be a valid http/https URL.");
        }

        return uri.AbsoluteUri.TrimEnd('/');
    }

    private static string? NormalizeAndTrim(string? value, int maxLength)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return null;

        return normalized.Length <= maxLength
            ? normalized
            : normalized[..maxLength];
    }

    private static IReadOnlyList<string> SplitTypes(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv))
            return [];
        return csv
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string? ChooseBestTypesCsv(string? currentTypesCsv, string? liveTypesCsv)
    {
        var currentSpecific = CountSpecificTypes(currentTypesCsv);
        var liveSpecific = CountSpecificTypes(liveTypesCsv);
        if (liveSpecific > currentSpecific)
            return liveTypesCsv;
        return currentTypesCsv;
    }

    private static int CountSpecificTypes(string? typesCsv)
        => SplitTypes(typesCsv).Count(x => !IsGenericType(x));

    private static string? SelectBestPrimaryType(string? primaryType, string? typesCsv)
    {
        var candidates = SplitTypes(typesCsv).Where(x => !IsGenericType(x)).ToList();
        var nonServiceCandidates = candidates.Where(x => !HasServiceToken(x)).ToList();

        if (nonServiceCandidates.Count > 0)
            return nonServiceCandidates[0];
        if (!string.IsNullOrWhiteSpace(primaryType) && !IsGenericType(primaryType))
            return primaryType;
        if (candidates.Count > 0)
            return candidates[0];
        return primaryType;
    }

    private static string SelectPrimaryCategoryLabel(string? primaryCategory, string? primaryType)
    {
        if (!string.IsNullOrWhiteSpace(primaryCategory) && !IsGenericCategoryLabel(primaryCategory))
            return primaryCategory;
        return HumanizeType(primaryType) ?? "(unknown)";
    }

    private static bool IsGenericType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return false;

        var normalized = NormalizeKey(type);
        if (GenericTypes.Contains(normalized))
            return true;
        return HasServiceToken(normalized) && normalized != "web_designer";
    }

    private static bool IsGenericCategoryLabel(string? label)
    {
        if (string.IsNullOrWhiteSpace(label))
            return false;
        var normalized = NormalizeLabel(label);
        if (GenericCategoryLabels.Contains(normalized))
            return true;
        return normalized.Contains("service");
    }

    private static string? PreferNonEmpty(string? first, string? second)
        => !string.IsNullOrWhiteSpace(first) ? first : second;

    private static string? PreferLongerText(string? first, string? second)
    {
        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        return second.Length > first.Length ? second : first;
    }

    private static string? PreferSpecificType(string? first, string? second)
    {
        var firstGeneric = IsGenericType(first);
        var secondGeneric = IsGenericType(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static string? PreferSpecificCategory(string? first, string? second)
    {
        var firstGeneric = IsGenericCategoryLabel(first);
        var secondGeneric = IsGenericCategoryLabel(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static string? HumanizeType(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return string.Join(' ',
            value.Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries)
                .Select(token => char.ToUpperInvariant(token[0]) + token[1..].ToLowerInvariant()));
    }

    private static bool HasServiceToken(string value)
    {
        var normalized = NormalizeKey(value);
        return normalized.Contains("_service")
            || normalized.Contains("_services")
            || normalized == "service"
            || normalized == "services";
    }

    private static string NormalizeKey(string value)
        => value.Trim().ToLowerInvariant();

    private static string NormalizeLabel(string value)
        => value.Trim().ToLowerInvariant();

    private static IReadOnlyList<string> ParseJsonStringArray(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];
        try
        {
            var parsed = JsonSerializer.Deserialize<List<string>>(json);
            if (parsed is null)
                return [];
            return parsed.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private static IReadOnlyList<PlaceUpdateLinkRow> ParseUpdateLinks(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        try
        {
            var parsed = JsonSerializer.Deserialize<List<PlaceUpdateLinkRow>>(json);
            if (parsed is null)
                return [];

            return parsed
                .Where(x => !string.IsNullOrWhiteSpace(x.Type) || !string.IsNullOrWhiteSpace(x.Title) || !string.IsNullOrWhiteSpace(x.Url))
                .Select(x => new PlaceUpdateLinkRow(
                    string.IsNullOrWhiteSpace(x.Type) ? null : x.Type.Trim(),
                    string.IsNullOrWhiteSpace(x.Title) ? null : x.Title.Trim(),
                    string.IsNullOrWhiteSpace(x.Url) ? null : x.Url.Trim()))
                .ToList();
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private sealed record PlaceDetailsRow(
        string PlaceId,
        string? DisplayName,
        string? LogoUrl,
        string? MainPhotoUrl,
        string? FormattedAddress,
        string? PrimaryType,
        string? PrimaryCategory,
        string? TypesCsv,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        string? FacebookUrl,
        string? InstagramUrl,
        string? LinkedInUrl,
        string? XUrl,
        string? YouTubeUrl,
        string? TikTokUrl,
        string? PinterestUrl,
        string? BlueskyUrl,
        DateTime? OpeningDate,
        string? SearchLocationName,
        int? QuestionAnswerCount,
        decimal? Lat,
        decimal? Lng,
        string? Description,
        int? PhotoCount,
        string? OtherCategoriesJson,
        string? PlaceTopicsJson,
        bool? IsServiceAreaBusiness,
        string? BusinessStatus,
        string? RegularOpeningHoursJson,
        bool ZohoLeadCreated,
        DateTime? ZohoLeadCreatedAtUtc,
        string? ZohoLeadId,
        DateTime? ZohoLastSyncAtUtc,
        string? ZohoLastError,
        DateTime? FinancialDateOfCreation,
        string? FinancialCompanyNumber,
        string? FinancialCompanyType,
        DateTime? FinancialLastAccountsFiled,
        DateTime? FinancialNextAccountsDue,
        string? FinancialCompanyStatus,
        bool? FinancialHasLiquidated,
        bool? FinancialHasCharges,
        bool? FinancialHasInsolvencyHistory);

    private sealed record PlaceSocialLinksEditRow(
        string PlaceId,
        string? DisplayName,
        string? FacebookUrl,
        string? InstagramUrl,
        string? LinkedInUrl,
        string? XUrl,
        string? YouTubeUrl,
        string? TikTokUrl,
        string? PinterestUrl,
        string? BlueskyUrl);

    private sealed record PlaceFinancialRow(
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

    private sealed record PlaceFinancialOfficerRow(
        long Id,
        string PlaceId,
        string? FirstNames,
        string? LastName,
        string? CountryOfResidence,
        DateTime? DateOfBirth,
        string? Nationality,
        string? Role,
        DateTime? Appointed,
        DateTime? Resigned);

    private sealed record PlaceFinancialPscRow(
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
        string? RawJson);

    private sealed record PlaceFinancialPscNatureRow(
        long PSCId,
        string NatureCode);

    private sealed record PlaceFinancialAccountRow(
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

    private sealed record NormalizedPscUpsertRow(
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

    private sealed record FinancialMatchKey(
        string LastName,
        string FirstName,
        char FirstInitial,
        int BirthMonth,
        int BirthYear);

    private sealed record SearchRunCenterRow(long SearchRunId, decimal? CenterLat, decimal? CenterLng);

    private sealed record SearchRunContextRow(long SearchRunId, string SeedKeyword, string LocationName);

    private sealed record SearchRunSelectionRow(
        string CategoryId,
        string CategoryDisplayName,
        long TownId,
        long CountyId,
        string TownName,
        string CountyName,
        decimal? TownLat,
        decimal? TownLng);

    private sealed record RunKeywordTrafficContextRow(string CategoryId, long TownId);
    private sealed record WeightedMonthRow(int Year, int Month, decimal WeightedSearchVolume);
    private sealed record KeywordTrafficAggregate(
        IReadOnlyList<WeightedSearchVolumePoint> Last12Months,
        int WeightedAvgSearchVolume,
        int? WeightedLastMonthSearchVolume,
        int? LatestMonthYear,
        int? LatestMonthNumber);

    private sealed record PlaceSnapshotContextRow(long SearchRunId, int RankPosition, decimal? Rating, int? UserRatingCount, DateTime CapturedAtUtc);

    private sealed record LatestTaskStatusRow(long DataForSeoReviewTaskId, string TaskType, string Status, DateTime CreatedAtUtc);
    private sealed record LatestTaskRunByPlaceRow(string PlaceId, string TaskType, DateTime LastCreatedAtUtc);
    private sealed record RunTaskProgressStatusRow(string TaskType, int ProcessingCount, int CompletedCount, int ErrorCount, int DueCount);
    private sealed record RunReviewComparisonRowData(
        string PlaceId,
        int RankPosition,
        string DisplayName,
        decimal? AvgRating,
        int? UserRatingCount,
        int ReviewsLast90,
        int ReviewsLast180,
        int ReviewsLast270,
        int ReviewsLast365,
        decimal? AvgPerMonth12m,
        int? DaysSinceLastReview,
        int? LongestGapDays12m,
        decimal? RespondedPct12m,
        decimal? AvgOwnerResponseHours12m,
        string StatusLabel);
    private sealed record RunReviewMonthlyCountRow(string PlaceId, string DisplayName, int Year, int Month, int ReviewCount);

    private sealed record ReviewTaskRequest(string PlaceId, int? ReviewCount, string? LocationName, decimal? Lat, decimal? Lng);

    private sealed record PlaceUpdateDataRow(
        string UpdateKey,
        string? PostText,
        string? Url,
        string? ImagesUrlJson,
        DateTime? PostDateUtc,
        string? LinksJson,
        DateTime FirstSeenUtc,
        DateTime LastSeenUtc);

    private sealed record PlaceQuestionAnswerDataRow(
        string QaKey,
        string? QuestionText,
        DateTime? QuestionTimestampUtc,
        string? QuestionProfileName,
        string? AnswerText,
        DateTime? AnswerTimestampUtc,
        string? AnswerProfileName,
        DateTime LastSeenUtc);

    private static readonly HashSet<string> GenericTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "point_of_interest",
        "establishment",
        "service",
        "services",
        "professional_services",
        "business_to_business_service",
        "local_services"
    };

    private static readonly HashSet<string> GenericCategoryLabels = new(StringComparer.OrdinalIgnoreCase)
    {
        "services",
        "service",
        "business",
        "point of interest",
        "establishment",
        "professional services",
        "local services"
    };
}
