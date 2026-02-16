using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface ISearchIngestionService
{
    Task<long> RunAsync(SearchFormModel model, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct);
    Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct);
    Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct);
    Task<IReadOnlyList<RunTaskProgressRow>> GetRunTaskProgressAsync(SearchRun run, CancellationToken ct);
    Task<RunReviewComparisonViewModel?> GetRunReviewComparisonAsync(long runId, CancellationToken ct);
    Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct, int reviewPage = 1, int reviewPageSize = 25);
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
        decimal centerLat;
        decimal centerLng;
        string? canonicalLocationName = null;

        if (model.CenterLat.HasValue && model.CenterLng.HasValue)
        {
            centerLat = model.CenterLat.Value;
            centerLng = model.CenterLng.Value;
            // Even when coordinates are prefilled (rerun), resolve a canonical location
            // for downstream providers that validate location_name format.
            try
            {
                var canonicalCenter = await google.GeocodeAsync(model.LocationName, placesOptions.Value.GeocodeCountryCode, ct);
                canonicalLocationName = canonicalCenter?.CanonicalLocationName;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Geocode canonicalization failed for rerun location '{LocationName}'. Falling back to raw input.", model.LocationName);
            }
        }
        else
        {
            var center = await google.GeocodeAsync(model.LocationName, placesOptions.Value.GeocodeCountryCode, ct);
            if (center is null)
                throw new InvalidOperationException($"Could not determine coordinates for '{model.LocationName}'.");

            centerLat = center.Value.Lat;
            centerLng = center.Value.Lng;
            canonicalLocationName = center.Value.CanonicalLocationName;
        }

        var effectiveLocationName = !string.IsNullOrWhiteSpace(canonicalLocationName)
            ? canonicalLocationName
            : model.LocationName;
        var places = await google.SearchAsync(model.SeedKeyword, model.LocationName, centerLat, centerLng, model.RadiusMeters, model.ResultLimit, ct);

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

        var runId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SearchRun(SeedKeyword,LocationName,CenterLat,CenterLng,RadiusMeters,ResultLimit,FetchDetailedData,FetchGoogleReviews,FetchGoogleUpdates,FetchGoogleQuestionsAndAnswers)
OUTPUT INSERTED.SearchRunId
VALUES(@SeedKeyword,@LocationName,@CenterLat,@CenterLng,@RadiusMeters,@ResultLimit,@FetchDetailedData,@FetchGoogleReviews,@FetchGoogleUpdates,@FetchGoogleQuestionsAndAnswers)",
            new
            {
                model.SeedKeyword,
                model.LocationName,
                CenterLat = centerLat,
                CenterLng = centerLng,
                model.RadiusMeters,
                model.ResultLimit,
                FetchDetailedData = model.FetchEnhancedGoogleData,
                FetchGoogleReviews = model.FetchGoogleReviews,
                FetchGoogleUpdates = model.FetchGoogleUpdates,
                FetchGoogleQuestionsAndAnswers = model.FetchGoogleQuestionsAndAnswers
            }, tx, cancellationToken: ct));

        var requestGoogleReviews = model.FetchGoogleReviews;
        var requestMyBusinessInfo = model.FetchEnhancedGoogleData;
        var requestGoogleUpdates = model.FetchGoogleUpdates;
        var requestGoogleQuestionsAndAnswers = model.FetchGoogleQuestionsAndAnswers;
        var shouldFetchAnyDataForSeo = requestGoogleReviews || requestMyBusinessInfo || requestGoogleUpdates || requestGoogleQuestionsAndAnswers;

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
            var enhancedHours = Math.Max(1, settings.EnhancedGoogleDataRefreshHours);
            var reviewsHours = Math.Max(1, settings.GoogleReviewsRefreshHours);
            var updatesHours = Math.Max(1, settings.GoogleUpdatesRefreshHours);
            var qasHours = Math.Max(1, settings.GoogleQuestionsAndAnswersRefreshHours);
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
                "Detailed data requested. Provider {ProviderName}. Creating DataForSEO tasks for {PlaceCount} places. Reviews={Reviews}, MyBusinessInfo={MyBusinessInfo}, Updates={Updates}, QAs={QAs}.",
                providerName,
                reviewRequests.Count,
                requestGoogleReviews,
                requestMyBusinessInfo,
                requestGoogleUpdates,
                requestGoogleQuestionsAndAnswers);
            foreach (var reviewRequest in reviewRequests)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var reviewsDue = requestGoogleReviews && IsTaskDue(reviewRequest.PlaceId, "reviews", reviewsHours, nowUtc, latestRunsByKey);
                    var infoDue = requestMyBusinessInfo && IsTaskDue(reviewRequest.PlaceId, "my_business_info", enhancedHours, nowUtc, latestRunsByKey);
                    var updatesDue = requestGoogleUpdates && IsTaskDue(reviewRequest.PlaceId, "my_business_updates", updatesHours, nowUtc, latestRunsByKey);
                    var qasDue = requestGoogleQuestionsAndAnswers && IsTaskDue(reviewRequest.PlaceId, "questions_and_answers", qasHours, nowUtc, latestRunsByKey);

                    if (!reviewsDue && requestGoogleReviews)
                        logger.LogInformation("Skipping reviews task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, reviewsHours);
                    if (!infoDue && requestMyBusinessInfo)
                        logger.LogInformation("Skipping my_business_info task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, enhancedHours);
                    if (!updatesDue && requestGoogleUpdates)
                        logger.LogInformation("Skipping my_business_updates task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, updatesHours);
                    if (!qasDue && requestGoogleQuestionsAndAnswers)
                        logger.LogInformation("Skipping questions_and_answers task for place {PlaceId}; last run is within {Hours}h window.", reviewRequest.PlaceId, qasHours);

                    if (!reviewsDue && !infoDue && !updatesDue && !qasDue)
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

        return (nowUtc - lastRunUtc) >= TimeSpan.FromHours(Math.Max(1, thresholdHours));
    }

    public async Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT TOP (@Take) SearchRunId, SeedKeyword, LocationName, CenterLat, CenterLng, RadiusMeters, ResultLimit, FetchDetailedData, FetchGoogleReviews, FetchGoogleUpdates, FetchGoogleQuestionsAndAnswers, RanAtUtc
FROM dbo.SearchRun ORDER BY SearchRunId DESC", new { Take = take }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<SearchRun>(new CommandDefinition(@"
SELECT SearchRunId, SeedKeyword, LocationName, CenterLat, CenterLng, RadiusMeters, ResultLimit, FetchDetailedData, FetchGoogleReviews, FetchGoogleUpdates, FetchGoogleQuestionsAndAnswers, RanAtUtc
FROM dbo.SearchRun
WHERE SearchRunId=@RunId", new { RunId = runId }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<PlaceSnapshotRow>(new CommandDefinition(@"
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
       v.StatusLabel, v.MomentumScore
FROM dbo.PlaceSnapshot s
JOIN dbo.Place p ON p.PlaceId=s.PlaceId
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
ORDER BY s.RankPosition", new { RunId = runId }, cancellationToken: ct));
        return rows.ToList();
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

    public async Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct, int reviewPage = 1, int reviewPageSize = 25)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var effectiveReviewPageSize = Math.Clamp(reviewPageSize, 10, 100);
        var effectiveReviewPage = Math.Max(1, reviewPage);

        var place = await conn.QuerySingleOrDefaultAsync<PlaceDetailsRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  DisplayName,
  FormattedAddress,
  PrimaryType,
  PrimaryCategory,
  TypesCsv,
  NationalPhoneNumber,
  WebsiteUri,
  SearchLocationName,
  QuestionAnswerCount,
  Lat,
  Lng,
  Description,
  PhotoCount,
  OtherCategoriesJson,
  PlaceTopicsJson,
  IsServiceAreaBusiness,
  BusinessStatus,
  RegularOpeningHoursJson
FROM dbo.Place
WHERE PlaceId=@PlaceId", new { PlaceId = placeId }, cancellationToken: ct));

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
SELECT TOP 1 SearchRunId, CenterLat, CenterLng
FROM dbo.SearchRun
WHERE SearchRunId=@SearchRunId;",
                new { SearchRunId = mapRunId.Value }, cancellationToken: ct));
        }

        var contextRunId = runId ?? active?.SearchRunId;
        SearchRunContextRow? runContext = null;
        if (contextRunId.HasValue)
        {
            runContext = await conn.QuerySingleOrDefaultAsync<SearchRunContextRow>(new CommandDefinition(@"
SELECT TOP 1 SearchRunId, SeedKeyword, LocationName
FROM dbo.SearchRun
WHERE SearchRunId=@SearchRunId;",
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
  r.SeedKeyword,
  r.LocationName
FROM dbo.PlaceSnapshot s
LEFT JOIN dbo.SearchRun r ON r.SearchRunId = s.SearchRunId
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

        return new PlaceDetailsViewModel
        {
            PlaceId = place.PlaceId,
            DisplayName = PreferNonEmpty(place.DisplayName, liveDetails?.DisplayName),
            ContextSeedKeyword = runContext?.SeedKeyword,
            ContextLocationName = runContext?.LocationName,
            FormattedAddress = PreferNonEmpty(place.FormattedAddress, liveDetails?.FormattedAddress),
            PrimaryType = primaryType,
            PrimaryCategory = primaryCategory,
            NationalPhoneNumber = PreferNonEmpty(place.NationalPhoneNumber, liveDetails?.NationalPhoneNumber),
            WebsiteUri = PreferNonEmpty(place.WebsiteUri, liveDetails?.WebsiteUri),
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
            ReviewVelocity = await reviewVelocityService.GetPlaceReviewVelocityAsync(placeId, ct),
            UpdateVelocity = await reviewVelocityService.GetPlaceUpdateVelocityAsync(placeId, ct)
        };
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
            var canRefresh = (nowUtc - lastRunAtUtc) >= TimeSpan.FromHours(Math.Max(1, refreshThresholdHours));
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
            Build("questions_and_answers", "Google Question & Answers collection", settings.GoogleQuestionsAndAnswersRefreshHours)
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
        string? FormattedAddress,
        string? PrimaryType,
        string? PrimaryCategory,
        string? TypesCsv,
        string? NationalPhoneNumber,
        string? WebsiteUri,
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
        string? RegularOpeningHoursJson);

    private sealed record SearchRunCenterRow(long SearchRunId, decimal? CenterLat, decimal? CenterLng);

    private sealed record SearchRunContextRow(long SearchRunId, string SeedKeyword, string LocationName);

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
