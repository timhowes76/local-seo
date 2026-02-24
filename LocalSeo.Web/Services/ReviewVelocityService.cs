using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IReviewVelocityService
{
    Task RecomputePlaceStatsAsync(string placeId, CancellationToken ct);
    Task<IReadOnlyList<PlaceVelocityListItemDto>> GetPlaceVelocityListAsync(string? sort, string? direction, string? placeName, string? keyword, string? location, int take, CancellationToken ct);
    Task<PlacesRunFilterOptions> GetRunFilterOptionsAsync(CancellationToken ct);
    Task<PlaceReviewVelocityDetailsDto?> GetPlaceReviewVelocityAsync(string placeId, CancellationToken ct);
    Task<PlaceUpdateVelocityDetailsDto?> GetPlaceUpdateVelocityAsync(string placeId, CancellationToken ct);
}

public interface ICompetitorVelocityAdapter
{
    Task<CompetitorVelocityBlockDto?> GetCompetitorVelocityAsync(string placeId, DateTime asOfUtc, CancellationToken ct);
}

public sealed class NullCompetitorVelocityAdapter : ICompetitorVelocityAdapter
{
    public Task<CompetitorVelocityBlockDto?> GetCompetitorVelocityAsync(string placeId, DateTime asOfUtc, CancellationToken ct)
        => Task.FromResult<CompetitorVelocityBlockDto?>(null);
}

public static class ReviewVelocityLogic
{
    public static int MapGrowthScore(decimal? trend90Pct)
    {
        if (!trend90Pct.HasValue)
            return 50;

        var clamped = Math.Clamp(trend90Pct.Value, -100m, 200m);
        if (clamped <= 0m)
            return (int)Math.Round((clamped + 100m) * 0.5m, MidpointRounding.AwayFromZero);
        if (clamped <= 100m)
            return (int)Math.Round(50m + (clamped * 0.25m), MidpointRounding.AwayFromZero);
        return (int)Math.Round(75m + ((clamped - 100m) * 0.25m), MidpointRounding.AwayFromZero);
    }
}

public sealed class ReviewVelocityService(
    ISqlConnectionFactory connectionFactory,
    ICompetitorVelocityAdapter competitorVelocityAdapter) : IReviewVelocityService
{
    public async Task RecomputePlaceStatsAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            "EXEC dbo.usp_RecomputePlaceReviewVelocityStats @PlaceId",
            new { PlaceId = placeId },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<PlaceVelocityListItemDto>> GetPlaceVelocityListAsync(string? sort, string? direction, string? placeName, string? keyword, string? location, int take, CancellationToken ct)
    {
        var sortKey = (sort ?? "rank").Trim();
        var dir = string.Equals(direction, "asc", StringComparison.OrdinalIgnoreCase) ? "ASC" : "DESC";
        var normalizedPlaceName = string.IsNullOrWhiteSpace(placeName) ? null : placeName.Trim();
        var normalizedKeyword = string.IsNullOrWhiteSpace(keyword) ? null : keyword.Trim();
        var normalizedLocation = string.IsNullOrWhiteSpace(location) ? null : location.Trim();
        var normalizedTake = Math.Clamp(take, 1, 1000);
        var orderBy = sortKey.ToLowerInvariant() switch
        {
            "name" => $"p.DisplayName {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "totalreviews" => $"COALESCE(latest.UserRatingCount, 0) {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "reviewslast90" => $"COALESCE(v.ReviewsLast90, 0) {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "dayssincelastreview" => $"COALESCE(v.DaysSinceLastReview, 999999) {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "updates" => $"COALESCE(updCount.UpdateCount, 0) {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "dayssincelastupdate" => $"COALESCE(CASE WHEN upd.LastUpdateDate IS NULL THEN 999999 ELSE DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) END, 999999) {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            "needsattention" => $"CASE COALESCE(v.StatusLabel,'NoReviews') WHEN 'Stalled' THEN 1 WHEN 'Slowing' THEN 2 WHEN 'NoReviews' THEN 3 WHEN 'Healthy' THEN 4 WHEN 'Accelerating' THEN 5 ELSE 6 END {dir}, COALESCE(latest.RankPosition, 999999) ASC",
            _ => $"COALESCE(latest.RankPosition, 999999) {dir}, p.DisplayName ASC"
        };

        var sql = $@"
SELECT TOP (@Take)
  p.PlaceId,
  latest.RankPosition,
  p.DisplayName,
  p.LogoUrl,
  CAST(CASE WHEN pf.PlaceId IS NULL THEN 0 ELSE 1 END AS bit) AS HasFinancialInfo,
  CAST(COALESCE(p.ZohoLeadCreated, 0) AS bit) AS IsZohoConnected,
  p.PrimaryCategory,
  latest.Rating,
  latest.UserRatingCount,
  v.ReviewsLast90,
  v.DaysSinceLastReview,
  v.StatusLabel,
  COALESCE(updCount.UpdateCount, 0) AS UpdateCount,
  CASE WHEN upd.LastUpdateDate IS NULL THEN NULL ELSE DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) END AS DaysSinceLastUpdate,
  CASE
    WHEN latestUpdateTask.Status IN ('NoData','CompletedNoUpdates','CompletedNoData') THEN 'No Data'
    WHEN upd.LastUpdateDate IS NULL THEN 'NoUpdates'
    WHEN DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) > 60 THEN 'Stalled'
    WHEN updTrend.Trend90Pct <= -30 THEN 'Slowing'
    WHEN updTrend.Trend90Pct >= 30 AND DATEDIFF(day, upd.LastUpdateDate, CAST(SYSUTCDATETIME() AS date)) <= 30 THEN 'Accelerating'
    ELSE 'Healthy'
  END AS UpdateStatusLabel,
  CASE WHEN NULLIF(LTRIM(RTRIM(p.WebsiteUri)), N'') IS NULL THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS HasWebsite,
  LEN(COALESCE(p.Description, N'')) AS DescriptionLength,
  CASE
    WHEN p.OtherCategoriesJson IS NULL THEN CAST(0 AS bit)
    WHEN LTRIM(RTRIM(p.OtherCategoriesJson)) IN (N'', N'[]', N'{{}}') THEN CAST(0 AS bit)
    ELSE CAST(1 AS bit)
  END AS HasOtherCategories,
  p.PhotoCount,
  p.QuestionAnswerCount
FROM dbo.Place p
LEFT JOIN dbo.PlacesFinancial pf ON pf.PlaceId = p.PlaceId
OUTER APPLY (
  SELECT TOP 1 s.SearchRunId, s.RankPosition, s.Rating, s.UserRatingCount
  FROM dbo.PlaceSnapshot s
  WHERE s.PlaceId=p.PlaceId
  ORDER BY s.CapturedAtUtc DESC
) latest
LEFT JOIN dbo.SearchRun sr ON sr.SearchRunId = latest.SearchRunId
LEFT JOIN dbo.GoogleBusinessProfileCategory cat ON cat.CategoryId = sr.CategoryId
LEFT JOIN dbo.GbTown town ON town.TownId = sr.TownId
LEFT JOIN dbo.GbCounty county ON county.CountyId = town.CountyId
LEFT JOIN dbo.PlaceReviewVelocityStats v ON v.PlaceId=p.PlaceId
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
  WHERE u.PlaceId=p.PlaceId
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
  WHERE t.PlaceId=p.PlaceId
    AND COALESCE(t.TaskType, 'reviews')='my_business_updates'
  ORDER BY t.CreatedAtUtc DESC, t.DataForSeoReviewTaskId DESC
) latestUpdateTask
OUTER APPLY (
  SELECT COUNT(1) AS UpdateCount
  FROM dbo.PlaceUpdate u2
  WHERE u2.PlaceId=p.PlaceId
) updCount
WHERE (@PlaceName IS NULL OR p.DisplayName LIKE '%' + @PlaceName + '%')
  AND (@Keyword IS NULL OR cat.DisplayName = @Keyword)
  AND (@Location IS NULL OR CONCAT(town.Name, N', ', county.Name) = @Location)
ORDER BY {orderBy};";

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<PlaceVelocityListItemDto>(new CommandDefinition(
            sql,
            new
            {
                PlaceName = normalizedPlaceName,
                Keyword = normalizedKeyword,
                Location = normalizedLocation,
                Take = normalizedTake
            },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<PlacesRunFilterOptions> GetRunFilterOptionsAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var keywords = (await conn.QueryAsync<string>(new CommandDefinition(@"
SELECT DISTINCT cat.DisplayName
FROM dbo.SearchRun sr
JOIN dbo.GoogleBusinessProfileCategory cat ON cat.CategoryId = sr.CategoryId
WHERE cat.DisplayName IS NOT NULL
  AND LTRIM(RTRIM(cat.DisplayName)) <> N''
ORDER BY cat.DisplayName;", cancellationToken: ct))).ToList();

        var locations = (await conn.QueryAsync<string>(new CommandDefinition(@"
SELECT DISTINCT CONCAT(town.Name, N', ', county.Name) AS LocationName
FROM dbo.SearchRun sr
JOIN dbo.GbTown town ON town.TownId = sr.TownId
JOIN dbo.GbCounty county ON county.CountyId = town.CountyId
WHERE town.Name IS NOT NULL
  AND county.Name IS NOT NULL
ORDER BY CONCAT(town.Name, N', ', county.Name);", cancellationToken: ct))).ToList();

        return new PlacesRunFilterOptions(keywords, locations);
    }

    public async Task<PlaceReviewVelocityDetailsDto?> GetPlaceReviewVelocityAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var stats = await conn.QuerySingleOrDefaultAsync<VelocityStatsRow>(new CommandDefinition(@"
SELECT PlaceId, AsOfUtc, ReviewsLast90, ReviewsLast180, ReviewsLast270, ReviewsLast365, AvgPerMonth12m, Prev90, Trend90Pct,
       DaysSinceLastReview, LastReviewTimestampUtc, LongestGapDays12m, RespondedPct12m, AvgOwnerResponseHours12m, MomentumScore, StatusLabel
FROM dbo.PlaceReviewVelocityStats
WHERE PlaceId=@PlaceId", new { PlaceId = placeId }, cancellationToken: ct));

        if (stats is null)
            return null;

        var monthly = await conn.QueryAsync<MonthlyReviewCountDto>(new CommandDefinition(@"
DECLARE @StartMonth date = (
  SELECT DATEFROMPARTS(
    YEAR(MIN(COALESCE(ReviewTimestampUtc, LastSeenUtc))),
    MONTH(MIN(COALESCE(ReviewTimestampUtc, LastSeenUtc))),
    1)
  FROM dbo.PlaceReview
  WHERE PlaceId=@PlaceId
    AND COALESCE(ReviewTimestampUtc, LastSeenUtc) IS NOT NULL
);

IF @StartMonth IS NULL
BEGIN
  SELECT CAST(NULL AS int) AS [Year], CAST(NULL AS int) AS [Month], CAST(NULL AS int) AS ReviewCount
  WHERE 1 = 0;
END
ELSE
BEGIN
  ;WITH months AS (
    SELECT @StartMonth AS MonthStart
    UNION ALL
    SELECT DATEADD(month,1,MonthStart)
    FROM months
    WHERE MonthStart < DATEFROMPARTS(YEAR(SYSUTCDATETIME()), MONTH(SYSUTCDATETIME()), 1)
  )
  SELECT YEAR(m.MonthStart) AS [Year], MONTH(m.MonthStart) AS [Month], COUNT(r.PlaceReviewId) AS ReviewCount
  FROM months m
  LEFT JOIN dbo.PlaceReview r
    ON r.PlaceId=@PlaceId
   AND COALESCE(r.ReviewTimestampUtc, r.LastSeenUtc) >= m.MonthStart
   AND COALESCE(r.ReviewTimestampUtc, r.LastSeenUtc) < DATEADD(month,1,m.MonthStart)
  GROUP BY m.MonthStart
  ORDER BY m.MonthStart
  OPTION (MAXRECURSION 32767);
END;", new { PlaceId = placeId }, cancellationToken: ct));

        var years = await conn.QueryAsync<YearReviewBreakdownDto>(new CommandDefinition(@"
SELECT YEAR(ReviewTimestampUtc) AS [Year],
       COUNT(*) AS ReviewCount,
       CAST(AVG(CAST(Rating AS decimal(9,4))) AS decimal(5,2)) AS AvgRating,
       CAST(SUM(CASE WHEN OwnerTimestampUtc IS NOT NULL AND OwnerTimestampUtc >= ReviewTimestampUtc THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS decimal(7,2)) AS RespondedPct
FROM dbo.PlaceReview
WHERE PlaceId=@PlaceId AND ReviewTimestampUtc IS NOT NULL
GROUP BY YEAR(ReviewTimestampUtc)
ORDER BY [Year] DESC;", new { PlaceId = placeId }, cancellationToken: ct));

        var competitor = await competitorVelocityAdapter.GetCompetitorVelocityAsync(placeId, stats.AsOfUtc ?? DateTime.UtcNow, ct);

        return new PlaceReviewVelocityDetailsDto(
            stats.PlaceId,
            stats.AsOfUtc,
            stats.ReviewsLast90,
            stats.ReviewsLast180,
            stats.ReviewsLast270,
            stats.ReviewsLast365,
            stats.AvgPerMonth12m,
            stats.Prev90,
            stats.Trend90Pct,
            stats.DaysSinceLastReview,
            stats.LastReviewTimestampUtc,
            stats.LongestGapDays12m,
            stats.RespondedPct12m,
            stats.AvgOwnerResponseHours12m,
            stats.MomentumScore,
            stats.StatusLabel,
            monthly.ToList(),
            years.ToList(),
            competitor);
    }

    public async Task<PlaceUpdateVelocityDetailsDto?> GetPlaceUpdateVelocityAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var baseStats = await conn.QuerySingleOrDefaultAsync<UpdateStatsRow>(new CommandDefinition(@"
;WITH updates_effective AS (
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
  FROM dbo.PlaceUpdate u
  WHERE u.PlaceId=@PlaceId
)
SELECT
  @PlaceId AS PlaceId,
  SYSUTCDATETIME() AS AsOfUtc,
  COALESCE((SELECT COUNT(1) FROM updates_effective WHERE EffectiveUpdateUtc >= DATEADD(day,-90,SYSUTCDATETIME())), 0) AS UpdatesLast90,
  COALESCE((SELECT COUNT(1) FROM updates_effective WHERE EffectiveUpdateUtc >= DATEADD(day,-180,SYSUTCDATETIME())), 0) AS UpdatesLast180,
  COALESCE((SELECT COUNT(1) FROM updates_effective WHERE EffectiveUpdateUtc >= DATEADD(day,-270,SYSUTCDATETIME())), 0) AS UpdatesLast270,
  COALESCE((SELECT COUNT(1) FROM updates_effective WHERE EffectiveUpdateUtc >= DATEADD(day,-365,SYSUTCDATETIME())), 0) AS UpdatesLast365,
  COALESCE((SELECT COUNT(1) FROM updates_effective WHERE EffectiveUpdateUtc >= DATEADD(day,-180,SYSUTCDATETIME()) AND EffectiveUpdateUtc < DATEADD(day,-90,SYSUTCDATETIME())), 0) AS Prev90,
  (SELECT MAX(EffectiveUpdateUtc) FROM updates_effective) AS LastUpdateTimestampUtc;",
            new { PlaceId = placeId }, cancellationToken: ct));

        if (baseStats is null)
            return null;

        var updatesLast90 = baseStats.UpdatesLast90 ?? 0;
        var prev90 = baseStats.Prev90 ?? 0;
        var trend90Pct = prev90 == 0
            ? (updatesLast90 == 0 ? 0m : 100m)
            : Math.Round(((updatesLast90 - prev90) * 100m) / prev90, 2, MidpointRounding.AwayFromZero);
        var daysSinceLastUpdate = baseStats.LastUpdateTimestampUtc.HasValue
            ? (int?)Math.Max(0, (baseStats.AsOfUtc - baseStats.LastUpdateTimestampUtc.Value).TotalDays)
            : null;
        var statusLabel = baseStats.LastUpdateTimestampUtc is null
            ? "NoUpdates"
            : daysSinceLastUpdate > 60
                ? "Stalled"
                : trend90Pct <= -30m
                    ? "Slowing"
                    : trend90Pct >= 30m && daysSinceLastUpdate <= 30
                        ? "Accelerating"
                        : "Healthy";

        var monthly = await conn.QueryAsync<MonthlyUpdateCountDto>(new CommandDefinition(@"
DECLARE @StartMonth date = (
  SELECT DATEFROMPARTS(
    YEAR(MIN(src.EffectiveUpdateUtc)),
    MONTH(MIN(src.EffectiveUpdateUtc)),
    1)
  FROM (
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
    FROM dbo.PlaceUpdate u
    WHERE u.PlaceId=@PlaceId
  ) src
  WHERE src.EffectiveUpdateUtc IS NOT NULL
);

IF @StartMonth IS NULL
BEGIN
  SELECT CAST(NULL AS int) AS [Year], CAST(NULL AS int) AS [Month], CAST(NULL AS int) AS UpdateCount
  WHERE 1 = 0;
END
ELSE
BEGIN
  ;WITH months AS (
    SELECT @StartMonth AS MonthStart
    UNION ALL
    SELECT DATEADD(month,1,MonthStart)
    FROM months
    WHERE MonthStart < DATEFROMPARTS(YEAR(SYSUTCDATETIME()), MONTH(SYSUTCDATETIME()), 1)
  )
  SELECT YEAR(m.MonthStart) AS [Year], MONTH(m.MonthStart) AS [Month], COUNT(u.EffectiveUpdateUtc) AS UpdateCount
  FROM months m
  LEFT JOIN (
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
    FROM dbo.PlaceUpdate u
    WHERE u.PlaceId=@PlaceId
  ) u
    ON u.EffectiveUpdateUtc >= m.MonthStart
   AND u.EffectiveUpdateUtc < DATEADD(month,1,m.MonthStart)
  GROUP BY m.MonthStart
  ORDER BY m.MonthStart
  OPTION (MAXRECURSION 32767);
END;", new { PlaceId = placeId }, cancellationToken: ct));

        var years = await conn.QueryAsync<YearUpdateBreakdownDto>(new CommandDefinition(@"
;WITH updates_filtered AS (
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
  FROM dbo.PlaceUpdate u
  WHERE u.PlaceId=@PlaceId
)
SELECT YEAR(EffectiveUpdateUtc) AS [Year],
       COUNT(*) AS UpdateCount
FROM updates_filtered
WHERE EffectiveUpdateUtc IS NOT NULL
GROUP BY YEAR(EffectiveUpdateUtc)
ORDER BY [Year] DESC;", new { PlaceId = placeId }, cancellationToken: ct));

        return new PlaceUpdateVelocityDetailsDto(
            baseStats.PlaceId,
            baseStats.AsOfUtc,
            baseStats.UpdatesLast90,
            baseStats.UpdatesLast180,
            baseStats.UpdatesLast270,
            baseStats.UpdatesLast365,
            baseStats.Prev90,
            trend90Pct,
            daysSinceLastUpdate,
            baseStats.LastUpdateTimestampUtc,
            statusLabel,
            monthly.ToList(),
            years.ToList());
    }

    private sealed record VelocityStatsRow(
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
        string? StatusLabel);

    private sealed record UpdateStatsRow(
        string PlaceId,
        DateTime AsOfUtc,
        int? UpdatesLast90,
        int? UpdatesLast180,
        int? UpdatesLast270,
        int? UpdatesLast365,
        int? Prev90,
        DateTime? LastUpdateTimestampUtc);
}
