using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IReviewVelocityService
{
    Task RecomputePlaceStatsAsync(string placeId, CancellationToken ct);
    Task<IReadOnlyList<PlaceVelocityListItemDto>> GetPlaceVelocityListAsync(string? sort, string? direction, CancellationToken ct);
    Task<PlaceReviewVelocityDetailsDto?> GetPlaceReviewVelocityAsync(string placeId, CancellationToken ct);
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

    public async Task<IReadOnlyList<PlaceVelocityListItemDto>> GetPlaceVelocityListAsync(string? sort, string? direction, CancellationToken ct)
    {
        var sortKey = (sort ?? "needsAttention").Trim();
        var dir = string.Equals(direction, "asc", StringComparison.OrdinalIgnoreCase) ? "ASC" : "DESC";
        var orderBy = sortKey.ToLowerInvariant() switch
        {
            "reviewslast90" => $"COALESCE(v.ReviewsLast90, 0) {dir}, p.DisplayName ASC",
            "trend90pct" => $"COALESCE(v.Trend90Pct, 0) {dir}, p.DisplayName ASC",
            "dayssincelastreview" => $"COALESCE(v.DaysSinceLastReview, 999999) {dir}, p.DisplayName ASC",
            _ => "CASE COALESCE(v.StatusLabel,'NoReviews') WHEN 'Stalled' THEN 1 WHEN 'Slowing' THEN 2 WHEN 'NoReviews' THEN 3 WHEN 'Healthy' THEN 4 WHEN 'Accelerating' THEN 5 ELSE 6 END ASC, COALESCE(v.MomentumScore, 0) ASC, p.DisplayName ASC"
        };

        var sql = $@"
SELECT
  p.PlaceId,
  p.DisplayName,
  latest.Rating,
  latest.UserRatingCount,
  v.ReviewsLast90,
  v.AvgPerMonth12m,
  v.Trend90Pct,
  v.DaysSinceLastReview,
  v.StatusLabel,
  v.MomentumScore
FROM dbo.Place p
OUTER APPLY (
  SELECT TOP 1 s.Rating, s.UserRatingCount
  FROM dbo.PlaceSnapshot s
  WHERE s.PlaceId=p.PlaceId
  ORDER BY s.CapturedAtUtc DESC
) latest
LEFT JOIN dbo.PlaceReviewVelocityStats v ON v.PlaceId=p.PlaceId
ORDER BY {orderBy};";

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<PlaceVelocityListItemDto>(new CommandDefinition(sql, cancellationToken: ct));
        return rows.ToList();
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
DECLARE @StartMonth date = DATEFROMPARTS(YEAR(DATEADD(month,-23,SYSUTCDATETIME())), MONTH(DATEADD(month,-23,SYSUTCDATETIME())), 1);
;WITH months AS (
  SELECT @StartMonth AS MonthStart
  UNION ALL
  SELECT DATEADD(month,1,MonthStart) FROM months WHERE MonthStart < DATEFROMPARTS(YEAR(SYSUTCDATETIME()), MONTH(SYSUTCDATETIME()), 1)
)
SELECT YEAR(m.MonthStart) AS [Year], MONTH(m.MonthStart) AS [Month], COUNT(r.PlaceReviewId) AS ReviewCount
FROM months m
LEFT JOIN dbo.PlaceReview r
  ON r.PlaceId=@PlaceId
 AND r.ReviewTimestampUtc >= m.MonthStart
 AND r.ReviewTimestampUtc < DATEADD(month,1,m.MonthStart)
GROUP BY m.MonthStart
ORDER BY m.MonthStart
OPTION (MAXRECURSION 60);", new { PlaceId = placeId }, cancellationToken: ct));

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
}
