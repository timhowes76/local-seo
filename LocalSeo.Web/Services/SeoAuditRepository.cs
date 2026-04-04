using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public sealed class SeoAuditRepository(ISqlConnectionFactory connectionFactory) : ISeoAuditRepository
{
    public async Task<IReadOnlyList<SeoAuditRuleDefinition>> GetAllRulesAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        using var grid = await conn.QueryMultipleAsync(new CommandDefinition(@"
SELECT
  SeoAuditRuleId,
  RuleKey,
  [Name],
  [Description],
  RuleGroup,
  RuleMode,
  RuleType,
  EntityType,
  Severity,
  WarningScoreImpact,
  FailScoreImpact,
  SortOrder,
  IsActive,
  ShowInActionsTab,
  WhyItMattersText,
  RecommendedActionText,
  CreatedAtUtc,
  UpdatedAtUtc
FROM dbo.SeoAuditRule
ORDER BY SortOrder ASC, SeoAuditRuleId ASC;

SELECT
  SeoAuditRuleParameterId,
  SeoAuditRuleId,
  ParameterName,
  ParameterValue,
  ValueType,
  SortOrder,
  IsActive
FROM dbo.SeoAuditRuleParameter
ORDER BY SeoAuditRuleId ASC, SortOrder ASC, SeoAuditRuleParameterId ASC;",
            cancellationToken: ct));

        var rules = (await grid.ReadAsync<SeoAuditRuleDataRow>()).ToList();
        var parameters = (await grid.ReadAsync<SeoAuditRuleParameterDataRow>())
            .GroupBy(x => x.SeoAuditRuleId)
            .ToDictionary(
                x => x.Key,
                x => (IReadOnlyList<SeoAuditRuleParameterDefinition>)x
                    .Select(p => new SeoAuditRuleParameterDefinition(
                        p.SeoAuditRuleParameterId,
                        p.SeoAuditRuleId,
                        p.ParameterName,
                        p.ParameterValue,
                        p.ValueType,
                        p.SortOrder,
                        p.IsActive))
                    .ToList());

        return rules
            .Select(rule => new SeoAuditRuleDefinition(
                rule.SeoAuditRuleId,
                rule.RuleKey,
                rule.Name,
                rule.Description,
                rule.RuleGroup,
                rule.RuleMode,
                rule.RuleType,
                rule.EntityType,
                rule.Severity,
                rule.WarningScoreImpact,
                rule.FailScoreImpact,
                rule.SortOrder,
                rule.IsActive,
                rule.ShowInActionsTab,
                rule.WhyItMattersText,
                rule.RecommendedActionText,
                rule.CreatedAtUtc,
                rule.UpdatedAtUtc,
                parameters.TryGetValue(rule.SeoAuditRuleId, out var ruleParameters) ? ruleParameters : []))
            .ToList();
    }

    public async Task<IReadOnlyList<SeoAuditRuleListRow>> GetAdminRuleListAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SeoAuditRuleListRow>(new CommandDefinition(@"
SELECT
  r.SeoAuditRuleId,
  r.RuleKey,
  r.[Name],
  r.RuleGroup,
  r.RuleMode,
  r.RuleType,
  r.EntityType,
  r.Severity,
  r.WarningScoreImpact,
  r.FailScoreImpact,
  r.SortOrder,
  r.IsActive,
  r.ShowInActionsTab,
  r.UpdatedAtUtc,
  (
    SELECT COUNT(1)
    FROM dbo.SeoAuditRuleParameter p
    WHERE p.SeoAuditRuleId = r.SeoAuditRuleId
      AND p.IsActive = 1
  ) AS ParameterCount
FROM dbo.SeoAuditRule r
ORDER BY r.SortOrder ASC, r.SeoAuditRuleId ASC;",
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<SeoAuditRuleDefinition?> GetRuleByIdAsync(long ruleId, CancellationToken ct)
    {
        if (ruleId <= 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        using var grid = await conn.QueryMultipleAsync(new CommandDefinition(@"
SELECT
  SeoAuditRuleId,
  RuleKey,
  [Name],
  [Description],
  RuleGroup,
  RuleMode,
  RuleType,
  EntityType,
  Severity,
  WarningScoreImpact,
  FailScoreImpact,
  SortOrder,
  IsActive,
  ShowInActionsTab,
  WhyItMattersText,
  RecommendedActionText,
  CreatedAtUtc,
  UpdatedAtUtc
FROM dbo.SeoAuditRule
WHERE SeoAuditRuleId = @SeoAuditRuleId;

SELECT
  SeoAuditRuleParameterId,
  SeoAuditRuleId,
  ParameterName,
  ParameterValue,
  ValueType,
  SortOrder,
  IsActive
FROM dbo.SeoAuditRuleParameter
WHERE SeoAuditRuleId = @SeoAuditRuleId
ORDER BY SortOrder ASC, SeoAuditRuleParameterId ASC;",
            new { SeoAuditRuleId = ruleId },
            cancellationToken: ct));

        var rule = await grid.ReadSingleOrDefaultAsync<SeoAuditRuleDataRow>();
        if (rule is null)
            return null;

        var parameters = (await grid.ReadAsync<SeoAuditRuleParameterDataRow>())
            .Select(p => new SeoAuditRuleParameterDefinition(
                p.SeoAuditRuleParameterId,
                p.SeoAuditRuleId,
                p.ParameterName,
                p.ParameterValue,
                p.ValueType,
                p.SortOrder,
                p.IsActive))
            .ToList();

        return new SeoAuditRuleDefinition(
            rule.SeoAuditRuleId,
            rule.RuleKey,
            rule.Name,
            rule.Description,
            rule.RuleGroup,
            rule.RuleMode,
            rule.RuleType,
            rule.EntityType,
            rule.Severity,
            rule.WarningScoreImpact,
            rule.FailScoreImpact,
            rule.SortOrder,
            rule.IsActive,
            rule.ShowInActionsTab,
            rule.WhyItMattersText,
            rule.RecommendedActionText,
            rule.CreatedAtUtc,
            rule.UpdatedAtUtc,
            parameters);
    }

    public async Task<long> CreateRuleAsync(SeoAuditRuleUpsertRequest request, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var ruleId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SeoAuditRule(
  RuleKey,
  [Name],
  [Description],
  RuleGroup,
  RuleMode,
  RuleType,
  EntityType,
  Severity,
  WarningScoreImpact,
  FailScoreImpact,
  SortOrder,
  IsActive,
  ShowInActionsTab,
  WhyItMattersText,
  RecommendedActionText,
  CreatedAtUtc,
  UpdatedAtUtc)
VALUES(
  @RuleKey,
  @Name,
  @Description,
  @RuleGroup,
  @RuleMode,
  @RuleType,
  @EntityType,
  @Severity,
  @WarningScoreImpact,
  @FailScoreImpact,
  @SortOrder,
  @IsActive,
  @ShowInActionsTab,
  @WhyItMattersText,
  @RecommendedActionText,
  @NowUtc,
  @NowUtc);
SELECT CAST(SCOPE_IDENTITY() AS bigint);",
            new
            {
                request.RuleKey,
                request.Name,
                request.Description,
                request.RuleGroup,
                request.RuleMode,
                request.RuleType,
                request.EntityType,
                request.Severity,
                request.WarningScoreImpact,
                request.FailScoreImpact,
                request.SortOrder,
                request.IsActive,
                request.ShowInActionsTab,
                request.WhyItMattersText,
                request.RecommendedActionText,
                request.NowUtc
            },
            tx,
            cancellationToken: ct));

        await SyncParametersAsync(conn, tx, ruleId, request.Parameters, ct);
        await tx.CommitAsync(ct);
        return ruleId;
    }

    public async Task<bool> UpdateRuleAsync(long ruleId, SeoAuditRuleUpsertRequest request, CancellationToken ct)
    {
        if (ruleId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.SeoAuditRule
SET
  [Name] = @Name,
  [Description] = @Description,
  RuleGroup = @RuleGroup,
  RuleMode = @RuleMode,
  RuleType = @RuleType,
  EntityType = @EntityType,
  Severity = @Severity,
  WarningScoreImpact = @WarningScoreImpact,
  FailScoreImpact = @FailScoreImpact,
  SortOrder = @SortOrder,
  IsActive = @IsActive,
  ShowInActionsTab = @ShowInActionsTab,
  WhyItMattersText = @WhyItMattersText,
  RecommendedActionText = @RecommendedActionText,
  UpdatedAtUtc = @NowUtc
WHERE SeoAuditRuleId = @SeoAuditRuleId;",
            new
            {
                SeoAuditRuleId = ruleId,
                request.Name,
                request.Description,
                request.RuleGroup,
                request.RuleMode,
                request.RuleType,
                request.EntityType,
                request.Severity,
                request.WarningScoreImpact,
                request.FailScoreImpact,
                request.SortOrder,
                request.IsActive,
                request.ShowInActionsTab,
                request.WhyItMattersText,
                request.RecommendedActionText,
                request.NowUtc
            },
            tx,
            cancellationToken: ct));
        if (updated <= 0)
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        await SyncParametersAsync(conn, tx, ruleId, request.Parameters, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<bool> SetRuleActiveAsync(long ruleId, bool isActive, CancellationToken ct)
    {
        if (ruleId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.SeoAuditRule
SET
  IsActive = @IsActive,
  UpdatedAtUtc = SYSUTCDATETIME()
WHERE SeoAuditRuleId = @SeoAuditRuleId;",
            new
            {
                SeoAuditRuleId = ruleId,
                IsActive = isActive
            },
            cancellationToken: ct));
        return updated > 0;
    }

    public async Task<PlaceAuditContext?> GetPlaceAuditContextAsync(string placeId, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        using var grid = await conn.QueryMultipleAsync(new CommandDefinition(@"
  SELECT
    p.PlaceId,
    p.DisplayName,
    p.PrimaryCategory,
    p.Description,
    p.FormattedAddress,
    p.NationalPhoneNumber,
    p.WebsiteUri,
    p.WebsiteType,
  p.PhotoCount,
  p.Lat,
  p.Lng,
  p.QuestionAnswerCount AS StoredQuestionAnswerCount,
  p.OtherCategoriesJson,
  p.RegularOpeningHoursJson,
  town.Latitude AS TownCenterLat,
  town.Longitude AS TownCenterLng,
  sr.CategoryId AS SourceCategoryId,
  cat.DisplayName AS SourceKeyword,
  town.Name AS SourceTownName,
  latest.SearchRunId AS LastSourceSearchRunId,
  latest.Rating AS LatestRating,
  latest.UserRatingCount AS LatestUserRatingCount
FROM dbo.Place p
OUTER APPLY (
  SELECT TOP 1
    ps.SearchRunId,
    ps.Rating,
    ps.UserRatingCount
  FROM dbo.PlaceSnapshot ps
  WHERE ps.PlaceId = p.PlaceId
  ORDER BY ps.CapturedAtUtc DESC, ps.PlaceSnapshotId DESC
) latest
LEFT JOIN dbo.SearchRun sr ON sr.SearchRunId = latest.SearchRunId
LEFT JOIN dbo.GbTown town ON town.TownId = sr.TownId
LEFT JOIN dbo.GoogleBusinessProfileCategory cat ON cat.CategoryId = sr.CategoryId
WHERE p.PlaceId = @PlaceId;

SELECT
  pr.ReviewTimestampUtc,
  pr.LastSeenUtc,
  pr.OwnerTimestampUtc,
  pr.ReviewText,
  pr.Rating,
  pr.PhotosCount,
  CAST(CASE
    WHEN NULLIF(LTRIM(RTRIM(COALESCE(pr.OwnerAnswer, N''))), N'') IS NOT NULL OR pr.OwnerTimestampUtc IS NOT NULL
      THEN 1
      ELSE 0
  END AS bit) AS HasOwnerResponse
FROM dbo.PlaceReview pr
WHERE pr.PlaceId = @PlaceId;

SELECT
  effective.EffectiveUpdateUtc
FROM dbo.PlaceUpdate pu
CROSS APPLY (
  SELECT COALESCE(
    CAST(TRY_CAST(JSON_VALUE(pu.RawJson, '$.timestamp') AS datetimeoffset(0)) AS datetime2(0)),
    CASE
      WHEN TRY_CONVERT(bigint, JSON_VALUE(pu.RawJson, '$.timestamp')) BETWEEN 1000000000 AND 9999999999
        THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(pu.RawJson, '$.timestamp'))), CAST('1970-01-01T00:00:00' AS datetime2(0)))
      WHEN TRY_CONVERT(bigint, JSON_VALUE(pu.RawJson, '$.timestamp')) BETWEEN 1000000000000 AND 9999999999999
        THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(pu.RawJson, '$.timestamp')) / 1000), CAST('1970-01-01T00:00:00' AS datetime2(0)))
      ELSE NULL
    END,
    CAST(TRY_CAST(JSON_VALUE(pu.RawJson, '$.post_date') AS datetimeoffset(0)) AS datetime2(0)),
    CAST(TRY_CAST(JSON_VALUE(pu.RawJson, '$.date_posted') AS datetimeoffset(0)) AS datetime2(0)),
    CAST(TRY_CAST(JSON_VALUE(pu.RawJson, '$.posted_at') AS datetimeoffset(0)) AS datetime2(0)),
    CAST(TRY_CAST(JSON_VALUE(pu.RawJson, '$.date') AS datetimeoffset(0)) AS datetime2(0)),
    TRY_CONVERT(datetime2(0), JSON_VALUE(pu.RawJson, '$.timestamp'), 112),
    TRY_CONVERT(datetime2(0), JSON_VALUE(pu.RawJson, '$.post_date'), 112),
    TRY_CONVERT(datetime2(0), JSON_VALUE(pu.RawJson, '$.date_posted'), 112),
    TRY_CONVERT(datetime2(0), JSON_VALUE(pu.RawJson, '$.posted_at'), 112),
    TRY_CONVERT(datetime2(0), JSON_VALUE(pu.RawJson, '$.date'), 112),
    pu.PostDateUtc,
    pu.FirstSeenUtc
  ) AS EffectiveUpdateUtc
) effective
WHERE pu.PlaceId = @PlaceId
ORDER BY effective.EffectiveUpdateUtc DESC, pu.PlaceUpdateId DESC;

SELECT
  pqa.AnswerText,
  pqa.AnswerTimestampUtc
FROM dbo.PlaceQuestionAnswer pqa
WHERE pqa.PlaceId = @PlaceId
ORDER BY COALESCE(pqa.QuestionTimestampUtc, pqa.LastSeenUtc) DESC, pqa.PlaceQuestionAnswerId DESC;

WITH LatestRun AS (
  SELECT TOP 1
    ps.SearchRunId
  FROM dbo.PlaceSnapshot ps
  WHERE ps.PlaceId = @PlaceId
  ORDER BY ps.CapturedAtUtc DESC, ps.PlaceSnapshotId DESC
)
SELECT
  p.PlaceId,
  p.Lat,
  p.Lng
FROM LatestRun lr
JOIN dbo.PlaceSnapshot ps ON ps.SearchRunId = lr.SearchRunId
JOIN dbo.Place p ON p.PlaceId = ps.PlaceId
GROUP BY p.PlaceId, p.Lat, p.Lng
ORDER BY p.PlaceId ASC;

IF OBJECT_ID(N'dbo.PlaceWebsite', N'U') IS NOT NULL
BEGIN
  SELECT TOP 1
    PlaceWebsiteId,
    WebsiteUrl,
    NormalizedWebsiteUrl,
    HostName,
    IsHttps,
    [Status],
    LastCheckedUtc,
    LastSuccessfulFetchUtc
  FROM dbo.PlaceWebsite
  WHERE PlaceId = @PlaceId
  ORDER BY PlaceWebsiteId DESC;
END
ELSE
BEGIN
  SELECT
    CAST(NULL AS int) AS PlaceWebsiteId,
    CAST(NULL AS nvarchar(1000)) AS WebsiteUrl,
    CAST(NULL AS nvarchar(1000)) AS NormalizedWebsiteUrl,
    CAST(NULL AS nvarchar(255)) AS HostName,
    CAST(NULL AS bit) AS IsHttps,
    CAST(NULL AS nvarchar(50)) AS [Status],
    CAST(NULL AS datetime2(3)) AS LastCheckedUtc,
    CAST(NULL AS datetime2(3)) AS LastSuccessfulFetchUtc;
END;

IF OBJECT_ID(N'dbo.PlaceWebsiteFetch', N'U') IS NOT NULL AND OBJECT_ID(N'dbo.PlaceWebsite', N'U') IS NOT NULL
BEGIN
  SELECT TOP 1
    f.PlaceWebsiteFetchId,
    f.FetchStartedUtc,
    f.FetchCompletedUtc,
    f.Success,
    f.RequestedUrl,
    f.FinalUrl,
    f.HttpStatusCode,
    f.ErrorCode,
    f.ErrorMessage
  FROM dbo.PlaceWebsiteFetch f
  JOIN dbo.PlaceWebsite w ON w.PlaceWebsiteId = f.PlaceWebsiteId
  WHERE w.PlaceId = @PlaceId
  ORDER BY f.FetchStartedUtc DESC, f.PlaceWebsiteFetchId DESC;
END
ELSE
BEGIN
  SELECT
    CAST(NULL AS bigint) AS PlaceWebsiteFetchId,
    CAST(NULL AS datetime2(3)) AS FetchStartedUtc,
    CAST(NULL AS datetime2(3)) AS FetchCompletedUtc,
    CAST(0 AS bit) AS Success,
    CAST(NULL AS nvarchar(1000)) AS RequestedUrl,
    CAST(NULL AS nvarchar(1000)) AS FinalUrl,
    CAST(NULL AS int) AS HttpStatusCode,
    CAST(NULL AS nvarchar(100)) AS ErrorCode,
    CAST(NULL AS nvarchar(2000)) AS ErrorMessage;
END;

IF OBJECT_ID(N'dbo.PlaceWebsiteHomepageAudit', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.PlaceWebsiteFetch', N'U') IS NOT NULL
   AND OBJECT_ID(N'dbo.PlaceWebsite', N'U') IS NOT NULL
BEGIN
  SELECT TOP 1
    a.PlaceWebsiteFetchId,
    a.TitleTag,
    a.H1Text,
    a.H2TextsJson,
    a.H3TextsJson,
    a.PhoneNumbersJson,
    a.PostalAddressesJson,
    a.PostcodesJson,
    a.CityNamesJson,
    a.BusinessNamesJson,
    a.SchemaTypesJson,
    a.HasLocalBusinessSchema,
    a.PageScheme,
    a.InternalLinkCount,
    a.ServicePageLinkCount,
    a.InternalAnchorTextsJson,
    a.ServiceKeywordsJson,
    a.LocationKeywordsJson,
    a.ServiceTownCombinationsJson,
    a.BrandNamesJson
  FROM dbo.PlaceWebsiteHomepageAudit a
  JOIN dbo.PlaceWebsiteFetch f ON f.PlaceWebsiteFetchId = a.PlaceWebsiteFetchId
  JOIN dbo.PlaceWebsite w ON w.PlaceWebsiteId = f.PlaceWebsiteId
  WHERE w.PlaceId = @PlaceId
    AND f.Success = 1
  ORDER BY f.FetchStartedUtc DESC, a.PlaceWebsiteHomepageAuditId DESC;
END
ELSE
BEGIN
  SELECT
    CAST(NULL AS bigint) AS PlaceWebsiteFetchId,
    CAST(NULL AS nvarchar(1000)) AS TitleTag,
    CAST(NULL AS nvarchar(1000)) AS H1Text,
    CAST(NULL AS nvarchar(max)) AS H2TextsJson,
    CAST(NULL AS nvarchar(max)) AS H3TextsJson,
    CAST(NULL AS nvarchar(max)) AS PhoneNumbersJson,
    CAST(NULL AS nvarchar(max)) AS PostalAddressesJson,
    CAST(NULL AS nvarchar(max)) AS PostcodesJson,
    CAST(NULL AS nvarchar(max)) AS CityNamesJson,
    CAST(NULL AS nvarchar(max)) AS BusinessNamesJson,
    CAST(NULL AS nvarchar(max)) AS SchemaTypesJson,
    CAST(0 AS bit) AS HasLocalBusinessSchema,
    CAST(NULL AS nvarchar(10)) AS PageScheme,
    CAST(NULL AS int) AS InternalLinkCount,
    CAST(NULL AS int) AS ServicePageLinkCount,
    CAST(NULL AS nvarchar(max)) AS InternalAnchorTextsJson,
    CAST(NULL AS nvarchar(max)) AS ServiceKeywordsJson,
    CAST(NULL AS nvarchar(max)) AS LocationKeywordsJson,
    CAST(NULL AS nvarchar(max)) AS ServiceTownCombinationsJson,
    CAST(NULL AS nvarchar(max)) AS BrandNamesJson;
END;",
            new { PlaceId = normalizedPlaceId },
            cancellationToken: ct));

        var place = await grid.ReadSingleOrDefaultAsync<PlaceAuditContextRow>();
        if (place is null)
            return null;

        var reviews = (await grid.ReadAsync<PlaceReviewAuditRowData>())
            .Select(x => new PlaceReviewAuditRow(
                x.ReviewTimestampUtc,
                x.LastSeenUtc,
                x.OwnerTimestampUtc,
                x.ReviewText,
                x.Rating,
                x.PhotosCount,
                x.HasOwnerResponse))
            .ToList();
        var responseTimings = reviews
            .Where(x => x.ReviewTimestampUtc.HasValue && x.OwnerTimestampUtc.HasValue && x.HasOwnerResponse)
            .Select(x => new ReviewResponseTiming(
                x.ReviewTimestampUtc!.Value,
                x.OwnerTimestampUtc!.Value,
                ClampResponseDays(x.ReviewTimestampUtc!.Value, x.OwnerTimestampUtc!.Value),
                ClampCalendarDayDiff(x.ReviewTimestampUtc!.Value, x.OwnerTimestampUtc!.Value)))
            .ToList();
        var updates = (await grid.ReadAsync<PlaceUpdateAuditRowData>())
            .Select(x => new PlaceUpdateAuditRow(x.EffectiveUpdateUtc))
            .ToList();
        var questionsAndAnswers = (await grid.ReadAsync<PlaceQuestionAnswerAuditRowData>())
            .Select(x => new PlaceQuestionAnswerAuditRow(
                x.AnswerText,
                x.AnswerTimestampUtc))
            .ToList();
        var comparablePlaces = (await grid.ReadAsync<PlaceAuditPeerRowData>())
            .Select(x => new PlaceAuditPeer(
                x.PlaceId,
                x.Lat,
                x.Lng))
            .ToList();
        var website = await grid.ReadSingleOrDefaultAsync<PlaceWebsiteAuditContextRow>();
        var latestWebsiteFetch = await grid.ReadSingleOrDefaultAsync<PlaceWebsiteFetchAuditContextRow>();
        var latestHomepageAudit = await grid.ReadSingleOrDefaultAsync<PlaceWebsiteHomepageAuditContextRow>();
        var respondedReviewCount = reviews.Count(x => x.HasOwnerResponse);

        return new PlaceAuditContext(
            place.PlaceId,
            place.DisplayName,
            place.PrimaryCategory,
            place.Description,
            place.FormattedAddress,
            place.NationalPhoneNumber,
            place.WebsiteUri,
            place.WebsiteType,
            place.PhotoCount,
            place.StoredQuestionAnswerCount,
            place.OtherCategoriesJson,
            place.RegularOpeningHoursJson,
            place.Lat,
            place.Lng,
            place.TownCenterLat,
            place.TownCenterLng,
            place.SourceCategoryId,
            place.SourceKeyword,
            place.SourceTownName,
            place.LatestRating,
            place.LatestUserRatingCount,
            place.LastSourceSearchRunId,
            reviews.Count,
            respondedReviewCount,
            updates.Count,
            questionsAndAnswers.Count,
            responseTimings,
            reviews,
            updates,
            questionsAndAnswers,
            comparablePlaces,
            website is null || website.PlaceWebsiteId <= 0
                ? null
                : new PlaceWebsiteAuditContext(
                    website.WebsiteUrl,
                    website.NormalizedWebsiteUrl,
                    website.HostName,
                    website.IsHttps,
                    website.Status,
                    website.LastCheckedUtc,
                    website.LastSuccessfulFetchUtc),
            latestWebsiteFetch is null || latestWebsiteFetch.PlaceWebsiteFetchId <= 0
                ? null
                : BuildLatestWebsiteFetch(latestWebsiteFetch),
            latestHomepageAudit is null || latestHomepageAudit.PlaceWebsiteFetchId <= 0
                ? null
                : BuildLatestHomepageAudit(latestHomepageAudit));
    }

    public async Task UpsertAuditResultsAsync(string placeId, long? lastSourceSearchRunId, IReadOnlyList<SeoAuditEvaluationResult> results, DateTime nowUtc, CancellationToken ct)
    {
        if (results.Count == 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        foreach (var result in results)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.SeoAuditResult AS target
USING (SELECT @PlaceId AS PlaceId, @SeoAuditRuleId AS SeoAuditRuleId) AS source
ON target.PlaceId = source.PlaceId
   AND target.SeoAuditRuleId = source.SeoAuditRuleId
WHEN MATCHED THEN UPDATE SET
  [Status] = @Status,
  ScoreImpactApplied = @ScoreImpactApplied,
  ActualValue = @ActualValue,
  ExpectedValue = @ExpectedValue,
  GapValue = @GapValue,
  SummaryText = @SummaryText,
  WhyItMattersText = @WhyItMattersText,
  RecommendedActionText = @RecommendedActionText,
  SortOrderSnapshot = @SortOrderSnapshot,
  LastSourceSearchRunId = @LastSourceSearchRunId,
  LastEvaluatedAtUtc = @LastEvaluatedAtUtc,
  UpdatedAtUtc = @NowUtc
WHEN NOT MATCHED THEN INSERT(
  PlaceId,
  SeoAuditRuleId,
  [Status],
  ScoreImpactApplied,
  ActualValue,
  ExpectedValue,
  GapValue,
  SummaryText,
  WhyItMattersText,
  RecommendedActionText,
  SortOrderSnapshot,
  LastSourceSearchRunId,
  LastEvaluatedAtUtc,
  CreatedAtUtc,
  UpdatedAtUtc)
VALUES(
  @PlaceId,
  @SeoAuditRuleId,
  @Status,
  @ScoreImpactApplied,
  @ActualValue,
  @ExpectedValue,
  @GapValue,
  @SummaryText,
  @WhyItMattersText,
  @RecommendedActionText,
  @SortOrderSnapshot,
  @LastSourceSearchRunId,
  @LastEvaluatedAtUtc,
  @NowUtc,
  @NowUtc);",
                new
                {
                    PlaceId = placeId,
                    result.SeoAuditRuleId,
                    result.Status,
                    result.ScoreImpactApplied,
                    result.ActualValue,
                    result.ExpectedValue,
                    result.GapValue,
                    result.SummaryText,
                    result.WhyItMattersText,
                    result.RecommendedActionText,
                    result.SortOrderSnapshot,
                    LastSourceSearchRunId = lastSourceSearchRunId,
                    LastEvaluatedAtUtc = nowUtc,
                    NowUtc = nowUtc
                },
                tx,
                cancellationToken: ct));
        }

        await tx.CommitAsync(ct);
    }

    public async Task<IReadOnlyList<SeoAuditPlaceResultRow>> GetAuditResultsForPlaceAsync(string placeId, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SeoAuditPlaceResultRow>(new CommandDefinition(@"
SELECT
  ar.SeoAuditRuleId,
  r.RuleKey,
  r.[Name],
  r.Severity,
  ar.[Status],
  ar.ScoreImpactApplied,
  r.FailScoreImpact AS PossiblePoints,
  ar.ActualValue,
  ar.ExpectedValue,
  ar.GapValue,
  ar.SummaryText,
  ar.WhyItMattersText,
  ar.RecommendedActionText,
  ar.SortOrderSnapshot,
  ar.LastSourceSearchRunId,
  ar.LastEvaluatedAtUtc
FROM dbo.SeoAuditResult ar
JOIN dbo.SeoAuditRule r ON r.SeoAuditRuleId = ar.SeoAuditRuleId
WHERE ar.PlaceId = @PlaceId
  AND r.IsActive = 1
  AND r.ShowInActionsTab = 1
ORDER BY ar.ScoreImpactApplied DESC, ar.SortOrderSnapshot ASC, ar.SeoAuditRuleId ASC;",
            new { PlaceId = normalizedPlaceId },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<string>> GetPlaceIdsMissingResultsAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<string>(new CommandDefinition(@"
WITH ActiveRules AS (
  SELECT COUNT(1) AS ActiveRuleCount
  FROM dbo.SeoAuditRule
  WHERE IsActive = 1
)
SELECT p.PlaceId
FROM dbo.Place p
CROSS JOIN ActiveRules ar
OUTER APPLY (
  SELECT COUNT(1) AS ResultCount
  FROM dbo.SeoAuditResult r
  JOIN dbo.SeoAuditRule sr ON sr.SeoAuditRuleId = r.SeoAuditRuleId
  WHERE r.PlaceId = p.PlaceId
    AND sr.IsActive = 1
) rc
WHERE ar.ActiveRuleCount > 0
  AND ISNULL(rc.ResultCount, 0) < ar.ActiveRuleCount
ORDER BY p.PlaceId ASC;",
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<string>> GetAllPlaceIdsAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<string>(new CommandDefinition(@"
SELECT p.PlaceId
FROM dbo.Place p
ORDER BY p.PlaceId ASC;",
            cancellationToken: ct));
        return rows.ToList();
    }

    private static async Task SyncParametersAsync(SqlConnection conn, System.Data.Common.DbTransaction tx, long ruleId, IReadOnlyList<SeoAuditRuleParameterUpsertRequest> parameters, CancellationToken ct)
    {
        var existingIds = (await conn.QueryAsync<long>(new CommandDefinition(@"
SELECT SeoAuditRuleParameterId
FROM dbo.SeoAuditRuleParameter
WHERE SeoAuditRuleId = @SeoAuditRuleId;",
            new { SeoAuditRuleId = ruleId },
            tx,
            cancellationToken: ct))).ToHashSet();

        var submittedIds = new HashSet<long>();
        foreach (var parameter in parameters)
        {
            if (parameter.SeoAuditRuleParameterId.HasValue && existingIds.Contains(parameter.SeoAuditRuleParameterId.Value))
            {
                submittedIds.Add(parameter.SeoAuditRuleParameterId.Value);
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.SeoAuditRuleParameter
SET
  ParameterName = @ParameterName,
  ParameterValue = @ParameterValue,
  ValueType = @ValueType,
  SortOrder = @SortOrder,
  IsActive = @IsActive
WHERE SeoAuditRuleParameterId = @SeoAuditRuleParameterId;",
                    new
                    {
                        SeoAuditRuleParameterId = parameter.SeoAuditRuleParameterId.Value,
                        parameter.ParameterName,
                        parameter.ParameterValue,
                        parameter.ValueType,
                        parameter.SortOrder,
                        parameter.IsActive
                    },
                    tx,
                    cancellationToken: ct));
                continue;
            }

            var id = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SeoAuditRuleParameter(
  SeoAuditRuleId,
  ParameterName,
  ParameterValue,
  ValueType,
  SortOrder,
  IsActive)
VALUES(
  @SeoAuditRuleId,
  @ParameterName,
  @ParameterValue,
  @ValueType,
  @SortOrder,
  @IsActive);
SELECT CAST(SCOPE_IDENTITY() AS bigint);",
                new
                {
                    SeoAuditRuleId = ruleId,
                    parameter.ParameterName,
                    parameter.ParameterValue,
                    parameter.ValueType,
                    parameter.SortOrder,
                    parameter.IsActive
                },
                tx,
                cancellationToken: ct));
            submittedIds.Add(id);
        }

        var missingIds = existingIds.Where(x => !submittedIds.Contains(x)).ToArray();
        if (missingIds.Length > 0)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.SeoAuditRuleParameter
SET IsActive = 0
WHERE SeoAuditRuleParameterId IN @Ids;",
                new { Ids = missingIds },
                tx,
                cancellationToken: ct));
        }
    }

    private static double ClampResponseDays(DateTime reviewTimestampUtc, DateTime ownerTimestampUtc)
    {
        if (ownerTimestampUtc <= reviewTimestampUtc)
            return 0d;

        return (ownerTimestampUtc - reviewTimestampUtc).TotalDays;
    }

    private static int ClampCalendarDayDiff(DateTime reviewTimestampUtc, DateTime ownerTimestampUtc)
    {
        if (ownerTimestampUtc <= reviewTimestampUtc)
            return 0;

        return Math.Max(0, (ownerTimestampUtc.Date - reviewTimestampUtc.Date).Days);
    }

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

    private static PlaceWebsiteFetchAuditContext BuildLatestWebsiteFetch(PlaceWebsiteFetchAuditContextRow row)
    {
        return new PlaceWebsiteFetchAuditContext(
            row.PlaceWebsiteFetchId.GetValueOrDefault(),
            row.FetchStartedUtc ?? DateTime.MinValue,
            row.FetchCompletedUtc,
            row.Success,
            row.RequestedUrl ?? string.Empty,
            row.FinalUrl,
            row.HttpStatusCode,
            row.ErrorCode,
            row.ErrorMessage);
    }

    private static PlaceWebsiteHomepageAuditContext BuildLatestHomepageAudit(PlaceWebsiteHomepageAuditContextRow row)
    {
        return new PlaceWebsiteHomepageAuditContext(
            row.PlaceWebsiteFetchId.GetValueOrDefault(),
            row.TitleTag,
            row.H1Text,
            DeserializeJsonList(row.H2TextsJson),
            DeserializeJsonList(row.H3TextsJson),
            DeserializeJsonList(row.PhoneNumbersJson),
            DeserializeJsonList(row.PostalAddressesJson),
            DeserializeJsonList(row.PostcodesJson),
            DeserializeJsonList(row.CityNamesJson),
            DeserializeJsonList(row.BusinessNamesJson),
            DeserializeJsonList(row.SchemaTypesJson),
            row.HasLocalBusinessSchema,
            row.PageScheme,
            row.InternalLinkCount,
            row.ServicePageLinkCount,
            DeserializeJsonList(row.InternalAnchorTextsJson),
            DeserializeJsonList(row.ServiceKeywordsJson),
            DeserializeJsonList(row.LocationKeywordsJson),
            DeserializeJsonList(row.ServiceTownCombinationsJson),
            DeserializeJsonList(row.BrandNamesJson));
    }

    private sealed record SeoAuditRuleDataRow(
        long SeoAuditRuleId,
        string RuleKey,
        string Name,
        string Description,
        string RuleGroup,
        string RuleMode,
        string RuleType,
        string EntityType,
        string Severity,
        int WarningScoreImpact,
        int FailScoreImpact,
        int SortOrder,
        bool IsActive,
        bool ShowInActionsTab,
        string WhyItMattersText,
        string RecommendedActionText,
        DateTime CreatedAtUtc,
        DateTime UpdatedAtUtc);

    private sealed record SeoAuditRuleParameterDataRow(
        long SeoAuditRuleParameterId,
        long SeoAuditRuleId,
        string ParameterName,
        string ParameterValue,
        string ValueType,
        int SortOrder,
        bool IsActive);

    private sealed record PlaceAuditContextRow(
        string PlaceId,
        string? DisplayName,
        string? PrimaryCategory,
        string? Description,
        string? FormattedAddress,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        WebsiteType WebsiteType,
        int? PhotoCount,
        decimal? Lat,
        decimal? Lng,
        int? StoredQuestionAnswerCount,
        string? OtherCategoriesJson,
        string? RegularOpeningHoursJson,
        decimal? TownCenterLat,
        decimal? TownCenterLng,
        string? SourceCategoryId,
        string? SourceKeyword,
        string? SourceTownName,
        long? LastSourceSearchRunId,
        decimal? LatestRating,
        int? LatestUserRatingCount);

    private sealed record PlaceWebsiteAuditContextRow(
        int? PlaceWebsiteId,
        string? WebsiteUrl,
        string? NormalizedWebsiteUrl,
        string? HostName,
        bool? IsHttps,
        string? Status,
        DateTime? LastCheckedUtc,
        DateTime? LastSuccessfulFetchUtc);

    private sealed record PlaceWebsiteFetchAuditContextRow(
        long? PlaceWebsiteFetchId,
        DateTime? FetchStartedUtc,
        DateTime? FetchCompletedUtc,
        bool Success,
        string? RequestedUrl,
        string? FinalUrl,
        int? HttpStatusCode,
        string? ErrorCode,
        string? ErrorMessage);

    private sealed record PlaceWebsiteHomepageAuditContextRow(
        long? PlaceWebsiteFetchId,
        string? TitleTag,
        string? H1Text,
        string? H2TextsJson,
        string? H3TextsJson,
        string? PhoneNumbersJson,
        string? PostalAddressesJson,
        string? PostcodesJson,
        string? CityNamesJson,
        string? BusinessNamesJson,
        string? SchemaTypesJson,
        bool HasLocalBusinessSchema,
        string? PageScheme,
        int? InternalLinkCount,
        int? ServicePageLinkCount,
        string? InternalAnchorTextsJson,
        string? ServiceKeywordsJson,
        string? LocationKeywordsJson,
        string? ServiceTownCombinationsJson,
        string? BrandNamesJson);

    private sealed record PlaceReviewAuditRowData(
        DateTime? ReviewTimestampUtc,
        DateTime LastSeenUtc,
        DateTime? OwnerTimestampUtc,
        string? ReviewText,
        decimal? Rating,
        int? PhotosCount,
        bool HasOwnerResponse);

    private sealed record PlaceUpdateAuditRowData(DateTime? EffectiveUpdateUtc);

    private sealed record PlaceQuestionAnswerAuditRowData(
        string? AnswerText,
        DateTime? AnswerTimestampUtc);

    private sealed record PlaceAuditPeerRowData(
        string PlaceId,
        decimal? Lat,
        decimal? Lng);
}
