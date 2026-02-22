using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;
using Microsoft.Playwright;

namespace LocalSeo.Web.Services;

public interface IReportsService
{
    Task<FirstContactReportAvailability> GetFirstContactAvailabilityAsync(string placeId, long runId, CancellationToken ct);
    Task<FirstContactReportViewModel?> BuildFirstContactReportAsync(string placeId, long runId, FirstContactReportVariant variant, CancellationToken ct);
    Task<FirstContactPdfGenerationResult> GenerateFirstContactPdfAsync(FirstContactPdfGenerationRequest request, CancellationToken ct);
}

public sealed class ReportsService(
    ISqlConnectionFactory connectionFactory,
    IAdminSettingsService adminSettingsService,
    IOptions<BrandSettings> brandOptions,
    IOptions<ReportsOptions> reportsOptions,
    Microsoft.AspNetCore.Hosting.IWebHostEnvironment webHostEnvironment,
    ILogger<ReportsService> logger) : IReportsService
{
    private const string FirstContactReportType = "FirstContactOnePager";
    private const string MissingReviewsMessage = "Run again with Reviews enabled to generate this report.";

    public async Task<FirstContactReportAvailability> GetFirstContactAvailabilityAsync(string placeId, long runId, CancellationToken ct)
    {
        var context = await LoadFirstContactContextAsync(placeId, runId, ct);
        if (context is null)
        {
            return new FirstContactReportAvailability
            {
                PlaceId = placeId,
                RunId = runId,
                IsAvailable = false,
                Message = MissingReviewsMessage
            };
        }

        var variants = BuildVariantSummaries(context.ExistingReportRows);
        if (context.Competitors.Count < 3)
        {
            return new FirstContactReportAvailability
            {
                PlaceId = placeId,
                RunId = runId,
                BusinessName = context.BusinessName,
                IsAvailable = false,
                Message = MissingReviewsMessage,
                Variants = variants
            };
        }

        if (!context.FetchGoogleReviews)
        {
            return new FirstContactReportAvailability
            {
                PlaceId = placeId,
                RunId = runId,
                BusinessName = context.BusinessName,
                IsAvailable = false,
                Message = MissingReviewsMessage,
                Variants = variants
            };
        }

        var requiredPlaceIds = context.RequiredPlaceIds;
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var reviewCounts = (await conn.QueryAsync<ReviewCountRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  COUNT(1) AS ReviewCount
FROM dbo.PlaceReview
WHERE PlaceId IN @PlaceIds
  AND COALESCE(ReviewTimestampUtc, LastSeenUtc) IS NOT NULL
GROUP BY PlaceId;", new { PlaceIds = requiredPlaceIds }, cancellationToken: ct)))
            .ToDictionary(x => x.PlaceId, x => x.ReviewCount, StringComparer.OrdinalIgnoreCase);

        var allParticipantsHaveReviews = requiredPlaceIds.All(id => reviewCounts.TryGetValue(id, out var count) && count > 0);
        if (!allParticipantsHaveReviews)
        {
            return new FirstContactReportAvailability
            {
                PlaceId = placeId,
                RunId = runId,
                BusinessName = context.BusinessName,
                IsAvailable = false,
                Message = MissingReviewsMessage,
                Variants = variants
            };
        }

        return new FirstContactReportAvailability
        {
            PlaceId = placeId,
            RunId = runId,
            BusinessName = context.BusinessName,
            IsAvailable = true,
            Message = string.Empty,
            Variants = variants
        };
    }

    public async Task<FirstContactReportViewModel?> BuildFirstContactReportAsync(string placeId, long runId, FirstContactReportVariant variant, CancellationToken ct)
    {
        var availability = await GetFirstContactAvailabilityAsync(placeId, runId, ct);
        if (!availability.IsAvailable)
            return null;

        var context = await LoadFirstContactContextAsync(placeId, runId, ct);
        if (context is null)
            return null;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var metrics = await LoadParticipantMetricsAsync(conn, context, ct);
        var events = await LoadReviewEventsAsync(conn, context.RequiredPlaceIds, ct);

        var searchVolume = await LoadEstimatedSearchVolumeAsync(conn, context.CategoryId, context.TownId, ct);
        var settings = await adminSettingsService.GetAsync(ct);
        var currentEstimated = CalculateEstimatedMapPackClicks(context.CurrentPosition, searchVolume, settings);
        var estimatedAt3 = CalculateEstimatedMapPackClicks(3, searchVolume, settings);
        var estimatedAt1 = CalculateEstimatedMapPackClicks(1, searchVolume, settings);
        var expectedTo3 = Math.Max(0, estimatedAt3 - currentEstimated);
        var expectedTo1 = Math.Max(0, estimatedAt1 - currentEstimated);

        var conservativeMultiplier = ClampMultiplier(reportsOptions.Value.ConservativeMultiplier, 0.1m, 0.99m, 0.75m);
        var upsideMultiplier = ClampMultiplier(reportsOptions.Value.UpsideMultiplier, 1.01m, 3m, 1.25m);

        var model = new FirstContactReportViewModel
        {
            PlaceId = placeId,
            RunId = runId,
            Variant = variant,
            VariantLabel = GetVariantLabel(variant),
            ShowRawMetrics = false,
            Version = Math.Max(1, reportsOptions.Value.FirstContactVersion),
            BusinessName = context.BusinessName,
            TownName = context.TownName,
            PrimaryCategory = context.PrimaryCategory,
            RunDateUtc = context.RunDateUtc,
            CurrentPosition = context.CurrentPosition,
            MoveToPosition3 = BuildRange(expectedTo3, conservativeMultiplier, upsideMultiplier),
            MoveToPosition1 = BuildRange(expectedTo1, conservativeMultiplier, upsideMultiplier),
            WhyThisMatters = BuildWhyThisMatters(context.BusinessName, context.TownName, context.PrimaryCategory),
            EstimatedMonthlySearchDemand = searchVolume,
            ConservativeMultiplier = conservativeMultiplier,
            UpsideMultiplier = upsideMultiplier,
            Competitors = context.Competitors
                .Select(x => new FirstContactCompetitorSummary
                {
                    PlaceId = x.PlaceId,
                    DisplayName = x.DisplayName,
                    RankPosition = x.RankPosition
                })
                .ToList(),
            ReviewChart = BuildReviewChart(context, events),
            StrengthSignals = BuildStrengthSignals(context, metrics),
            Outcomes = BuildOutcomes(context.TownName, context.PrimaryCategory),
            AboutKontrolit = "Kontrolit helps local businesses improve visibility, trust and conversion from high-intent local searches. We combine performance marketing and practical SEO execution to create measurable growth. Our focus is consistent lead flow, not vanity metrics.",
            CtaLine = $"Reply to this email and we'll share a short plan tailored to {context.BusinessName}.",
            BrandPrimary = NormalizeHexColor(brandOptions.Value.Primary, "#0f2a5f"),
            BrandAccent = NormalizeHexColor(brandOptions.Value.Accent, "#5b7cfa"),
            BrandDark = NormalizeHexColor(brandOptions.Value.Dark, "#0f172a"),
            BrandLight = NormalizeHexColor(brandOptions.Value.Light, "#f6f8fc"),
            ExistingPdfPath = availability.Variants.FirstOrDefault(x => x.Variant == variant)?.ExistingPdfPath
        };

        return model;
    }

    public async Task<FirstContactPdfGenerationResult> GenerateFirstContactPdfAsync(FirstContactPdfGenerationRequest request, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.PlaceId) || request.RunId <= 0)
        {
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = "Invalid report request."
            };
        }

        if (!Uri.TryCreate(request.ReportUrl, UriKind.Absolute, out _))
        {
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = "Report URL is invalid."
            };
        }

        var model = await BuildFirstContactReportAsync(request.PlaceId, request.RunId, request.Variant, ct);
        if (model is null)
        {
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = MissingReviewsMessage
            };
        }

        var contentHash = ComputeContentHash(model);
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var existing = await GetExistingReportRecordAsync(conn, request.PlaceId, request.RunId, request.Variant, model.Version, ct);
        if (existing is not null
            && !string.IsNullOrWhiteSpace(existing.ContentHash)
            && string.Equals(existing.ContentHash, contentHash, StringComparison.OrdinalIgnoreCase)
            && IsStoredPdfPathAvailable(existing.PdfPath))
        {
            return new FirstContactPdfGenerationResult
            {
                Success = true,
                Message = "PDF already up to date.",
                DownloadUrl = existing.PdfPath,
                ReportId = existing.ReportId
            };
        }

        string relativePdfPath;
        string absolutePdfPath;
        try
        {
            (relativePdfPath, absolutePdfPath) = BuildOutputPdfPath();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unable to resolve reports output directory.");
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = "Report output folder is not configured correctly."
            };
        }

        try
        {
            await RenderPdfAsync(request.ReportUrl, request.Cookies, absolutePdfPath, ct);
        }
        catch (PlaywrightException ex)
        {
            logger.LogError(ex, "Playwright PDF generation failed for PlaceId={PlaceId}, RunId={RunId}.", request.PlaceId, request.RunId);
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = "PDF generation failed. Ensure Playwright Chromium is installed on the server."
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "PDF generation failed for PlaceId={PlaceId}, RunId={RunId}.", request.PlaceId, request.RunId);
            return new FirstContactPdfGenerationResult
            {
                Success = false,
                Message = "PDF generation failed."
            };
        }

        var reportId = await UpsertReportRecordAsync(
            conn,
            request.PlaceId,
            request.RunId,
            request.Variant,
            model.Version,
            relativePdfPath,
            request.CreatedByUserId,
            contentHash,
            ct);

        return new FirstContactPdfGenerationResult
        {
            Success = true,
            Message = "PDF generated.",
            DownloadUrl = relativePdfPath,
            ReportId = reportId
        };
    }

    private async Task<FirstContactRunContext?> LoadFirstContactContextAsync(string placeId, long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var contextRow = await conn.QuerySingleOrDefaultAsync<RunContextRow>(new CommandDefinition(@"
SELECT TOP 1
  sr.SearchRunId AS RunId,
  sr.RanAtUtc AS RunDateUtc,
  sr.CategoryId,
  sr.TownId,
  sr.FetchGoogleReviews,
  town.Name AS TownName,
  cat.DisplayName AS CategoryDisplayName,
  ps.RankPosition AS CurrentPosition,
  COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), @PlaceId) AS BusinessName,
  COALESCE(NULLIF(LTRIM(RTRIM(p.PrimaryCategory)), N''), cat.DisplayName, N'Unknown') AS PrimaryCategory
FROM dbo.SearchRun sr
JOIN dbo.PlaceSnapshot ps ON ps.SearchRunId = sr.SearchRunId AND ps.PlaceId = @PlaceId
LEFT JOIN dbo.Place p ON p.PlaceId = ps.PlaceId
LEFT JOIN dbo.GbTown town ON town.TownId = sr.TownId
LEFT JOIN dbo.GoogleBusinessProfileCategory cat ON cat.CategoryId = sr.CategoryId
WHERE sr.SearchRunId = @RunId;", new { PlaceId = placeId, RunId = runId }, cancellationToken: ct));

        if (contextRow is null)
            return null;

        var competitors = (await conn.QueryAsync<CompetitorRow>(new CommandDefinition(@"
SELECT TOP 3
  ps.PlaceId,
  COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), ps.PlaceId) AS DisplayName,
  ps.RankPosition
FROM dbo.PlaceSnapshot ps
LEFT JOIN dbo.Place p ON p.PlaceId = ps.PlaceId
WHERE ps.SearchRunId = @RunId
  AND ps.PlaceId <> @PlaceId
ORDER BY ps.RankPosition ASC, ps.PlaceSnapshotId ASC;", new { RunId = runId, PlaceId = placeId }, cancellationToken: ct))).ToList();

        var existingRows = (await conn.QueryAsync<PlaceRunReportRecord>(new CommandDefinition(@"
SELECT
  ReportId,
  PlaceId,
  RunId,
  ReportType,
  Variant,
  Version,
  HtmlSnapshotPath,
  PdfPath,
  CreatedUtc,
  CreatedByUserId,
  ContentHash
FROM dbo.PlaceRunReports
WHERE PlaceId = @PlaceId
  AND RunId = @RunId
  AND ReportType = @ReportType;", new { PlaceId = placeId, RunId = runId, ReportType = FirstContactReportType }, cancellationToken: ct))).ToList();

        var target = new ParticipantContext(
            placeId,
            contextRow.BusinessName,
            contextRow.CurrentPosition,
            true);
        var competitorContexts = competitors
            .Select(x => new ParticipantContext(x.PlaceId, x.DisplayName, x.RankPosition, false))
            .ToList();

        return new FirstContactRunContext
        {
            PlaceId = placeId,
            RunId = runId,
            RunDateUtc = contextRow.RunDateUtc,
            CategoryId = contextRow.CategoryId,
            TownId = contextRow.TownId,
            TownName = string.IsNullOrWhiteSpace(contextRow.TownName) ? "Unknown Town" : contextRow.TownName,
            PrimaryCategory = contextRow.PrimaryCategory,
            BusinessName = contextRow.BusinessName,
            CurrentPosition = contextRow.CurrentPosition,
            FetchGoogleReviews = contextRow.FetchGoogleReviews,
            Target = target,
            Competitors = competitorContexts,
            ExistingReportRows = existingRows
        };
    }

    private static IReadOnlyList<ReportVariantSummary> BuildVariantSummaries(IReadOnlyList<PlaceRunReportRecord> rows)
    {
        var clientPath = rows
            .Where(x => string.Equals(x.ReportType, FirstContactReportType, StringComparison.OrdinalIgnoreCase))
            .Where(x => string.Equals(x.Variant, "ClientFacing", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(x => x.Version)
            .ThenByDescending(x => x.CreatedUtc)
            .Select(x => x.PdfPath)
            .FirstOrDefault();

        var internalPath = rows
            .Where(x => string.Equals(x.ReportType, FirstContactReportType, StringComparison.OrdinalIgnoreCase))
            .Where(x => string.Equals(x.Variant, "Internal", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(x => x.Version)
            .ThenByDescending(x => x.CreatedUtc)
            .Select(x => x.PdfPath)
            .FirstOrDefault();

        return new List<ReportVariantSummary>
        {
            new(FirstContactReportVariant.ClientFacing, "Client-facing", clientPath),
            new(FirstContactReportVariant.Internal, "Internal", internalPath)
        };
    }

    private async Task<Dictionary<string, ParticipantMetric>> LoadParticipantMetricsAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        FirstContactRunContext context,
        CancellationToken ct)
    {
        var placeIds = context.RequiredPlaceIds;
        var rows = (await conn.QueryAsync<ParticipantMetricRow>(new CommandDefinition(@"
SELECT
  p.PlaceId,
  COALESCE(NULLIF(LTRIM(RTRIM(p.DisplayName)), N''), p.PlaceId) AS DisplayName,
  COALESCE(NULLIF(LTRIM(RTRIM(p.PrimaryCategory)), N''), N'') AS PrimaryCategory,
  p.OtherCategoriesJson,
  p.PhotoCount,
  p.Description,
  COALESCE(v.ReviewsLast90, 0) AS ReviewsLast90,
  COALESCE(reviewSummary.TotalReviews, 0) AS TotalReviews,
  reviewSummary.LastReviewUtc
FROM dbo.Place p
LEFT JOIN dbo.PlaceReviewVelocityStats v ON v.PlaceId = p.PlaceId
OUTER APPLY (
  SELECT
    COUNT(1) AS TotalReviews,
    MAX(COALESCE(pr.ReviewTimestampUtc, pr.LastSeenUtc)) AS LastReviewUtc
  FROM dbo.PlaceReview pr
  WHERE pr.PlaceId = p.PlaceId
    AND COALESCE(pr.ReviewTimestampUtc, pr.LastSeenUtc) IS NOT NULL
) reviewSummary
WHERE p.PlaceId IN @PlaceIds;", new { PlaceIds = placeIds }, cancellationToken: ct))).ToList();

        var map = rows.ToDictionary(
            x => x.PlaceId,
            x =>
            {
                var secondaryCount = ParseCategoryCount(x.OtherCategoriesJson);
                var hasPrimary = !string.IsNullOrWhiteSpace(x.PrimaryCategory);
                var descriptionLength = string.IsNullOrWhiteSpace(x.Description) ? 0 : x.Description.Trim().Length;
                return new ParticipantMetric(
                    x.PlaceId,
                    x.DisplayName,
                    x.TotalReviews,
                    x.LastReviewUtc,
                    Math.Round(x.ReviewsLast90 / 3m, 2, MidpointRounding.AwayFromZero),
                    hasPrimary ? 1 + secondaryCount : secondaryCount,
                    Math.Max(0, x.PhotoCount ?? 0),
                    descriptionLength);
            },
            StringComparer.OrdinalIgnoreCase);

        foreach (var participant in placeIds)
        {
            if (!map.ContainsKey(participant))
            {
                var fallbackName = context.AllParticipants.FirstOrDefault(x => string.Equals(x.PlaceId, participant, StringComparison.OrdinalIgnoreCase))?.DisplayName
                    ?? participant;
                map[participant] = new ParticipantMetric(participant, fallbackName, 0, null, 0m, 0, 0, 0);
            }
        }

        return map;
    }

    private static int ParseCategoryCount(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return 0;
        try
        {
            var values = JsonSerializer.Deserialize<List<string>>(json);
            if (values is null)
                return 0;
            return values.Count(x => !string.IsNullOrWhiteSpace(x));
        }
        catch
        {
            return 0;
        }
    }

    private async Task<List<ReviewEventRow>> LoadReviewEventsAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        IReadOnlyList<string> placeIds,
        CancellationToken ct)
    {
        return (await conn.QueryAsync<ReviewEventRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  CAST(COALESCE(ReviewTimestampUtc, LastSeenUtc) AS date) AS EventDate,
  COUNT(1) AS ReviewCount
FROM dbo.PlaceReview
WHERE PlaceId IN @PlaceIds
  AND COALESCE(ReviewTimestampUtc, LastSeenUtc) IS NOT NULL
GROUP BY PlaceId, CAST(COALESCE(ReviewTimestampUtc, LastSeenUtc) AS date)
ORDER BY EventDate ASC;", new { PlaceIds = placeIds }, cancellationToken: ct))).ToList();
    }

    private static FirstContactChartModel BuildReviewChart(FirstContactRunContext context, IReadOnlyList<ReviewEventRow> events)
    {
        var minDate = events.Min(x => x.EventDate);
        var maxDate = events.Max(x => x.EventDate);
        var spanDays = (maxDate - minDate).TotalDays;
        var useMonthly = spanDays > 180;

        var buckets = BuildBuckets(minDate, maxDate, useMonthly);
        var byPlace = events
            .GroupBy(x => x.PlaceId, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                g => g.Key,
                g => g.OrderBy(x => x.EventDate).ToList(),
                StringComparer.OrdinalIgnoreCase);

        var series = new List<FirstContactChartSeries>(context.AllParticipants.Count);
        foreach (var participant in context.AllParticipants)
        {
            byPlace.TryGetValue(participant.PlaceId, out var rows);
            rows ??= [];

            var values = new List<int>(buckets.Count);
            var running = 0;
            var rowIndex = 0;
            foreach (var bucket in buckets)
            {
                while (rowIndex < rows.Count && rows[rowIndex].EventDate <= bucket.CutoffDate)
                {
                    running += rows[rowIndex].ReviewCount;
                    rowIndex++;
                }
                values.Add(running);
            }

            series.Add(new FirstContactChartSeries
            {
                Name = participant.IsTarget ? "You" : participant.DisplayName,
                IsYou = participant.IsTarget,
                Values = values
            });
        }

        return new FirstContactChartModel
        {
            GranularityLabel = useMonthly ? "Monthly" : "Weekly",
            Labels = buckets.Select(x => x.Label).ToList(),
            Series = series
        };
    }

    private static List<ChartBucket> BuildBuckets(DateTime minDate, DateTime maxDate, bool useMonthly)
    {
        var buckets = new List<ChartBucket>();
        if (useMonthly)
        {
            var cursor = new DateTime(minDate.Year, minDate.Month, 1, 0, 0, 0, DateTimeKind.Utc);
            var final = new DateTime(maxDate.Year, maxDate.Month, 1, 0, 0, 0, DateTimeKind.Utc);
            while (cursor <= final)
            {
                var end = new DateTime(cursor.Year, cursor.Month, DateTime.DaysInMonth(cursor.Year, cursor.Month), 0, 0, 0, DateTimeKind.Utc);
                buckets.Add(new ChartBucket(
                    end.ToString("MMM yyyy", CultureInfo.InvariantCulture),
                    end));
                cursor = cursor.AddMonths(1);
            }
            return buckets;
        }

        var offsetToMonday = ((int)minDate.DayOfWeek + 6) % 7;
        var weekStart = minDate.Date.AddDays(-offsetToMonday);
        while (weekStart <= maxDate.Date)
        {
            var weekEnd = weekStart.AddDays(6);
            buckets.Add(new ChartBucket(
                weekStart.ToString("dd MMM", CultureInfo.InvariantCulture),
                weekEnd));
            weekStart = weekStart.AddDays(7);
        }

        return buckets;
    }

    private static IReadOnlyList<FirstContactStrengthSignal> BuildStrengthSignals(
        FirstContactRunContext context,
        IReadOnlyDictionary<string, ParticipantMetric> metrics)
    {
        var byReviewVolume = metrics.ToDictionary(x => x.Key, x => (decimal?)x.Value.TotalReviews, StringComparer.OrdinalIgnoreCase);
        var byRecency = metrics.ToDictionary(
            x => x.Key,
            x => x.Value.LastReviewUtc.HasValue
                ? (decimal?)Math.Max(0, (DateTime.UtcNow.Date - x.Value.LastReviewUtc.Value.Date).TotalDays)
                : null,
            StringComparer.OrdinalIgnoreCase);
        var byVelocity = metrics.ToDictionary(x => x.Key, x => (decimal?)x.Value.ReviewsPer30Days, StringComparer.OrdinalIgnoreCase);
        var byCategory = metrics.ToDictionary(x => x.Key, x => (decimal?)x.Value.CategoryCompletenessScore, StringComparer.OrdinalIgnoreCase);
        var byPhotos = metrics.ToDictionary(x => x.Key, x => (decimal?)x.Value.PhotoCount, StringComparer.OrdinalIgnoreCase);
        var byDescription = metrics.ToDictionary(x => x.Key, x => (decimal?)DescriptionBand(x.Value.DescriptionLength), StringComparer.OrdinalIgnoreCase);

        var yourMetric = metrics[context.PlaceId];

        return new List<FirstContactStrengthSignal>
        {
            new()
            {
                Key = "review-volume",
                Label = "Review Volume",
                Strength = ReportScoring.ScoreRelative(byReviewVolume, context.PlaceId),
                Impact = "Higher review volume supports trust and can improve click preference in the map pack.",
                RawValue = $"{yourMetric.TotalReviews} reviews"
            },
            new()
            {
                Key = "review-recency",
                Label = "Review Recency",
                Strength = ReportScoring.ScoreRelative(byRecency, context.PlaceId, lowerIsBetter: true),
                Impact = "Recent reviews signal profile freshness and sustained customer demand.",
                RawValue = yourMetric.LastReviewUtc.HasValue
                    ? $"{Math.Max(0, (DateTime.UtcNow.Date - yourMetric.LastReviewUtc.Value.Date).TotalDays):N0} days since last review"
                    : "No recent review date"
            },
            new()
            {
                Key = "review-velocity",
                Label = "Review Velocity",
                Strength = ReportScoring.ScoreRelative(byVelocity, context.PlaceId),
                Impact = "Steady review velocity tends to support visibility consistency against local competitors.",
                RawValue = $"{yourMetric.ReviewsPer30Days.ToString("0.##", CultureInfo.InvariantCulture)} reviews / 30 days"
            },
            new()
            {
                Key = "category-completeness",
                Label = "Category Completeness",
                Strength = ReportScoring.ScoreRelative(byCategory, context.PlaceId),
                Impact = "Broader and accurate category coverage strengthens matching for relevant local searches.",
                RawValue = $"{yourMetric.CategoryCompletenessScore} category signals"
            },
            new()
            {
                Key = "photos",
                Label = "Photos",
                Strength = ReportScoring.ScoreRelative(byPhotos, context.PlaceId),
                Impact = "Richer image coverage can increase profile engagement when users compare options.",
                RawValue = $"{yourMetric.PhotoCount} photos"
            },
            new()
            {
                Key = "description",
                Label = "Business Description",
                Strength = ReportScoring.ScoreRelative(byDescription, context.PlaceId),
                Impact = "A complete description improves relevance cues and profile context in competitive results.",
                RawValue = yourMetric.DescriptionLength <= 0 ? "Not present" : $"{yourMetric.DescriptionLength} characters"
            }
        };
    }

    private static int DescriptionBand(int length)
    {
        if (length <= 0) return 0;
        if (length < 120) return 1;
        if (length < 300) return 2;
        return 3;
    }

    private static IReadOnlyList<string> BuildOutcomes(string townName, string primaryCategory)
    {
        return new List<string>
        {
            $"Increase relevance signals for {primaryCategory} searches in {townName}.",
            "Strengthen trust signals that influence map-pack visibility.",
            "Improve engagement signals and reduce ranking volatility.",
            "Increase qualified map-pack clicks from local intent searches."
        };
    }

    private static string BuildWhyThisMatters(string businessName, string townName, string primaryCategory)
    {
        return $"{businessName} is already visible for {primaryCategory} in {townName}, but most attention concentrates in the top map-pack positions. Even modest ranking movement can create a meaningful increase in monthly high-intent visits. This estimate highlights the scale of opportunity so decisions can be made with clear commercial context.";
    }

    private static FirstContactOpportunityEstimate BuildRange(int expected, decimal conservativeMultiplier, decimal upsideMultiplier)
    {
        if (expected <= 0)
        {
            return new FirstContactOpportunityEstimate
            {
                Conservative = 0,
                Expected = 0,
                Upside = 0
            };
        }

        return new FirstContactOpportunityEstimate
        {
            Conservative = Math.Max(0, (int)Math.Round(expected * conservativeMultiplier, MidpointRounding.AwayFromZero)),
            Expected = Math.Max(0, expected),
            Upside = Math.Max(0, (int)Math.Round(expected * upsideMultiplier, MidpointRounding.AwayFromZero))
        };
    }

    private static decimal ClampMultiplier(decimal value, decimal min, decimal max, decimal fallback)
    {
        if (value < min || value > max)
            return fallback;
        return value;
    }

    private static string GetVariantLabel(FirstContactReportVariant variant)
        => variant == FirstContactReportVariant.ClientFacing ? "Client-facing" : "Internal";

    private async Task<int> LoadEstimatedSearchVolumeAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        string categoryId,
        long townId,
        CancellationToken ct)
    {
        var keywordCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @TownId;", new { CategoryId = categoryId, TownId = townId }, cancellationToken: ct));
        if (keywordCount <= 0)
            return 0;

        var values = (await conn.QueryAsync<WeightedSearchVolumeRow>(new CommandDefinition(@"
SELECT TOP 12
  CAST(SUM(
    CAST(m.SearchVolume AS decimal(18,4)) *
    CASE
      WHEN k.KeywordType = 1 THEN 1.0
      WHEN k.KeywordType IN (3, 4) THEN 0.7
      ELSE 0.0
    END
  ) AS decimal(18,4)) AS WeightedSearchVolume
FROM dbo.CategoryLocationSearchVolume m
JOIN dbo.CategoryLocationKeyword k ON k.Id = m.CategoryLocationKeywordId
WHERE k.CategoryId = @CategoryId
  AND k.LocationId = @TownId
  AND k.NoData = 0
  AND k.KeywordType IN (1,3,4)
GROUP BY m.[Year], m.[Month]
ORDER BY m.[Year] DESC, m.[Month] DESC;", new { CategoryId = categoryId, TownId = townId }, cancellationToken: ct))).ToList();

        if (values.Count == 0)
            return 0;
        var average = values.Average(x => x.WeightedSearchVolume);
        return Math.Max(0, (int)Math.Round(average, MidpointRounding.AwayFromZero));
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

    private static string NormalizeHexColor(string? value, string fallback)
    {
        var candidate = (value ?? string.Empty).Trim();
        if (candidate.Length is 4 or 7 && candidate.StartsWith('#'))
            return candidate;
        return fallback;
    }

    private static string ComputeContentHash(FirstContactReportViewModel model)
    {
        var canonical = new
        {
            model.PlaceId,
            model.RunId,
            variant = model.Variant.ToString(),
            model.Version,
            model.BusinessName,
            model.TownName,
            model.PrimaryCategory,
            model.RunDateUtc,
            model.CurrentPosition,
            model.MoveToPosition3,
            model.MoveToPosition1,
            model.EstimatedMonthlySearchDemand,
            chart = model.ReviewChart.Series.Select(x => new { x.Name, x.Values }),
            model.StrengthSignals,
            model.Outcomes
        };

        var json = JsonSerializer.Serialize(canonical);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(json));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private async Task<PlaceRunReportRecord?> GetExistingReportRecordAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        string placeId,
        long runId,
        FirstContactReportVariant variant,
        int version,
        CancellationToken ct)
    {
        return await conn.QuerySingleOrDefaultAsync<PlaceRunReportRecord>(new CommandDefinition(@"
SELECT TOP 1
  ReportId,
  PlaceId,
  RunId,
  ReportType,
  Variant,
  Version,
  HtmlSnapshotPath,
  PdfPath,
  CreatedUtc,
  CreatedByUserId,
  ContentHash
FROM dbo.PlaceRunReports
WHERE PlaceId = @PlaceId
  AND RunId = @RunId
  AND ReportType = @ReportType
  AND Variant = @Variant
  AND Version = @Version;", new
        {
            PlaceId = placeId,
            RunId = runId,
            ReportType = FirstContactReportType,
            Variant = VariantToStorage(variant),
            Version = version
        }, cancellationToken: ct));
    }

    private static string VariantToStorage(FirstContactReportVariant variant)
        => variant == FirstContactReportVariant.ClientFacing ? "ClientFacing" : "Internal";

    private async Task<long> UpsertReportRecordAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        string placeId,
        long runId,
        FirstContactReportVariant variant,
        int version,
        string relativePdfPath,
        int? createdByUserId,
        string contentHash,
        CancellationToken ct)
    {
        return await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
MERGE dbo.PlaceRunReports AS target
USING (SELECT @PlaceId AS PlaceId, @RunId AS RunId, @ReportType AS ReportType, @Variant AS Variant, @Version AS Version) AS source
ON target.PlaceId = source.PlaceId
 AND target.RunId = source.RunId
 AND target.ReportType = source.ReportType
 AND target.Variant = source.Variant
 AND target.Version = source.Version
WHEN MATCHED THEN
  UPDATE SET
    PdfPath = @PdfPath,
    HtmlSnapshotPath = NULL,
    CreatedUtc = SYSUTCDATETIME(),
    CreatedByUserId = @CreatedByUserId,
    ContentHash = @ContentHash
WHEN NOT MATCHED THEN
  INSERT (PlaceId, RunId, ReportType, Variant, Version, HtmlSnapshotPath, PdfPath, CreatedUtc, CreatedByUserId, ContentHash)
  VALUES (@PlaceId, @RunId, @ReportType, @Variant, @Version, NULL, @PdfPath, SYSUTCDATETIME(), @CreatedByUserId, @ContentHash)
OUTPUT INSERTED.ReportId;", new
        {
            PlaceId = placeId,
            RunId = runId,
            ReportType = FirstContactReportType,
            Variant = VariantToStorage(variant),
            Version = version,
            PdfPath = relativePdfPath,
            CreatedByUserId = createdByUserId,
            ContentHash = contentHash
        }, cancellationToken: ct));
    }

    private (string RelativePath, string AbsolutePath) BuildOutputPdfPath()
    {
        var relativeDirectory = NormalizeOutputRelativeDirectory(reportsOptions.Value.PdfOutputRelativeDirectory);
        var webRoot = webHostEnvironment.WebRootPath;
        if (string.IsNullOrWhiteSpace(webRoot))
            throw new InvalidOperationException("Web root path is unavailable.");

        var absoluteDirectory = Path.GetFullPath(Path.Combine(webRoot, relativeDirectory.TrimStart('/').Replace('/', Path.DirectorySeparatorChar)));
        var rootFullPath = Path.GetFullPath(webRoot).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (!absoluteDirectory.StartsWith(rootFullPath + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(absoluteDirectory, rootFullPath, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Invalid report output path.");
        }

        Directory.CreateDirectory(absoluteDirectory);

        var fileName = $"{GenerateFileStem(8)}.pdf";
        var absolutePath = Path.Combine(absoluteDirectory, fileName);
        var relativePath = $"{relativeDirectory.TrimEnd('/')}/{fileName}";
        return (relativePath, absolutePath);
    }

    private static string NormalizeOutputRelativeDirectory(string? raw)
    {
        var normalized = (raw ?? string.Empty).Trim();
        if (normalized.Length == 0)
            normalized = "/site-assets/reports";
        normalized = normalized.Replace('\\', '/');
        if (!normalized.StartsWith('/'))
            normalized = "/" + normalized;
        if (normalized.Contains("..", StringComparison.Ordinal))
            normalized = "/site-assets/reports";
        return normalized.TrimEnd('/');
    }

    private static string GenerateFileStem(int bytesLength)
    {
        var bytes = RandomNumberGenerator.GetBytes(Math.Max(1, bytesLength));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private bool IsStoredPdfPathAvailable(string? relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            return false;
        var webRoot = webHostEnvironment.WebRootPath;
        if (string.IsNullOrWhiteSpace(webRoot))
            return false;

        var normalized = relativePath.Replace('\\', '/');
        if (!normalized.StartsWith('/'))
            normalized = "/" + normalized;
        if (normalized.Contains("..", StringComparison.Ordinal))
            return false;

        var absolute = Path.GetFullPath(Path.Combine(webRoot, normalized.TrimStart('/').Replace('/', Path.DirectorySeparatorChar)));
        var rootFullPath = Path.GetFullPath(webRoot).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var isUnderRoot = absolute.StartsWith(rootFullPath + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)
            || string.Equals(absolute, rootFullPath, StringComparison.OrdinalIgnoreCase);
        if (!isUnderRoot)
            return false;

        return File.Exists(absolute);
    }

    private static async Task RenderPdfAsync(string reportUrl, IReadOnlyList<ReportCookie> cookies, string outputPath, CancellationToken ct)
    {
        using var playwright = await Playwright.CreateAsync();
        await using var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = true
        });

        var context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            IgnoreHTTPSErrors = true
        });

        if (cookies.Count > 0)
        {
            await context.AddCookiesAsync(cookies.Select(ToPlaywrightCookie).ToArray());
        }

        var page = await context.NewPageAsync();
        await page.GotoAsync(reportUrl, new PageGotoOptions
        {
            WaitUntil = WaitUntilState.NetworkIdle,
            Timeout = 120_000
        });
        await page.WaitForFunctionAsync("() => window.__reportReady === true", new PageWaitForFunctionOptions
        {
            Timeout = 120_000
        });

        await page.PdfAsync(new PagePdfOptions
        {
            Path = outputPath,
            Format = "A4",
            PrintBackground = true,
            Margin = new Margin
            {
                Top = "12mm",
                Bottom = "12mm",
                Left = "12mm",
                Right = "12mm"
            }
        });

        await context.CloseAsync();
    }

    private static Cookie ToPlaywrightCookie(ReportCookie cookie)
    {
        return new Cookie
        {
            Name = cookie.Name,
            Value = cookie.Value,
            Domain = cookie.Domain,
            Path = string.IsNullOrWhiteSpace(cookie.Path) ? "/" : cookie.Path,
            HttpOnly = cookie.HttpOnly,
            Secure = cookie.Secure,
            Expires = cookie.ExpiresUtc.HasValue
                ? (float)(cookie.ExpiresUtc.Value - DateTime.UnixEpoch).TotalSeconds
                : -1,
            SameSite = cookie.SameSite.Equals("Strict", StringComparison.OrdinalIgnoreCase)
                ? SameSiteAttribute.Strict
                : cookie.SameSite.Equals("None", StringComparison.OrdinalIgnoreCase)
                    ? SameSiteAttribute.None
                    : SameSiteAttribute.Lax
        };
    }

    private sealed class FirstContactRunContext
    {
        public string PlaceId { get; init; } = string.Empty;
        public long RunId { get; init; }
        public DateTime RunDateUtc { get; init; }
        public string CategoryId { get; init; } = string.Empty;
        public long TownId { get; init; }
        public string TownName { get; init; } = string.Empty;
        public string PrimaryCategory { get; init; } = string.Empty;
        public string BusinessName { get; init; } = string.Empty;
        public int CurrentPosition { get; init; }
        public bool FetchGoogleReviews { get; init; }
        public ParticipantContext Target { get; init; } = new("", "", 0, true);
        public IReadOnlyList<ParticipantContext> Competitors { get; init; } = [];
        public IReadOnlyList<PlaceRunReportRecord> ExistingReportRows { get; init; } = [];
        public IReadOnlyList<ParticipantContext> AllParticipants => [Target, .. Competitors];
        public IReadOnlyList<string> RequiredPlaceIds => AllParticipants.Select(x => x.PlaceId).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    private sealed record ParticipantContext(
        string PlaceId,
        string DisplayName,
        int RankPosition,
        bool IsTarget);

    private sealed record RunContextRow(
        long RunId,
        DateTime RunDateUtc,
        string CategoryId,
        long TownId,
        bool FetchGoogleReviews,
        string TownName,
        string CategoryDisplayName,
        int CurrentPosition,
        string BusinessName,
        string PrimaryCategory);

    private sealed record CompetitorRow(
        string PlaceId,
        string DisplayName,
        int RankPosition);

    private sealed record ReviewCountRow(string PlaceId, int ReviewCount);

    private sealed class ParticipantMetricRow
    {
        public string PlaceId { get; set; } = string.Empty;
        public string DisplayName { get; set; } = string.Empty;
        public string PrimaryCategory { get; set; } = string.Empty;
        public string? OtherCategoriesJson { get; set; }
        public int? PhotoCount { get; set; }
        public string? Description { get; set; }
        public decimal ReviewsLast90 { get; set; }
        public int TotalReviews { get; set; }
        public DateTime? LastReviewUtc { get; set; }
    }

    private sealed record ParticipantMetric(
        string PlaceId,
        string DisplayName,
        int TotalReviews,
        DateTime? LastReviewUtc,
        decimal ReviewsPer30Days,
        int CategoryCompletenessScore,
        int PhotoCount,
        int DescriptionLength);

    private sealed record ReviewEventRow(string PlaceId, DateTime EventDate, int ReviewCount);

    private sealed record ChartBucket(string Label, DateTime CutoffDate);

    private sealed record WeightedSearchVolumeRow(decimal WeightedSearchVolume);
}
