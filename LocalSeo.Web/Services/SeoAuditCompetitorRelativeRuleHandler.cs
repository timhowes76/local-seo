using System.Globalization;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed class CompetitorRelativeSeoAuditRuleHandler : ISeoAuditRuleHandler
{
    public bool CanEvaluate(SeoAuditRuleDefinition rule)
    {
        return string.Equals(rule.RuleMode, SeoAuditRuleModes.CompetitorRelative, StringComparison.OrdinalIgnoreCase);
    }

    public SeoAuditEvaluationResult Evaluate(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return rule.RuleKey switch
        {
            SeoAuditRuleKeys.TownCentreDistanceRelative => EvaluateTownCentreDistanceRelative(rule, context),
            _ => BuildNotApplicable(rule, "No evaluator is configured for this competitor-relative rule key.")
        };
    }

    private static SeoAuditEvaluationResult EvaluateTownCentreDistanceRelative(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        if (!context.LastSourceSearchRunId.HasValue || context.LastSourceSearchRunId.Value <= 0)
            return BuildNotApplicable(rule, "No source run is available for distance comparison.");
        if (!context.TownCenterLat.HasValue || !context.TownCenterLng.HasValue)
            return BuildNotApplicable(rule, "Town centre coordinates are not available for this run.");
        if (!context.PlaceLat.HasValue || !context.PlaceLng.HasValue)
            return BuildNotApplicable(rule, "Place coordinates are not available for distance comparison.");

        var comparableDistances = context.ComparablePlaces
            .Select(peer => new
            {
                peer.PlaceId,
                DistanceKm = GeoDistanceCalculator.DistanceKm(context.TownCenterLat, context.TownCenterLng, peer.Lat, peer.Lng)
            })
            .Where(x => x.DistanceKm.HasValue)
            .Select(x => new DistanceRow(x.PlaceId, x.DistanceKm!.Value))
            .ToList();

        if (comparableDistances.Count < 2)
            return BuildNotApplicable(rule, "At least two places with coordinates are required for relative distance scoring.");

        var targetDistance = comparableDistances
            .FirstOrDefault(x => string.Equals(x.PlaceId, context.PlaceId, StringComparison.OrdinalIgnoreCase));
        if (targetDistance is null)
            return BuildNotApplicable(rule, "This place is not present in the comparable run set.");

        var distinctDistances = comparableDistances
            .Select(x => x.DistanceKm)
            .Distinct()
            .OrderBy(x => x)
            .ToList();
        var cohortCount = distinctDistances.Count;
        if (cohortCount <= 1)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Pass,
                0,
                $"{targetDistance.DistanceKm.ToString("0.000", CultureInfo.InvariantCulture)} km",
                "Shared nearest distance",
                $"Joint nearest of {comparableDistances.Count}",
                "All comparable places in this run are the same distance from the town centre.");
        }

        var cohortRank = distinctDistances.FindIndex(x => x == targetDistance.DistanceKm) + 1;
        var cohortSize = comparableDistances.Count(x => x.DistanceKm == targetDistance.DistanceKm);
        var rankLabel = cohortSize > 1
            ? $"Joint {ToOrdinal(cohortRank)} nearest of {comparableDistances.Count}"
            : $"{ToOrdinal(cohortRank)} nearest of {comparableDistances.Count}";
        var summaryText = $"This place is {rankLabel.ToLowerInvariant()} and is {targetDistance.DistanceKm.ToString("0.000", CultureInfo.InvariantCulture)} km from the town centre.";

        if (cohortRank == 1)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Pass,
                0,
                $"{targetDistance.DistanceKm.ToString("0.000", CultureInfo.InvariantCulture)} km",
                "Nearest cohort in run",
                rankLabel,
                summaryText);
        }

        if (cohortRank == cohortCount)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Fail,
                rule.FailScoreImpact,
                $"{targetDistance.DistanceKm.ToString("0.000", CultureInfo.InvariantCulture)} km",
                "Nearest cohort in run",
                rankLabel,
                summaryText);
        }

        var scaledImpact = (int)Math.Round(
            ((decimal)(cohortRank - 1) / (cohortCount - 1)) * rule.FailScoreImpact,
            MidpointRounding.AwayFromZero);
        scaledImpact = Math.Max(rule.WarningScoreImpact, scaledImpact);
        scaledImpact = Math.Min(Math.Max(0, rule.FailScoreImpact - 1), scaledImpact);

        return BuildResult(
            rule,
            SeoAuditStatuses.Warning,
            scaledImpact,
            $"{targetDistance.DistanceKm.ToString("0.000", CultureInfo.InvariantCulture)} km",
            "Nearest cohort in run",
            rankLabel,
            summaryText);
    }

    private static SeoAuditEvaluationResult BuildNotApplicable(SeoAuditRuleDefinition rule, string summaryText)
    {
        return BuildResult(rule, SeoAuditStatuses.NotApplicable, 0, null, null, null, summaryText);
    }

    private static SeoAuditEvaluationResult BuildResult(SeoAuditRuleDefinition rule, string status, int scoreImpactApplied, string? actualValue, string? expectedValue, string? gapValue, string summaryText)
    {
        return new SeoAuditEvaluationResult(
            rule.SeoAuditRuleId,
            rule.RuleKey,
            status,
            Math.Max(0, scoreImpactApplied),
            NormalizeNullable(actualValue),
            NormalizeNullable(expectedValue),
            NormalizeNullable(gapValue),
            summaryText.Trim(),
            NormalizeNullable(rule.WhyItMattersText),
            NormalizeNullable(rule.RecommendedActionText),
            rule.SortOrder);
    }

    private static string ToOrdinal(int value)
    {
        var absValue = Math.Abs(value);
        var suffix = (absValue % 100) switch
        {
            11 or 12 or 13 => "th",
            _ => (absValue % 10) switch
            {
                1 => "st",
                2 => "nd",
                3 => "rd",
                _ => "th"
            }
        };
        return $"{value}{suffix}";
    }

    private static string? NormalizeNullable(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private sealed record DistanceRow(string PlaceId, decimal DistanceKm);
}
