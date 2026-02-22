namespace LocalSeo.Web.Services;

using LocalSeo.Web.Models;

public static class ReportScoring
{
    public static StrengthLevel ScoreRelative(
        IReadOnlyDictionary<string, decimal?> valuesByKey,
        string targetKey,
        bool lowerIsBetter = false)
    {
        if (!valuesByKey.TryGetValue(targetKey, out var targetValue) || !targetValue.HasValue)
            return StrengthLevel.Unknown;

        var available = valuesByKey
            .Where(x => x.Value.HasValue)
            .Select(x => new KeyValuePair<string, decimal>(x.Key, x.Value!.Value))
            .ToList();

        if (available.Count == 0)
            return StrengthLevel.Unknown;
        if (available.Count == 1)
            return StrengthLevel.Ok;

        var ordered = lowerIsBetter
            ? available.OrderBy(x => x.Value).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase).ToList()
            : available.OrderByDescending(x => x.Value).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase).ToList();

        var rank = ordered.FindIndex(x => string.Equals(x.Key, targetKey, StringComparison.OrdinalIgnoreCase));
        if (rank < 0)
            return StrengthLevel.Unknown;

        var denominator = Math.Max(1m, ordered.Count - 1m);
        var percentile = (ordered.Count - 1m - rank) / denominator;
        if (percentile >= 0.67m)
            return StrengthLevel.Strong;
        if (percentile >= 0.34m)
            return StrengthLevel.Ok;
        return StrengthLevel.Weak;
    }
}
