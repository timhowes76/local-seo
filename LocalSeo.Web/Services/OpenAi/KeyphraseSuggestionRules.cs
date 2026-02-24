using System.Text;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public static class KeyphraseSuggestionRules
{
    public static string Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var builder = new StringBuilder(value.Length);
        foreach (var ch in value)
            builder.Append(char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : ' ');

        return CompactSpaces(builder.ToString());
    }

    public static bool ContainsLocationToken(string? keyword, string requiredLocationName)
    {
        var normalizedKeyword = Normalize(keyword);
        var normalizedLocation = Normalize(requiredLocationName);
        if (string.IsNullOrWhiteSpace(normalizedKeyword) || string.IsNullOrWhiteSpace(normalizedLocation))
            return false;

        return $" {normalizedKeyword} ".Contains($" {normalizedLocation} ", StringComparison.Ordinal);
    }

    public static bool IsMainTerm(string? keyword, string mainKeyword)
        => string.Equals(Normalize(keyword), Normalize(mainKeyword), StringComparison.Ordinal);

    public static IReadOnlyList<KeyphraseSuggestionItem> NormalizeAndFilterSuggestions(
        IEnumerable<KeyphraseSuggestionItem>? source,
        string requiredLocationName,
        string mainKeyword,
        int maxSuggestions)
    {
        var max = Math.Clamp(maxSuggestions, 5, 100);
        var rows = new List<KeyphraseSuggestionItem>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var raw in source ?? [])
        {
            var keyword = (raw.Keyword ?? string.Empty).Trim();
            if (keyword.Length == 0)
                continue;

            if (keyword.Length > 255)
                keyword = keyword[..255].Trim();
            if (keyword.Length == 0)
                continue;

            if (raw.KeywordType is not (CategoryLocationKeywordTypes.Modifier or CategoryLocationKeywordTypes.Adjacent))
                continue;
            if (!ContainsLocationToken(keyword, requiredLocationName))
                continue;
            if (IsMainTerm(keyword, mainKeyword))
                continue;

            var normalized = Normalize(keyword);
            if (normalized.Length == 0 || !seen.Add(normalized))
                continue;

            rows.Add(new KeyphraseSuggestionItem
            {
                Keyword = keyword,
                KeywordType = raw.KeywordType,
                Confidence = decimal.Clamp(raw.Confidence, 0m, 1m)
            });

            if (rows.Count >= max)
                break;
        }

        return rows;
    }

    public static string BuildMainKeyword(string categoryDisplayName, string townName)
        => $"{CompactSpaces(categoryDisplayName)} {CompactSpaces(townName)}".Trim();

    private static string CompactSpaces(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        return string.Join(' ', value.Split(' ', StringSplitOptions.RemoveEmptyEntries));
    }
}
