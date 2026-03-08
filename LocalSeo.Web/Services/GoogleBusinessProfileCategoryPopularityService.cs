using System.Text;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IGoogleBusinessProfileCategoryPopularityService
{
    GoogleBusinessProfileCategoryPopularityApplyResult MatchCandidates(IReadOnlyList<GoogleBusinessProfileCategoryMatchCandidate> candidates);
}

public sealed class GoogleBusinessProfileCategoryPopularityService : IGoogleBusinessProfileCategoryPopularityService
{
    private static readonly string[] CuratedPopularCategoryNames =
    [
        "Plumber",
        "Electrician",
        "Gas Engineer",
        "Heating Contractor",
        "Boiler Installation Service",
        "Drainage Service",
        "Locksmith",
        "Handyman",
        "Roofing Contractor",
        "Painter",
        "Plasterer",
        "Carpenter",
        "Dentist",
        "Dental Clinic",
        "Doctor / General Practitioner",
        "Physiotherapist",
        "Chiropractor",
        "Optician / Optometrist",
        "Pharmacy",
        "Podiatrist",
        "Osteopath",
        "Private Medical Clinic",
        "Hair Salon",
        "Hairdresser",
        "Barber Shop",
        "Beauty Salon",
        "Nail Salon",
        "Tanning Salon",
        "Massage Therapist",
        "Laser Hair Removal Service",
        "Cosmetic Clinic",
        "Tattoo Shop",
        "Restaurant",
        "Cafe",
        "Coffee Shop",
        "Takeaway Restaurant",
        "Pizza Restaurant",
        "Chinese Restaurant",
        "Indian Restaurant",
        "Fish and Chips Takeaway",
        "Pub",
        "Bar",
        "Estate Agent",
        "Letting Agent",
        "Property Management Company",
        "Surveyor",
        "Mortgage Broker",
        "Architect",
        "Interior Designer",
        "Kitchen Remodeler",
        "Bathroom Remodeler",
        "Flooring Contractor",
        "Car Repair and Maintenance",
        "Auto Repair Shop",
        "MOT Test Centre",
        "Tyre Shop",
        "Car Dealer",
        "Used Car Dealer",
        "Car Wash",
        "Car Body Shop",
        "Florist",
        "Butcher Shop",
        "Bakery",
        "Convenience Store",
        "Furniture Store",
        "Garden Centre",
        "Pet Store",
        "Hardware Store",
        "Accountant",
        "Accounting Firm",
        "Solicitor",
        "Marketing Agency",
        "Web Designer",
        "Graphic Designer",
        "Printing Service",
        "Sign Shop"
    ];

    private static readonly IReadOnlyDictionary<string, string[]> CuratedAliasMap =
        new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["Doctor / General Practitioner"] = ["Doctor", "General Practitioner", "GP"],
            ["Optician / Optometrist"] = ["Optician", "Optometrist"],
            ["Barber Shop"] = ["Barber"],
            ["Fish and Chips Takeaway"] = ["Fish & Chips Takeaway", "Fish and Chips Restaurant"],
            ["MOT Test Centre"] = ["MOT Testing Service", "MOT Centre"],
            ["Tyre Shop"] = ["Tire Shop"],
            ["Property Management Company"] = ["Property Management"],
            ["Takeaway Restaurant"] = ["Takeout Restaurant"],
            ["Garden Centre"] = ["Garden Center"],
            ["Sign Shop"] = ["Signwriter and Manufacturer"]
        };

    public GoogleBusinessProfileCategoryPopularityApplyResult MatchCandidates(IReadOnlyList<GoogleBusinessProfileCategoryMatchCandidate> candidates)
    {
        ArgumentNullException.ThrowIfNull(candidates);

        var candidateIdsByExactName = BuildIndex(candidates, static candidate => NormalizeExact(candidate.DisplayName));
        var candidateIdsByNormalizedName = BuildIndex(candidates, static candidate => NormalizeForMatch(candidate.DisplayName));

        var matchedCategoryIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var matchedSourceNames = new List<string>();
        var unmatchedSourceNames = new List<string>();

        foreach (var sourceName in CuratedPopularCategoryNames)
        {
            var matchedIds = FindMatches(sourceName, candidateIdsByExactName, candidateIdsByNormalizedName);
            if (matchedIds.Count == 0)
            {
                unmatchedSourceNames.Add(sourceName);
                continue;
            }

            matchedSourceNames.Add(sourceName);
            foreach (var categoryId in matchedIds)
                matchedCategoryIds.Add(categoryId);
        }

        return new GoogleBusinessProfileCategoryPopularityApplyResult(
            MatchedSourceCount: matchedSourceNames.Count,
            MatchedCategoryCount: matchedCategoryIds.Count,
            UpdatedCategoryCount: 0,
            MatchedCategoryIds: matchedCategoryIds.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray(),
            MatchedSourceNames: matchedSourceNames,
            UnmatchedSourceNames: unmatchedSourceNames);
    }

    private static Dictionary<string, List<string>> BuildIndex(
        IReadOnlyList<GoogleBusinessProfileCategoryMatchCandidate> candidates,
        Func<GoogleBusinessProfileCategoryMatchCandidate, string> keySelector)
    {
        var index = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var candidate in candidates)
        {
            var key = keySelector(candidate);
            if (string.IsNullOrWhiteSpace(key))
                continue;

            if (!index.TryGetValue(key, out var ids))
            {
                ids = [];
                index[key] = ids;
            }

            ids.Add(candidate.CategoryId);
        }

        return index;
    }

    private static IReadOnlyList<string> FindMatches(
        string sourceName,
        IReadOnlyDictionary<string, List<string>> exactIndex,
        IReadOnlyDictionary<string, List<string>> normalizedIndex)
    {
        if (TryMatch(sourceName, exactIndex, normalizedIndex, out var directMatches))
            return directMatches;

        if (CuratedAliasMap.TryGetValue(sourceName, out var aliases))
        {
            foreach (var alias in aliases)
            {
                if (TryMatch(alias, exactIndex, normalizedIndex, out var aliasMatches))
                    return aliasMatches;
            }
        }

        return [];
    }

    private static bool TryMatch(
        string value,
        IReadOnlyDictionary<string, List<string>> exactIndex,
        IReadOnlyDictionary<string, List<string>> normalizedIndex,
        out IReadOnlyList<string> matches)
    {
        var exact = NormalizeExact(value);
        if (exact.Length > 0 && exactIndex.TryGetValue(exact, out var exactMatches) && exactMatches.Count > 0)
        {
            matches = exactMatches;
            return true;
        }

        var normalized = NormalizeForMatch(value);
        if (normalized.Length > 0 && normalizedIndex.TryGetValue(normalized, out var normalizedMatches) && normalizedMatches.Count > 0)
        {
            matches = normalizedMatches;
            return true;
        }

        matches = [];
        return false;
    }

    private static string NormalizeExact(string? value)
        => (value ?? string.Empty).Trim();

    private static string NormalizeForMatch(string? value)
    {
        var raw = NormalizeExact(value).ToLowerInvariant();
        if (raw.Length == 0)
            return string.Empty;

        raw = raw.Replace("&", " and ", StringComparison.Ordinal);
        var builder = new StringBuilder(raw.Length);
        var lastWasSpace = false;

        foreach (var ch in raw)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(ch);
                lastWasSpace = false;
                continue;
            }

            if (!char.IsWhiteSpace(ch))
            {
                if (ch is '/' or '-' or ',' or '.' or '\'' or '"' or '(' or ')' or ':')
                {
                    if (!lastWasSpace && builder.Length > 0)
                    {
                        builder.Append(' ');
                        lastWasSpace = true;
                    }

                    continue;
                }

                continue;
            }

            if (!lastWasSpace && builder.Length > 0)
            {
                builder.Append(' ');
                lastWasSpace = true;
            }
        }

        return builder.ToString().Trim();
    }
}
