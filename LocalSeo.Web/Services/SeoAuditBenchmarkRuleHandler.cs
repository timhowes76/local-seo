using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed class BenchmarkSeoAuditRuleHandler : ISeoAuditRuleHandler
{
    private static readonly HashSet<string> BuildingTokens = new(StringComparer.Ordinal)
    {
        "apartments",
        "barn",
        "building",
        "business",
        "centre",
        "center",
        "court",
        "farm",
        "floor",
        "house",
        "hub",
        "innovation",
        "industrial",
        "mill",
        "office",
        "park",
        "room",
        "suite",
        "unit",
        "works",
        "yard"
    };

    private static readonly HashSet<string> StreetTokens = new(StringComparer.Ordinal)
    {
        "alley",
        "arcade",
        "avenue",
        "bank",
        "boulevard",
        "broadway",
        "chase",
        "circle",
        "close",
        "cl",
        "common",
        "court",
        "crescent",
        "drive",
        "dr",
        "estate",
        "gardens",
        "gate",
        "green",
        "grove",
        "hill",
        "lane",
        "ln",
        "mews",
        "parade",
        "parkway",
        "path",
        "place",
        "pl",
        "rd",
        "road",
        "row",
        "square",
        "st",
        "street",
        "terrace",
        "way"
    };

    public bool CanEvaluate(SeoAuditRuleDefinition rule)
    {
        return string.Equals(rule.RuleMode, SeoAuditRuleModes.Benchmark, StringComparison.OrdinalIgnoreCase);
    }

    public SeoAuditEvaluationResult Evaluate(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return rule.RuleKey switch
        {
            SeoAuditRuleKeys.KeywordsInBusinessTitle => EvaluateKeywordsInBusinessTitle(rule, context),
            SeoAuditRuleKeys.PrimaryCategoryMatchesRun => EvaluatePrimaryCategoryMatchesRun(rule, context),
            SeoAuditRuleKeys.PhysicalAddressMatchesSearchTown => EvaluatePhysicalAddressMatchesSearchTown(rule, context),
            _ => BuildNotApplicable(rule, "No evaluator is configured for this benchmark rule key.")
        };
    }

    private static SeoAuditEvaluationResult EvaluateKeywordsInBusinessTitle(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var normalizedTitle = KeyphraseSuggestionRules.Normalize(context.DisplayName);
        var normalizedTown = KeyphraseSuggestionRules.Normalize(context.SourceTownName);
        var serviceVariants = BuildServicePhraseVariants(context.SourceKeyword, context.SourceCategoryId);
        if (string.IsNullOrWhiteSpace(normalizedTitle))
            return BuildNotApplicable(rule, "Business title is not available.");
        if (serviceVariants.Count == 0 || string.IsNullOrWhiteSpace(normalizedTown))
            return BuildNotApplicable(rule, "Run keyword or town name is not available.");

        var titleText = $" {normalizedTitle} ";
        var serviceTokens = serviceVariants
            .SelectMany(variant => variant.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Where(IsStrongServiceToken)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (serviceTokens.Count == 0)
            return BuildNotApplicable(rule, "No searchable keyword tokens are available for this run.");

        var matchedServiceTokens = serviceTokens
            .Where(token => titleText.Contains($" {token} ", StringComparison.Ordinal))
            .ToList();
        var missingServiceTokens = serviceTokens
            .Where(token => !matchedServiceTokens.Contains(token, StringComparer.Ordinal))
            .ToList();
        var hasServicePhraseMatch = serviceVariants.Any(variant => titleText.Contains($" {variant} ", StringComparison.Ordinal));
        var hasTownMatch = titleText.Contains($" {normalizedTown} ", StringComparison.Ordinal);
        var expectedSummary = string.Join(" | ", serviceVariants
            .Select(variant => $"{variant} {normalizedTown}".Trim())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal));
        var matchedSummaryParts = new List<string>();
        if (matchedServiceTokens.Count > 0)
            matchedSummaryParts.Add($"Service terms: {string.Join(", ", matchedServiceTokens)}");
        if (hasTownMatch)
            matchedSummaryParts.Add($"Town: {normalizedTown}");
        var matchedSummary = matchedSummaryParts.Count == 0 ? "Matched: none" : $"Matched: {string.Join(" | ", matchedSummaryParts)}";

        var missingSummaryParts = new List<string>();
        if (missingServiceTokens.Count > 0)
            missingSummaryParts.Add($"Service terms: {string.Join(", ", missingServiceTokens)}");
        if (!hasTownMatch)
            missingSummaryParts.Add($"Town: {normalizedTown}");
        var missingSummary = missingSummaryParts.Count == 0 ? "Missing: none" : $"Missing: {string.Join(" | ", missingSummaryParts)}";

        if (hasServicePhraseMatch)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Pass,
                0,
                matchedSummary,
                expectedSummary,
                missingSummary,
                hasTownMatch
                    ? "Business title shows a good keyword match for this run, including a close service phrase and the town."
                    : "Business title shows a good keyword match for this run because it contains a close service phrase.");
        }

        if (matchedServiceTokens.Count > 0 || hasTownMatch)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Warning,
                rule.WarningScoreImpact,
                matchedSummary,
                expectedSummary,
                missingSummary,
                "Business title shows a partial keyword match for this run.");
        }

        return BuildResult(
            rule,
            SeoAuditStatuses.Fail,
            rule.FailScoreImpact,
            "Matched: none",
            expectedSummary,
            missingSummary,
            "Business title does not match the run keyword or town terms.");
    }

    private static SeoAuditEvaluationResult EvaluatePrimaryCategoryMatchesRun(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var normalizedPrimaryCategory = KeyphraseSuggestionRules.Normalize(context.PrimaryCategory);
        var normalizedCategoryIdLabel = NormalizeCategoryIdLabel(context.SourceCategoryId);
        var normalizedKeyword = KeyphraseSuggestionRules.Normalize(context.SourceKeyword);
        if (string.IsNullOrWhiteSpace(normalizedPrimaryCategory))
            return BuildNotApplicable(rule, "Primary category is not available.");
        if (string.IsNullOrWhiteSpace(normalizedKeyword) && string.IsNullOrWhiteSpace(normalizedCategoryIdLabel))
            return BuildNotApplicable(rule, "Run category is not available.");

        if (string.Equals(normalizedPrimaryCategory, normalizedKeyword, StringComparison.Ordinal)
            || string.Equals(normalizedPrimaryCategory, normalizedCategoryIdLabel, StringComparison.Ordinal))
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Pass,
                0,
                context.PrimaryCategory,
                context.SourceKeyword,
                "Exact match",
                "Primary category matches the category targeted by this run.");
        }

        return BuildResult(
            rule,
            SeoAuditStatuses.Fail,
            rule.FailScoreImpact,
            context.PrimaryCategory,
            context.SourceKeyword ?? HumanizeCategoryId(context.SourceCategoryId),
            "Mismatch",
            "Primary category does not match the category targeted by this run.");
    }

    private static SeoAuditEvaluationResult EvaluatePhysicalAddressMatchesSearchTown(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var normalizedAddress = KeyphraseSuggestionRules.Normalize(context.FormattedAddress);
        var normalizedTown = KeyphraseSuggestionRules.Normalize(context.SourceTownName);
        if (string.IsNullOrWhiteSpace(normalizedAddress))
            return BuildNotApplicable(rule, "Formatted address is not available.");
        if (string.IsNullOrWhiteSpace(normalizedTown))
            return BuildNotApplicable(rule, "Run town is not available.");

        var segments = SplitAddressSegments(context.FormattedAddress);
        var townSegmentIndex = FindTownSegmentIndex(segments, normalizedTown);
        if (townSegmentIndex < 0)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Fail,
                rule.FailScoreImpact,
                context.FormattedAddress,
                context.SourceTownName,
                "Town not found in address",
                "Physical address does not include the town targeted by this run.");
        }

        var earlierLocality = FindEarlierLocalitySegment(segments, townSegmentIndex);
        var laterLocality = segments
            .Skip(townSegmentIndex + 1)
            .FirstOrDefault(segment => IsLikelyLocalitySegment(segment, allowPostcode: true) && ContainsPostcode(segment.NormalizedValue));
        if (earlierLocality is null && laterLocality is null)
        {
            return BuildResult(
                rule,
                SeoAuditStatuses.Pass,
                0,
                context.FormattedAddress,
                context.SourceTownName,
                "Primary locality matches search town",
                "Physical address appears to be located in the town targeted by this run.");
        }

        var conflictingLocality = earlierLocality?.RawValue ?? laterLocality?.RawValue ?? "Another locality";
        return BuildResult(
            rule,
            SeoAuditStatuses.Fail,
            rule.FailScoreImpact,
            context.FormattedAddress,
            context.SourceTownName,
            $"Different locality: {conflictingLocality}",
            "Physical address includes the searched town as a postal area, but the locality appears to be elsewhere.");
    }

    private static List<string> BuildServicePhraseVariants(string? sourceKeyword, string? sourceCategoryId)
    {
        var sourcePhrases = new[]
            {
                KeyphraseSuggestionRules.Normalize(sourceKeyword),
                NormalizeCategoryIdLabel(sourceCategoryId)
            }
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (sourcePhrases.Count == 0)
            return [];

        var variants = new HashSet<string>(StringComparer.Ordinal);
        foreach (var phrase in sourcePhrases)
        {
            var tokens = phrase
                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .ToArray();
            if (tokens.Length == 0)
                continue;

            foreach (var variantTokens in ExpandVariantTokens(tokens))
            {
                var variant = string.Join(' ', variantTokens).Trim();
                if (variant.Length > 0)
                    variants.Add(variant);
            }
        }

        return variants
            .OrderByDescending(value => value.Length)
            .ThenBy(value => value, StringComparer.Ordinal)
            .ToList();
    }

    private static IEnumerable<string[]> ExpandVariantTokens(IReadOnlyList<string> tokens)
    {
        var choices = tokens
            .Select(GetTokenVariants)
            .ToArray();
        foreach (var combination in ExpandVariantTokensCore(choices, 0, new string[tokens.Count]))
            yield return combination;
    }

    private static IEnumerable<string[]> ExpandVariantTokensCore(IReadOnlyList<string[]> choices, int index, string[] current)
    {
        if (index >= choices.Count)
        {
            yield return (string[])current.Clone();
            yield break;
        }

        foreach (var choice in choices[index])
        {
            current[index] = choice;
            foreach (var result in ExpandVariantTokensCore(choices, index + 1, current))
                yield return result;
        }
    }

    private static string[] GetTokenVariants(string token)
    {
        return token switch
        {
            "web" => ["web", "website"],
            "website" => ["website", "web"],
            "designer" => ["designer", "design"],
            "design" => ["design", "designer"],
            "hairdresser" => ["hairdresser", "hairdressers"],
            "hairdressers" => ["hairdressers", "hairdresser"],
            _ => [token]
        };
    }

    private static bool IsStrongServiceToken(string token)
    {
        return (token.Length >= 4 || token is "web")
            && token is not "agency"
            && token is not "creative"
            && token is not "digital"
            && token is not "group"
            && token is not "media"
            && token is not "studio";
    }

    private static List<AddressSegment> SplitAddressSegments(string? formattedAddress)
    {
        return (formattedAddress ?? string.Empty)
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(raw => new AddressSegment(raw, KeyphraseSuggestionRules.Normalize(raw)))
            .Where(segment => !string.IsNullOrWhiteSpace(segment.NormalizedValue))
            .ToList();
    }

    private static int FindTownSegmentIndex(IReadOnlyList<AddressSegment> segments, string normalizedTown)
    {
        return segments
            .Select((segment, index) => new
            {
                index,
                score = ScoreTownSegment(segment, normalizedTown)
            })
            .Where(x => x.score > int.MinValue)
            .OrderByDescending(x => x.score)
            .ThenBy(x => x.index)
            .Select(x => x.index)
            .FirstOrDefault(-1);
    }

    private static AddressSegment? FindEarlierLocalitySegment(IReadOnlyList<AddressSegment> segments, int townSegmentIndex)
    {
        for (var index = 0; index < townSegmentIndex; index++)
        {
            var segment = segments[index];
            if (!IsLikelyLocalitySegment(segment, allowPostcode: false))
                continue;

            var laterSegments = segments
                .Skip(index + 1)
                .Take(townSegmentIndex - index - 1)
                .ToList();
            if (laterSegments.Any(later => HasStreetOrBuildingToken(later.NormalizedValue)))
                continue;

            return segment;
        }

        return null;
    }

    private static int ScoreTownSegment(AddressSegment segment, string normalizedTown)
    {
        if (!ContainsWholePhrase(segment.NormalizedValue, normalizedTown))
            return int.MinValue;

        var score = 0;
        if (string.Equals(segment.NormalizedValue, normalizedTown, StringComparison.Ordinal))
            score += 5;
        if (ContainsPostcode(segment.NormalizedValue))
            score += 4;
        if (!HasStreetOrBuildingToken(segment.NormalizedValue))
            score += 2;

        var trimmedRaw = segment.RawValue.TrimStart();
        if (trimmedRaw.Length > 0 && char.IsDigit(trimmedRaw[0]))
            score -= 3;
        if (HasStreetOrBuildingToken(segment.NormalizedValue))
            score -= 2;

        return score;
    }

    private static bool IsLikelyLocalitySegment(AddressSegment segment, bool allowPostcode)
    {
        var normalized = segment.NormalizedValue;
        if (normalized.Length == 0 || normalized is "uk" or "united kingdom")
            return false;
        if (!allowPostcode && ContainsPostcode(normalized))
            return false;

        var tokens = normalized
            .Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .ToArray();
        if (tokens.Length == 0)
            return false;
        if (tokens.Length == 1 && tokens[0].Length <= 2)
            return false;
        if (HasStreetOrBuildingToken(normalized))
            return false;
        if (char.IsDigit(segment.RawValue.TrimStart()[0]))
            return false;

        return true;
    }

    private static bool HasStreetOrBuildingToken(string normalizedSegment)
    {
        return normalizedSegment
            .Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Any(token => StreetTokens.Contains(token) || BuildingTokens.Contains(token));
    }

    private static bool ContainsWholePhrase(string text, string phrase)
    {
        return $" {text} ".Contains($" {phrase} ", StringComparison.Ordinal);
    }

    private static bool ContainsPostcode(string value)
    {
        var compact = value.Replace(" ", string.Empty, StringComparison.Ordinal);
        return compact.Length >= 5
            && compact.Any(char.IsDigit)
            && compact.Any(char.IsLetter);
    }

    private static string NormalizeCategoryIdLabel(string? categoryId)
        => KeyphraseSuggestionRules.Normalize(HumanizeCategoryId(categoryId));

    private static string? HumanizeCategoryId(string? categoryId)
    {
        if (string.IsNullOrWhiteSpace(categoryId))
            return null;

        var normalized = string.Join(' ',
            categoryId
                .Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries)
                .Select(token => char.ToUpperInvariant(token[0]) + token[1..].ToLowerInvariant()));

        return normalized.Length == 0 ? null : normalized;
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

    private static string? NormalizeNullable(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private sealed record AddressSegment(string RawValue, string NormalizedValue);
}
