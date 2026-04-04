using System.Text.RegularExpressions;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed partial class WebsiteBenchmarkSeoAuditRuleHandler(IWebsiteClassifier websiteClassifier) : ISeoAuditRuleHandler
{
    private static readonly Regex UkPostcodeRegex = new(@"\b(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly HashSet<string> SupportedRuleKeys =
    [
        SeoAuditRuleKeys.HtmlNapMatchesGbpNap,
        SeoAuditRuleKeys.KeywordsInLandingPageTitleTag,
        SeoAuditRuleKeys.KeywordsInLandingPageHeadings,
        SeoAuditRuleKeys.HomepageNicheFocus,
        SeoAuditRuleKeys.HomepageTopicalKeywordRelevance,
        SeoAuditRuleKeys.HomepageInternalLinking,
        SeoAuditRuleKeys.KeywordsInInternalLinkAnchorText,
        SeoAuditRuleKeys.WebsiteUsesHttpsByDefault,
        SeoAuditRuleKeys.KeywordsInDomainName
    ];

    public bool CanEvaluate(SeoAuditRuleDefinition rule)
    {
        return string.Equals(rule.RuleMode, SeoAuditRuleModes.Benchmark, StringComparison.OrdinalIgnoreCase)
            && SupportedRuleKeys.Contains(rule.RuleKey);
    }

    public SeoAuditEvaluationResult Evaluate(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        return rule.RuleKey switch
        {
            SeoAuditRuleKeys.HtmlNapMatchesGbpNap => EvaluateHomepageNapMatch(rule, context),
            SeoAuditRuleKeys.KeywordsInLandingPageTitleTag => EvaluateHomepageTitleKeywordMatch(rule, context),
            SeoAuditRuleKeys.KeywordsInLandingPageHeadings => EvaluateHomepageHeadingKeywordMatch(rule, context),
            SeoAuditRuleKeys.HomepageNicheFocus => EvaluateHomepageNicheFocus(rule, context),
            SeoAuditRuleKeys.HomepageTopicalKeywordRelevance => EvaluateHomepageTopicalKeywordRelevance(rule, context),
            SeoAuditRuleKeys.HomepageInternalLinking => EvaluateHomepageInternalLinking(rule, context),
            SeoAuditRuleKeys.KeywordsInInternalLinkAnchorText => EvaluateHomepageAnchorTextKeywordMatch(rule, context),
            SeoAuditRuleKeys.WebsiteUsesHttpsByDefault => EvaluateHomepageHttps(rule, context),
            SeoAuditRuleKeys.KeywordsInDomainName => EvaluateDomainKeywordMatch(rule, context),
            _ => BuildNotApplicable(rule, "No evaluator is configured for this website benchmark rule.")
        };
    }

    private SeoAuditEvaluationResult EvaluateHomepageNapMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var homepage = availability.HomepageAudit;
        var gbpName = NormalizeNullable(context.DisplayName);
        var gbpPhone = NormalizePhone(context.NationalPhoneNumber);
        var gbpAddress = NormalizeNullable(context.FormattedAddress);
        var gbpPostcode = ExtractPostcode(context.FormattedAddress);
        var gbpTown = NormalizeNullable(context.SourceTownName);

        var phoneMatch = gbpPhone is not null && homepage.PhoneNumbers.Any(number => PhonesMatch(gbpPhone, number));
        var businessNameMatch = gbpName is not null && homepage.BusinessNames.Any(name => NamesLookRelated(gbpName, name));
        var postcodeMatch = gbpPostcode is not null && homepage.Postcodes.Any(postcode => string.Equals(NormalizePostcode(postcode), gbpPostcode, StringComparison.OrdinalIgnoreCase));
        var townMatch = gbpTown is not null && homepage.CityNames.Any(city => ContainsWholePhrase(city, gbpTown));
        var matchedAddressTokens = CountMatchedAddressTokens(gbpAddress, homepage.PostalAddresses);
        var strongAddressMatch = postcodeMatch && matchedAddressTokens >= 1;
        var partialAddressMatch = postcodeMatch || matchedAddressTokens >= 2 || townMatch;

        var matchedSignals = new List<string>();
        if (strongAddressMatch)
            matchedSignals.Add("address");
        else if (partialAddressMatch)
            matchedSignals.Add("partial address");
        if (phoneMatch)
            matchedSignals.Add("phone");
        if (businessNameMatch)
            matchedSignals.Add("business name");

        var actualValue = matchedSignals.Count == 0
            ? "Homepage signals matched: none"
            : $"Homepage signals matched: {string.Join(", ", matchedSignals)}";
        var expectedValue = BuildNapExpectedValue(context);
        var gapValue = BuildNapGapValue(strongAddressMatch, phoneMatch, businessNameMatch);

        if (strongAddressMatch && (phoneMatch || businessNameMatch || townMatch))
        {
            return BuildResult(rule, SeoAuditStatuses.Pass, 0, actualValue, expectedValue, gapValue,
                "Homepage NAP matches the GBP profile strongly enough to support consistency.");
        }

        if (partialAddressMatch || phoneMatch || businessNameMatch)
        {
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, actualValue, expectedValue, gapValue,
                "Homepage NAP only partially matches the GBP profile.");
        }

        return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, actualValue, expectedValue, gapValue,
            "Homepage NAP does not provide a meaningful match to the GBP profile.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageTitleKeywordMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var titleTag = availability.HomepageAudit.TitleTag;
        return EvaluateKeywordMatch(rule, context, titleTag, titleTag, "title tag",
            "Homepage title tag does not match the run keyword strongly enough.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageHeadingKeywordMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var homepage = availability.HomepageAudit;
        var headingTexts = new List<string>();
        AppendIfPresent(headingTexts, homepage.H1Text);
        headingTexts.AddRange(homepage.H2Texts);
        headingTexts.AddRange(homepage.H3Texts);
        var actualValue = headingTexts.Count == 0
            ? "No homepage headings were stored."
            : string.Join(" | ", headingTexts.Take(4));

        return EvaluateKeywordMatch(rule, context, string.Join(" ", headingTexts), actualValue, "headings",
            "Homepage headings do not match the run keyword strongly enough.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageNicheFocus(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var homepage = availability.HomepageAudit;
        var locationTerms = new HashSet<string>(homepage.LocationKeywords.SelectMany(Tokenize), StringComparer.OrdinalIgnoreCase);
        var brandTerms = new HashSet<string>(homepage.BrandNames.SelectMany(Tokenize), StringComparer.OrdinalIgnoreCase);
        var weightedTopics = BuildWeightedTopicScores(homepage, locationTerms, brandTerms);
        if (weightedTopics.Count == 0)
        {
            return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact,
                "No dominant homepage service topics were detected.",
                "One clearly dominant homepage niche or service cluster.",
                "No strong niche signal",
                "Homepage content does not show a clear niche focus.");
        }

        var totalWeight = weightedTopics.Sum(x => x.Value);
        var orderedTopics = weightedTopics.OrderByDescending(x => x.Value).ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase).ToList();
        var topTopic = orderedTopics[0];
        var focusShare = totalWeight <= 0 ? 0m : topTopic.Value / totalWeight;
        var actualValue = $"Dominant homepage topic: {topTopic.Key} ({focusShare:P0})";
        var gapValue = $"Next topics: {string.Join(", ", orderedTopics.Skip(1).Take(3).Select(x => x.Key))}";

        if (focusShare >= 0.45m)
            return BuildResult(rule, SeoAuditStatuses.Pass, 0, actualValue, "A homepage with one clearly dominant niche/topic family.", gapValue, "Homepage content is strongly focused on a specific niche.");

        if (focusShare >= 0.28m)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, actualValue, "A homepage with one clearly dominant niche/topic family.", gapValue, "Homepage content shows some niche focus, but the message is still mixed.");

        return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, actualValue, "A homepage with one clearly dominant niche/topic family.", gapValue, "Homepage content is too broad to show a clear niche focus.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageTopicalKeywordRelevance(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var homepage = availability.HomepageAudit;
        var combinedCorpus = string.Join(" ", homepage.ServiceKeywords
            .Concat(homepage.ServiceTownCombinations)
            .Concat(homepage.H2Texts)
            .Concat(homepage.H3Texts)
            .Concat(homepage.InternalAnchorTexts));
        var actualValue = homepage.ServiceKeywords.Count == 0
            ? "No homepage service keywords were stored."
            : $"Homepage service keywords: {string.Join(", ", homepage.ServiceKeywords.Take(8))}";

        return EvaluateKeywordMatch(rule, context, combinedCorpus, actualValue, "homepage content",
            "Homepage content does not appear strongly relevant to the run keyword.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageInternalLinking(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var homepage = availability.HomepageAudit;
        var internalLinkCount = homepage.InternalLinkCount.GetValueOrDefault();
        var servicePageLinkCount = homepage.ServicePageLinkCount.GetValueOrDefault();
        var actualValue = $"Homepage internal links: {internalLinkCount}; homepage service-page links: {servicePageLinkCount}";
        var expectedValue = "Homepage should link internally to key service pages, not just general pages.";

        if (internalLinkCount >= 6 && servicePageLinkCount >= 3)
            return BuildResult(rule, SeoAuditStatuses.Pass, 0, actualValue, expectedValue, "Strong homepage internal linking", "Homepage internal linking provides a good service-page navigation signal.");

        if (internalLinkCount >= 3 && servicePageLinkCount >= 1)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, actualValue, expectedValue, "Homepage linking is present but limited", "Homepage internal linking exists, but service-page linking could be stronger.");

        return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, actualValue, expectedValue, "Very limited homepage internal linking", "Homepage internal linking is too weak to support clear service-page discovery.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageAnchorTextKeywordMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var anchorTexts = availability.HomepageAudit.InternalAnchorTexts;
        var actualValue = anchorTexts.Count == 0
            ? "No homepage internal anchor text was stored."
            : string.Join(" | ", anchorTexts.Take(6));

        return EvaluateKeywordMatch(rule, context, string.Join(" ", anchorTexts), actualValue, "internal anchor text",
            "Homepage internal anchor text does not match the run keyword strongly enough.");
    }

    private SeoAuditEvaluationResult EvaluateHomepageHttps(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: true);
        if (!availability.IsAvailable || availability.HomepageAudit is null)
            return BuildNotApplicable(rule, availability.Reason ?? "Homepage analysis is not available.");

        var scheme = NormalizeNullable(availability.HomepageAudit.PageScheme);
        if (string.Equals(scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return BuildResult(rule, SeoAuditStatuses.Pass, 0, "Homepage resolved over HTTPS.", "Homepage should use HTTPS by default.", "None", "Homepage uses HTTPS by default.");
        }

        return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact,
            string.IsNullOrWhiteSpace(scheme) ? "Homepage scheme was not detected." : $"Homepage scheme: {scheme}",
            "Homepage should use HTTPS by default.",
            "Homepage is not resolving over HTTPS",
            "Homepage does not use HTTPS by default.");
    }

    private SeoAuditEvaluationResult EvaluateDomainKeywordMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context)
    {
        var availability = GetWebsiteAvailability(context, requireHomepageAudit: false);
        if (!availability.IsAvailable || string.IsNullOrWhiteSpace(availability.WebsiteUrl))
            return BuildNotApplicable(rule, availability.Reason ?? "Website URL is not available.");

        var domainText = ExtractDomainKeywordText(availability.WebsiteUrl);
        if (string.IsNullOrWhiteSpace(domainText))
            return BuildNotApplicable(rule, "Domain name could not be parsed from the website URL.");

        return EvaluateKeywordMatch(rule, context, domainText, domainText, "domain name",
            "Domain name does not match the run keyword strongly enough.");
    }

    private SeoAuditEvaluationResult EvaluateKeywordMatch(SeoAuditRuleDefinition rule, PlaceAuditContext context, string? corpusValue, string? actualValue, string corpusLabel, string failSummary)
    {
        var normalizedTown = NormalizeNullable(context.SourceTownName);
        var serviceVariants = BuildServicePhraseVariants(context.SourceKeyword, context.SourceCategoryId);
        if (serviceVariants.Count == 0 || string.IsNullOrWhiteSpace(normalizedTown))
            return BuildNotApplicable(rule, "Run keyword or town is not available.");

        var serviceTokens = serviceVariants
            .SelectMany(variant => variant.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Where(IsStrongServiceToken)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (serviceTokens.Count == 0)
            return BuildNotApplicable(rule, "No searchable keyword tokens are available for this run.");

        var normalizedCorpus = KeyphraseSuggestionRules.Normalize(corpusValue);
        var corpusText = string.IsNullOrWhiteSpace(normalizedCorpus) ? string.Empty : $" {normalizedCorpus} ";
        var matchedServiceTokens = serviceTokens.Where(token => corpusText.Contains($" {token} ", StringComparison.Ordinal)).ToList();
        var missingServiceTokens = serviceTokens.Where(token => !matchedServiceTokens.Contains(token, StringComparer.Ordinal)).ToList();
        var hasTownMatch = corpusText.Contains($" {normalizedTown} ", StringComparison.Ordinal);
        var expectedValue = string.Join(" | ", serviceVariants.Select(variant => $"{variant} {normalizedTown}".Trim()).Distinct(StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal));
        var gapValue = missingServiceTokens.Count == 0 && hasTownMatch
            ? "Missing: none"
            : $"Missing: {(missingServiceTokens.Count == 0 ? "none" : string.Join(", ", missingServiceTokens))}{(hasTownMatch ? string.Empty : $"{(missingServiceTokens.Count == 0 ? string.Empty : " | ")}Town: {normalizedTown}")}";

        if (matchedServiceTokens.Count == serviceTokens.Count && hasTownMatch)
            return BuildResult(rule, SeoAuditStatuses.Pass, 0, NormalizeNullable(actualValue), expectedValue, gapValue, $"Homepage {corpusLabel} shows a strong match to the run keyword and town.");

        if (matchedServiceTokens.Count > 0 || hasTownMatch)
            return BuildResult(rule, SeoAuditStatuses.Warning, rule.WarningScoreImpact, NormalizeNullable(actualValue), expectedValue, gapValue, $"Homepage {corpusLabel} shows a partial match to the run keyword.");

        return BuildResult(rule, SeoAuditStatuses.Fail, rule.FailScoreImpact, NormalizeNullable(actualValue), expectedValue, gapValue, failSummary);
    }

    private WebsiteAvailability GetWebsiteAvailability(PlaceAuditContext context, bool requireHomepageAudit)
    {
        var websiteUrl = NormalizeWebsiteUrl(context.Website?.NormalizedWebsiteUrl)
            ?? NormalizeWebsiteUrl(context.Website?.WebsiteUrl)
            ?? NormalizeWebsiteUrl(context.WebsiteUri);
        if (string.IsNullOrWhiteSpace(websiteUrl))
            return WebsiteAvailability.Unavailable("No website URL is recorded for this place.");

        var websiteType = Enum.IsDefined(context.WebsiteType) && context.WebsiteType != WebsiteType.None
            ? context.WebsiteType
            : websiteClassifier.Classify(websiteUrl);
        if (websiteType == WebsiteType.SocialProfile)
            return WebsiteAvailability.Unavailable("The recorded website is classified as a social media profile.");

        if (!requireHomepageAudit)
            return WebsiteAvailability.Available(websiteUrl, null);

        if (context.LatestHomepageAudit is not null)
            return WebsiteAvailability.Available(websiteUrl, context.LatestHomepageAudit);

        var latestFetch = context.LatestWebsiteFetch;
        if (latestFetch is not null)
        {
            if (!latestFetch.FetchCompletedUtc.HasValue)
                return WebsiteAvailability.Unavailable("Homepage fetch is still in progress.");
            if (latestFetch.HttpStatusCode is 401 or 403 or 406 or 429)
                return WebsiteAvailability.Unavailable($"Homepage fetch was blocked by the target site (HTTP {latestFetch.HttpStatusCode}).");
            return WebsiteAvailability.Unavailable(string.IsNullOrWhiteSpace(latestFetch.ErrorMessage)
                ? "Homepage fetch failed and no stored homepage analysis is available."
                : latestFetch.ErrorMessage!);
        }

        return WebsiteAvailability.Unavailable("Homepage fetch has not been run yet.");
    }

    private static Dictionary<string, decimal> BuildWeightedTopicScores(PlaceWebsiteHomepageAuditContext homepage, ISet<string> locationTerms, ISet<string> brandTerms)
    {
        var scores = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        AddTopicScores(scores, homepage.ServiceKeywords, 3m, locationTerms, brandTerms);
        AddTopicScores(scores, homepage.ServiceTownCombinations, 2m, locationTerms, brandTerms);
        AddTopicScores(scores, homepage.InternalAnchorTexts, 1m, locationTerms, brandTerms);
        AddTopicScores(scores, homepage.H2Texts, 1m, locationTerms, brandTerms);
        AddTopicScores(scores, homepage.H3Texts, 0.5m, locationTerms, brandTerms);
        return scores;
    }

    private static void AddTopicScores(IDictionary<string, decimal> scores, IReadOnlyList<string> values, decimal weight, ISet<string> locationTerms, ISet<string> brandTerms)
    {
        foreach (var value in values)
        {
            var key = CanonicalTopicKey(value, locationTerms, brandTerms);
            if (key is null)
                continue;

            scores[key] = scores.TryGetValue(key, out var existingScore) ? existingScore + weight : weight;
        }
    }

    private static string? CanonicalTopicKey(string? value, ISet<string> locationTerms, ISet<string> brandTerms)
    {
        var tokens = Tokenize(value)
            .Where(token => token.Length >= 3)
            .Where(token => !locationTerms.Contains(token) && !brandTerms.Contains(token))
            .Where(token => token is not "service" and not "services" and not "homepage")
            .Select(NormalizeTopicToken)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(2)
            .ToList();
        return tokens.Count == 0 ? null : string.Join(' ', tokens);
    }

    private static string NormalizeTopicToken(string token)
    {
        return token switch
        {
            "designer" => "design",
            "designers" => "design",
            "website" => "web",
            "websites" => "web",
            "developer" => "development",
            "developers" => "development",
            _ when token.EndsWith("s", StringComparison.OrdinalIgnoreCase) && token.Length > 4 => token[..^1],
            _ => token
        };
    }

    private static int CountMatchedAddressTokens(string? formattedAddress, IReadOnlyList<string> homepagePostalAddresses)
    {
        var homepageText = KeyphraseSuggestionRules.Normalize(string.Join(" ", homepagePostalAddresses));
        if (string.IsNullOrWhiteSpace(formattedAddress) || string.IsNullOrWhiteSpace(homepageText))
            return 0;

        var addressTokens = Tokenize(formattedAddress)
            .Where(token => token.Length >= 3)
            .Where(token => !UkPostcodeRegex.IsMatch(token))
            .Where(token => token is not "road" and not "street" and not "lane" and not "close" and not "drive" and not "somerset")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        var corpus = $" {homepageText} ";
        return addressTokens.Count(token => corpus.Contains($" {token} ", StringComparison.Ordinal));
    }

    private static string BuildNapExpectedValue(PlaceAuditContext context)
    {
        var parts = new List<string>();
        AppendIfPresent(parts, context.DisplayName);
        AppendIfPresent(parts, context.NationalPhoneNumber);
        AppendIfPresent(parts, context.FormattedAddress);
        return parts.Count == 0 ? "GBP NAP signals" : string.Join(" | ", parts);
    }

    private static string BuildNapGapValue(bool addressMatch, bool phoneMatch, bool businessNameMatch)
    {
        var missing = new List<string>();
        if (!addressMatch)
            missing.Add("address");
        if (!phoneMatch)
            missing.Add("phone");
        if (!businessNameMatch)
            missing.Add("business name");
        return missing.Count == 0 ? "Missing: none" : $"Missing: {string.Join(", ", missing)}";
    }

    private static string? NormalizeWebsiteUrl(string? value)
    {
        var trimmed = NormalizeNullable(value);
        if (trimmed is null)
            return null;

        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var absolute)
            && (absolute.Scheme == Uri.UriSchemeHttp || absolute.Scheme == Uri.UriSchemeHttps))
            return absolute.ToString();

        if (Uri.TryCreate("https://" + trimmed, UriKind.Absolute, out var withHttps))
            return withHttps.ToString();

        return trimmed;
    }

    private static string? ExtractDomainKeywordText(string websiteUrl)
    {
        if (!Uri.TryCreate(websiteUrl, UriKind.Absolute, out var uri))
            return null;

        var segments = uri.Host
            .Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(segment => !string.Equals(segment, "www", StringComparison.OrdinalIgnoreCase))
            .Where(segment => !string.Equals(segment, "co", StringComparison.OrdinalIgnoreCase))
            .Where(segment => !string.Equals(segment, "uk", StringComparison.OrdinalIgnoreCase))
            .Where(segment => !string.Equals(segment, "com", StringComparison.OrdinalIgnoreCase))
            .Where(segment => !string.Equals(segment, "net", StringComparison.OrdinalIgnoreCase))
            .Where(segment => !string.Equals(segment, "org", StringComparison.OrdinalIgnoreCase))
            .ToList();
        return segments.Count == 0 ? null : KeyphraseSuggestionRules.Normalize(string.Join(' ', segments));
    }

    private static string? ExtractPostcode(string? value)
    {
        var normalized = NormalizeNullable(value);
        if (normalized is null)
            return null;

        var match = UkPostcodeRegex.Match(normalized.ToUpperInvariant());
        return match.Success ? NormalizePostcode(match.Value) : null;
    }

    private static string? NormalizePostcode(string? value)
    {
        var normalized = NormalizeNullable(value);
        return normalized?.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant();
    }

    private static string? NormalizePhone(string? value)
    {
        var digits = new string((value ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 0)
            return null;
        if (digits.StartsWith("44", StringComparison.Ordinal) && digits.Length > 10)
            digits = "0" + digits[2..];
        return digits;
    }

    private static bool PhonesMatch(string normalizedPhone, string? homepagePhone)
    {
        var candidate = NormalizePhone(homepagePhone);
        if (candidate is null)
            return false;
        if (string.Equals(normalizedPhone, candidate, StringComparison.Ordinal))
            return true;

        var left = normalizedPhone.Length > 10 ? normalizedPhone[^10..] : normalizedPhone;
        var right = candidate.Length > 10 ? candidate[^10..] : candidate;
        return string.Equals(left, right, StringComparison.Ordinal);
    }

    private static bool NamesLookRelated(string left, string right)
    {
        var normalizedLeft = NormalizeNullable(left)?.ToLowerInvariant();
        var normalizedRight = NormalizeNullable(right)?.ToLowerInvariant();
        if (normalizedLeft is null || normalizedRight is null)
            return false;

        return normalizedLeft.Contains(normalizedRight, StringComparison.Ordinal)
            || normalizedRight.Contains(normalizedLeft, StringComparison.Ordinal);
    }

    private static bool ContainsWholePhrase(string? text, string phrase)
    {
        var normalizedText = KeyphraseSuggestionRules.Normalize(text);
        var normalizedPhrase = KeyphraseSuggestionRules.Normalize(phrase);
        if (string.IsNullOrWhiteSpace(normalizedText) || string.IsNullOrWhiteSpace(normalizedPhrase))
            return false;

        return $" {normalizedText} ".Contains($" {normalizedPhrase} ", StringComparison.Ordinal);
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
            var tokens = phrase.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length == 0)
                continue;

            foreach (var variantTokens in ExpandVariantTokens(tokens))
            {
                var variant = string.Join(' ', variantTokens).Trim();
                if (variant.Length > 0)
                    variants.Add(variant);
            }
        }

        return variants.OrderByDescending(value => value.Length).ThenBy(value => value, StringComparer.Ordinal).ToList();
    }

    private static IEnumerable<string[]> ExpandVariantTokens(IReadOnlyList<string> tokens)
    {
        var choices = tokens.Select(GetTokenVariants).ToArray();
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

    private static IEnumerable<string> Tokenize(string? value)
    {
        var normalized = KeyphraseSuggestionRules.Normalize(value);
        if (string.IsNullOrWhiteSpace(normalized))
            yield break;

        foreach (var token in normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            yield return token;
    }

    private static void AppendIfPresent(ICollection<string> values, string? value)
    {
        var normalized = NormalizeNullable(value);
        if (normalized is not null)
            values.Add(normalized);
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

    private sealed record WebsiteAvailability(bool IsAvailable, string? WebsiteUrl, PlaceWebsiteHomepageAuditContext? HomepageAudit, string? Reason)
    {
        public static WebsiteAvailability Available(string websiteUrl, PlaceWebsiteHomepageAuditContext? homepageAudit)
            => new(true, websiteUrl, homepageAudit, null);

        public static WebsiteAvailability Unavailable(string reason)
            => new(false, null, null, reason);
    }
}
