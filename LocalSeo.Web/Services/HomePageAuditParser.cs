using System.Text.Json;
using System.Text.RegularExpressions;
using AngleSharp.Dom;
using AngleSharp.Html.Parser;

namespace LocalSeo.Web.Services;

public interface IHomePageAuditParser
{
    HomePageAuditParseResult Parse(HomePageAuditParseRequest request);
}

public sealed class HomePageAuditParser : IHomePageAuditParser
{
    private static readonly Regex WordRegex = new(@"\b[\p{L}\p{N}][\p{L}\p{N}'-]*\b", RegexOptions.Compiled);
    private static readonly Regex PhoneRegex = new(@"(?:(?:\+44\s?(?:\(0\)\s?)?)|0)(?:[\d\s\-\(\)]{8,})", RegexOptions.Compiled);
    private static readonly Regex UkPostcodeRegex = new(@"\b(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex AddressWithPostcodeRegex = new(@"([A-Za-z0-9&'./\- ]{2,80}(?:,\s*[A-Za-z0-9&'./\- ]{2,80}){1,8},\s*(?:GIR\s?0AA|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}))", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex WhitespaceRegex = new(@"\s+", RegexOptions.Compiled);
    private static readonly HashSet<string> AddressIndicatorWords =
    [
        "road", "rd", "street", "st", "lane", "ln", "close", "cl", "drive", "dr", "avenue", "ave", "way", "court", "ct",
        "house", "suite", "unit", "park", "centre", "center", "building", "floor", "industrial", "estate", "innovation",
        "works", "farm", "business"
    ];
    private static readonly HashSet<string> NonCityLocationWords =
    [
        "uk", "united kingdom", "england", "scotland", "wales", "northern ireland"
    ];
    private static readonly HashSet<string> StopWords =
    [
        "a", "about", "after", "all", "and", "are", "as", "at", "be", "by", "for", "from", "get", "has", "home",
        "in", "into", "is", "it", "its", "more", "near", "of", "on", "or", "our", "page", "the", "their", "this",
        "to", "up", "we", "with", "you", "your"
    ];

    public HomePageAuditParseResult Parse(HomePageAuditParseRequest request)
    {
        var html = (request.Html ?? string.Empty).Trim();
        if (html.Length == 0)
            return new HomePageAuditParseResult();

        var parser = new HtmlParser();
        var document = parser.ParseDocument(html);
        var body = document.Body;
        var baseUri = TryCreateUri(request.FinalUrl) ?? TryCreateUri(request.RequestedUrl);
        var requestedUri = TryCreateUri(request.RequestedUrl);
        var finalUri = TryCreateUri(request.FinalUrl);
        var loweredHtml = html.ToLowerInvariant();

        var titleTag = NormalizeText(document.Title);
        var metaDescription = NormalizeText(document.QuerySelector("meta[name='description' i]")?.GetAttribute("content"));
        var canonicalUrl = NormalizeText(document.QuerySelector("link[rel~='canonical' i]")?.GetAttribute("href"));
        if (canonicalUrl is not null && baseUri is not null && Uri.TryCreate(baseUri, canonicalUrl, out var resolvedCanonical))
            canonicalUrl = resolvedCanonical.ToString();

        var robotsMeta = NormalizeText(document.QuerySelector("meta[name='robots' i]")?.GetAttribute("content"));
        var htmlLang = NormalizeText(document.DocumentElement?.GetAttribute("lang"));

        var h1Texts = GetTexts(document.QuerySelectorAll("h1"));
        var h2Texts = GetTexts(document.QuerySelectorAll("h2"));
        var h3Texts = GetTexts(document.QuerySelectorAll("h3"));

        var visibleText = NormalizeText(ExtractVisibleText(body));
        var visibleWordCount = CountWords(visibleText);
        var paragraphCount = body?.QuerySelectorAll("p").Count(x => !string.IsNullOrWhiteSpace(NormalizeText(x.TextContent))) ?? 0;
        var bulletListCount = body?.QuerySelectorAll("ul,ol").Count(x => x.QuerySelectorAll("li").Length > 0) ?? 0;
        var contentSectionCount = body?.QuerySelectorAll("main,section,article").Length ?? 0;

        var structuredData = ExtractStructuredData(document);
        var phoneNumbers = CollectPhoneNumbers(visibleText);
        var postcodes = CollectPostcodes(visibleText, structuredData.Postcodes);
        var postalAddresses = CollectPostalAddresses(document, structuredData.PostalAddresses);
        var cityCandidates = CollectCityCandidates(request.SearchLocationName, request.FormattedAddress, structuredData.CityNames, postalAddresses);
        var cityNames = cityCandidates
            .Where(x => ContainsOrdinalIgnoreCase(visibleText, x) || ContainsOrdinalIgnoreCase(titleTag, x))
            .ToList();

        var businessNames = CollectBusinessNames(request.DisplayName, titleTag, h1Texts, visibleText, structuredData.BusinessNames);
        var pageScheme = finalUri?.Scheme ?? requestedUri?.Scheme;
        var canonicalScheme = TryCreateUri(canonicalUrl)?.Scheme;
        bool? redirectsToHttps = requestedUri is null
            ? null
            : string.Equals(requestedUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
                && string.Equals(finalUri?.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
        bool? hasMixedContent = string.Equals(pageScheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)
            ? ContainsMixedContent(loweredHtml)
            : null;

        var internalLinks = CollectInternalLinks(document, baseUri, request.PrimaryCategory);
        var images = CollectImages(document, baseUri);
        var generatorMetaTag = NormalizeText(document.QuerySelector("meta[name='generator' i]")?.GetAttribute("content"));
        var detectedCms = DetectCms(generatorMetaTag, loweredHtml);
        var hasViewportMeta = document.QuerySelector("meta[name='viewport' i]") is not null;
        var hasResponsiveIndicators = hasViewportMeta || loweredHtml.Contains("@media", StringComparison.Ordinal) || loweredHtml.Contains("srcset=", StringComparison.Ordinal);
        var hasFavicon = document.QuerySelector("link[rel*='icon' i]") is not null;
        var hasCookieBanner = HasCookieBanner(document, loweredHtml);

        var headingCorpus = new List<string>();
        AppendIfPresent(headingCorpus, titleTag);
        headingCorpus.AddRange(h1Texts);
        headingCorpus.AddRange(h2Texts);
        headingCorpus.AddRange(h3Texts);
        headingCorpus.AddRange(internalLinks.AnchorTexts);
        if (!string.IsNullOrWhiteSpace(request.PrimaryCategory))
            headingCorpus.Add(request.PrimaryCategory);

        var locationKeywords = CollectLocationKeywords(request.SearchLocationName, request.FormattedAddress, postcodes, headingCorpus, visibleText);
        var exclusionWords = BuildExclusionWords(locationKeywords, businessNames, request.DisplayName);
        var serviceKeywords = ExtractServiceKeywords(headingCorpus, exclusionWords, request.PrimaryCategory);
        var serviceTownCombinations = BuildServiceTownCombinations(serviceKeywords, locationKeywords, titleTag, h1Texts, h2Texts, visibleText);
        var brandNames = CollectBrandNames(request.DisplayName, titleTag, businessNames);

        return new HomePageAuditParseResult
        {
            TitleTag = titleTag,
            TitleTagLength = titleTag?.Length,
            MetaDescription = metaDescription,
            MetaDescriptionLength = metaDescription?.Length,
            CanonicalUrl = canonicalUrl,
            RobotsMeta = robotsMeta,
            HtmlLang = htmlLang,
            H1Text = h1Texts.FirstOrDefault(),
            H1Count = h1Texts.Count,
            H2Count = h2Texts.Count,
            H3Count = h3Texts.Count,
            H2Texts = h2Texts,
            H3Texts = h3Texts,
            VisibleWordCount = visibleWordCount,
            ParagraphCount = paragraphCount,
            BulletListCount = bulletListCount,
            ContentSectionCount = contentSectionCount,
            HasPhoneNumber = phoneNumbers.Count > 0,
            PhoneNumbers = phoneNumbers,
            HasPostalAddress = postalAddresses.Count > 0,
            PostalAddresses = postalAddresses,
            HasPostcode = postcodes.Count > 0,
            Postcodes = postcodes,
            HasCityName = cityNames.Count > 0,
            CityNames = cityNames,
            HasBusinessName = businessNames.Count > 0,
            BusinessNames = businessNames,
            SchemaTypes = structuredData.SchemaTypes,
            HasLocalBusinessSchema = structuredData.HasLocalBusinessSchema,
            HasOrganizationSchema = structuredData.HasOrganizationSchema,
            HasProductSchema = structuredData.HasProductSchema,
            HasFaqSchema = structuredData.HasFaqSchema,
            HasBreadcrumbSchema = structuredData.HasBreadcrumbSchema,
            HasNapInSchema = structuredData.HasNapInSchema,
            HasGeoCoordinatesInSchema = structuredData.HasGeoCoordinatesInSchema,
            PageScheme = pageScheme,
            CanonicalScheme = canonicalScheme,
            RedirectsToHttps = redirectsToHttps,
            HasMixedContent = hasMixedContent,
            InternalLinkCount = internalLinks.InternalLinkCount,
            ServicePageLinkCount = internalLinks.ServicePageLinkCount,
            InternalAnchorTexts = internalLinks.AnchorTexts,
            ImageCount = images.ImageCount,
            ImagesMissingAltCount = images.ImagesMissingAltCount,
            ImageAltTexts = images.ImageAltTexts,
            ImageFileNames = images.ImageFileNames,
            DetectedCms = detectedCms,
            GeneratorMetaTag = generatorMetaTag,
            HasViewportMeta = hasViewportMeta,
            HasResponsiveIndicators = hasResponsiveIndicators,
            HasFavicon = hasFavicon,
            HasCookieBanner = hasCookieBanner,
            ServiceKeywords = serviceKeywords,
            LocationKeywords = locationKeywords,
            ServiceTownCombinations = serviceTownCombinations,
            BrandNames = brandNames
        };
    }

    private static StructuredDataExtraction ExtractStructuredData(IDocument document)
    {
        var extraction = new StructuredDataExtraction();
        foreach (var script in document.QuerySelectorAll("script[type='application/ld+json' i]"))
        {
            var payload = (script.TextContent ?? string.Empty).Trim();
            if (payload.Length == 0)
                continue;

            try
            {
                using var doc = JsonDocument.Parse(payload);
                VisitJsonElement(doc.RootElement, extraction);
            }
            catch (JsonException)
            {
            }
        }

        extraction.SchemaTypes = NormalizeList(extraction.SchemaTypes, 25, 100);
        extraction.PhoneNumbers = NormalizeList(extraction.PhoneNumbers, 10, 60);
        extraction.PostalAddresses = NormalizeList(extraction.PostalAddresses, 10, 300);
        extraction.Postcodes = NormalizeList(extraction.Postcodes, 10, 20);
        extraction.CityNames = NormalizeList(extraction.CityNames, 10, 120);
        extraction.BusinessNames = NormalizeList(extraction.BusinessNames, 10, 160);
        return extraction;
    }

    private static void VisitJsonElement(JsonElement element, StructuredDataExtraction extraction)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    var name = property.Name;
                    var value = property.Value;
                    if (string.Equals(name, "@type", StringComparison.OrdinalIgnoreCase) || string.Equals(name, "type", StringComparison.OrdinalIgnoreCase))
                    {
                        if (value.ValueKind == JsonValueKind.String)
                        {
                            AddSchemaType(extraction, value.GetString());
                        }
                        else if (value.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var item in value.EnumerateArray())
                                AddSchemaType(extraction, item.GetString());
                        }
                    }
                    else if (string.Equals(name, "telephone", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasNapInSchema = true;
                        if (value.ValueKind == JsonValueKind.String)
                            extraction.PhoneNumbers.Add(value.GetString() ?? string.Empty);
                    }
                    else if (string.Equals(name, "name", StringComparison.OrdinalIgnoreCase)
                             || string.Equals(name, "legalName", StringComparison.OrdinalIgnoreCase))
                    {
                        if (value.ValueKind == JsonValueKind.String)
                            extraction.BusinessNames.Add(value.GetString() ?? string.Empty);
                    }
                    else if (string.Equals(name, "streetAddress", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasNapInSchema = true;
                        if (value.ValueKind == JsonValueKind.String)
                            extraction.PostalAddresses.Add(value.GetString() ?? string.Empty);
                    }
                    else if (string.Equals(name, "postalCode", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasNapInSchema = true;
                        if (value.ValueKind == JsonValueKind.String)
                            extraction.Postcodes.Add(value.GetString() ?? string.Empty);
                    }
                    else if (string.Equals(name, "addressLocality", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasNapInSchema = true;
                        if (value.ValueKind == JsonValueKind.String)
                            extraction.CityNames.Add(value.GetString() ?? string.Empty);
                    }
                    else if (string.Equals(name, "geo", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasGeoCoordinatesInSchema = true;
                    }
                    else if (string.Equals(name, "latitude", StringComparison.OrdinalIgnoreCase)
                             || string.Equals(name, "longitude", StringComparison.OrdinalIgnoreCase))
                    {
                        extraction.HasGeoCoordinatesInSchema = true;
                    }

                    VisitJsonElement(value, extraction);
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                    VisitJsonElement(item, extraction);
                break;
        }
    }

    private static void AddSchemaType(StructuredDataExtraction extraction, string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            return;

        extraction.SchemaTypes.Add(normalized);
        if (ContainsOrdinalIgnoreCase(normalized, "LocalBusiness"))
            extraction.HasLocalBusinessSchema = true;
        if (ContainsOrdinalIgnoreCase(normalized, "Organization"))
            extraction.HasOrganizationSchema = true;
        if (ContainsOrdinalIgnoreCase(normalized, "Product"))
            extraction.HasProductSchema = true;
    }

    private static string ExtractVisibleText(IElement? root)
    {
        if (root is null)
            return string.Empty;

        var parts = new List<string>();
        CollectVisibleText(root, parts);
        return string.Join(" ", parts);
    }

    private static void CollectVisibleText(INode node, List<string> parts)
    {
        if (node is IText textNode)
        {
            var text = NormalizeText(textNode.TextContent);
            if (!string.IsNullOrWhiteSpace(text))
                parts.Add(text);
            return;
        }

        if (node is IElement element)
        {
            if (IsExcludedElement(element))
                return;
        }

        foreach (var child in node.ChildNodes)
            CollectVisibleText(child, parts);
    }

    private static bool IsExcludedElement(IElement element)
    {
        var tagName = element.TagName.ToLowerInvariant();
        if (tagName is "script" or "style" or "noscript" or "template" or "svg" or "head")
            return true;
        if (element.HasAttribute("hidden"))
            return true;
        if (string.Equals(element.GetAttribute("aria-hidden"), "true", StringComparison.OrdinalIgnoreCase))
            return true;

        var style = (element.GetAttribute("style") ?? string.Empty).ToLowerInvariant();
        return style.Contains("display:none", StringComparison.Ordinal)
            || style.Contains("visibility:hidden", StringComparison.Ordinal);
    }

    private static List<string> GetTexts(IHtmlCollection<IElement> elements)
        => NormalizeList(elements
            .Select(x => NormalizeText(x.TextContent))
            .OfType<string>()
            .ToList(), 25, 1000);

    private static List<string> CollectPhoneNumbers(string? visibleText)
    {
        var values = new List<string>();
        if (!string.IsNullOrWhiteSpace(visibleText))
        {
            foreach (Match match in PhoneRegex.Matches(visibleText))
                values.Add(match.Value);
        }

        return NormalizeList(values, 12, 60);
    }

    private static List<string> CollectPostcodes(string? visibleText, IReadOnlyList<string> schemaPostcodes)
    {
        var values = new List<string>();
        if (!string.IsNullOrWhiteSpace(visibleText))
        {
            foreach (Match match in UkPostcodeRegex.Matches(visibleText.ToUpperInvariant()))
                values.Add(match.Value.ToUpperInvariant());
        }

        values.AddRange(schemaPostcodes);
        return NormalizeList(values, 12, 20);
    }

    private static List<string> CollectPostalAddresses(IDocument document, IReadOnlyList<string> schemaPostalAddresses)
    {
        var values = new List<string>();
        foreach (var address in document.QuerySelectorAll("address"))
        {
            values.AddRange(ExtractPostalAddressSnippets(address.TextContent));
        }

        foreach (var candidate in document.QuerySelectorAll("footer,p,div,li"))
        {
            values.AddRange(ExtractPostalAddressSnippets(candidate.TextContent));
        }

        foreach (var schemaPostalAddress in schemaPostalAddresses)
            values.AddRange(ExtractPostalAddressSnippets(schemaPostalAddress));

        return DeduplicatePostalAddresses(values, 10, 220);
    }

    private static List<string> CollectCityCandidates(string? searchLocationName, string? formattedAddress, IReadOnlyList<string> schemaCityNames, IReadOnlyList<string> postalAddresses)
    {
        var values = new List<string>();
        values.AddRange(SplitLocationCandidates(searchLocationName));
        values.AddRange(SplitLocationCandidates(formattedAddress));
        foreach (var postalAddress in postalAddresses)
            values.AddRange(SplitLocationCandidates(postalAddress));
        values.AddRange(schemaCityNames);
        return NormalizeList(values.Where(IsLikelyCityName), 12, 120);
    }

    private static IEnumerable<string> SplitLocationCandidates(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            yield break;

        foreach (var segment in normalized.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (UkPostcodeRegex.IsMatch(segment.ToUpperInvariant()))
                continue;
            if (segment.Length < 2 || segment.Length > 120)
                continue;
            if (NonCityLocationWords.Contains(segment.ToLowerInvariant()))
                continue;
            yield return segment;
        }
    }

    private static List<string> CollectBusinessNames(string? displayName, string? titleTag, IReadOnlyList<string> h1Texts, string? visibleText, IReadOnlyList<string> schemaBusinessNames)
    {
        var values = new List<string>();
        var normalizedDisplayName = NormalizeText(displayName);
        if (normalizedDisplayName is not null
            && (ContainsOrdinalIgnoreCase(titleTag, normalizedDisplayName)
                || ContainsOrdinalIgnoreCase(visibleText, normalizedDisplayName)
                || h1Texts.Any(x => ContainsOrdinalIgnoreCase(x, normalizedDisplayName))))
        {
            values.Add(normalizedDisplayName);
        }

        foreach (var schemaBusinessName in schemaBusinessNames)
        {
            var normalizedSchemaBusinessName = NormalizeText(schemaBusinessName);
            if (normalizedSchemaBusinessName is null)
                continue;

            if (normalizedDisplayName is not null)
            {
                if (NamesLookRelated(normalizedDisplayName, normalizedSchemaBusinessName))
                    values.Add(normalizedDisplayName);
                continue;
            }

            if (ContainsOrdinalIgnoreCase(titleTag, normalizedSchemaBusinessName)
                || h1Texts.Any(x => ContainsOrdinalIgnoreCase(x, normalizedSchemaBusinessName)))
            {
                values.Add(normalizedSchemaBusinessName);
            }
        }

        return NormalizeList(values, 10, 160);
    }

    private static InternalLinkExtraction CollectInternalLinks(IDocument document, Uri? baseUri, string? primaryCategory)
    {
        var anchorTexts = new List<string>();
        var internalLinkCount = 0;
        var servicePageLinkCount = 0;
        var serviceHints = BuildPrimaryCategoryTerms(primaryCategory);
        var hrefHints = new[] { "service", "services", "what-we-do", "solutions", "areas", "locations" };

        foreach (var anchor in document.QuerySelectorAll("a[href]"))
        {
            var href = NormalizeText(anchor.GetAttribute("href"));
            if (href is null || href.StartsWith("#", StringComparison.Ordinal) || href.StartsWith("javascript:", StringComparison.OrdinalIgnoreCase))
                continue;
            if (href.StartsWith("mailto:", StringComparison.OrdinalIgnoreCase) || href.StartsWith("tel:", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!IsInternalLink(href, baseUri, out var resolved))
                continue;

            internalLinkCount++;
            var anchorText = NormalizeText(anchor.TextContent);
            if (anchorText is not null)
                anchorTexts.Add(anchorText);

            var comparison = $"{href} {resolved} {anchorText}".ToLowerInvariant();
            if (hrefHints.Any(comparison.Contains) || serviceHints.Any(comparison.Contains))
                servicePageLinkCount++;
        }

        return new InternalLinkExtraction(
            internalLinkCount,
            servicePageLinkCount,
            NormalizeList(anchorTexts, 30, 200));
    }

    private static bool IsInternalLink(string href, Uri? baseUri, out string? resolvedUrl)
    {
        resolvedUrl = null;
        if (baseUri is null)
            return href.StartsWith("/", StringComparison.Ordinal) || !href.Contains("://", StringComparison.Ordinal);

        if (!Uri.TryCreate(baseUri, href, out var resolved))
            return false;

        if (!string.Equals(resolved.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(resolved.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        resolvedUrl = resolved.ToString();
        return string.Equals(resolved.Host, baseUri.Host, StringComparison.OrdinalIgnoreCase);
    }

    private static ImageExtraction CollectImages(IDocument document, Uri? baseUri)
    {
        var altTexts = new List<string>();
        var fileNames = new List<string>();
        var imageCount = 0;
        var imagesMissingAltCount = 0;

        foreach (var image in document.QuerySelectorAll("img"))
        {
            imageCount++;
            var alt = NormalizeText(image.GetAttribute("alt"));
            if (alt is null)
            {
                imagesMissingAltCount++;
                altTexts.Add("(Missing alt text)");
            }
            else
            {
                altTexts.Add(alt);
            }

            var src = NormalizeText(image.GetAttribute("src")) ?? NormalizeText(image.GetAttribute("data-src"));
            fileNames.Add(ExtractFileName(src, baseUri) ?? "(Unknown image)");
        }

        return new ImageExtraction(
            imageCount,
            imagesMissingAltCount,
            NormalizeOrderedList(altTexts, 40, 200),
            NormalizeOrderedList(fileNames, 40, 120));
    }

    private static string? DetectCms(string? generatorMetaTag, string loweredHtml)
    {
        var generator = (generatorMetaTag ?? string.Empty).ToLowerInvariant();
        if (generator.Contains("kontrolit kit cms", StringComparison.Ordinal) || generator.Contains("kit cms", StringComparison.Ordinal))
            return "KIT";
        if (generator.Contains("wordpress", StringComparison.Ordinal) || loweredHtml.Contains("wp-content/", StringComparison.Ordinal))
            return "WordPress";
        if (generator.Contains("shopify", StringComparison.Ordinal) || loweredHtml.Contains("cdn.shopify.com", StringComparison.Ordinal))
            return "Shopify";
        if (generator.Contains("wix", StringComparison.Ordinal) || loweredHtml.Contains("wixstatic.com", StringComparison.Ordinal))
            return "Wix";
        if (generator.Contains("squarespace", StringComparison.Ordinal) || loweredHtml.Contains("static1.squarespace.com", StringComparison.Ordinal))
            return "Squarespace";
        if (generator.Contains("webflow", StringComparison.Ordinal) || loweredHtml.Contains("webflow.", StringComparison.Ordinal))
            return "Webflow";
        return generator.Length == 0 ? "Unknown" : "Custom";
    }

    private static bool HasCookieBanner(IDocument document, string loweredHtml)
    {
        if (loweredHtml.Contains("cookie consent", StringComparison.Ordinal)
            || loweredHtml.Contains("cookie banner", StringComparison.Ordinal)
            || loweredHtml.Contains("cookies", StringComparison.Ordinal) && loweredHtml.Contains("privacy", StringComparison.Ordinal))
        {
            return true;
        }

        return document.QuerySelector("[id*='cookie' i],[class*='cookie' i],[id*='consent' i],[class*='consent' i]") is not null;
    }

    private static List<string> CollectLocationKeywords(string? searchLocationName, string? formattedAddress, IReadOnlyList<string> postcodes, IReadOnlyList<string> headingCorpus, string? visibleText)
    {
        var values = new List<string>();
        values.AddRange(SplitLocationCandidates(searchLocationName).Where(IsLikelyLocationKeyword));
        values.AddRange(SplitLocationCandidates(formattedAddress).Where(IsLikelyLocationKeyword));
        values.AddRange(postcodes);

        var normalizedCorpus = $"{string.Join(" ", headingCorpus)} {visibleText}";
        return NormalizeList(values
            .Where(x => ContainsOrdinalIgnoreCase(normalizedCorpus, x))
            .ToList(), 12, 120);
    }

    private static HashSet<string> BuildExclusionWords(IEnumerable<string> locationKeywords, IEnumerable<string> businessNames, string? displayName)
    {
        var exclusions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in locationKeywords.Concat(businessNames))
        {
            foreach (var token in Tokenize(entry))
                exclusions.Add(token);
        }

        foreach (var token in Tokenize(displayName))
            exclusions.Add(token);

        return exclusions;
    }

    private static List<string> ExtractServiceKeywords(IReadOnlyList<string> corpus, IReadOnlySet<string> exclusions, string? primaryCategory)
    {
        var frequency = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var text in corpus)
        {
            var tokens = Tokenize(text)
                .Where(x => x.Length >= 3 && !StopWords.Contains(x) && !exclusions.Contains(x))
                .ToList();

            foreach (var token in tokens)
                frequency[token] = frequency.GetValueOrDefault(token) + 1;

            for (var i = 0; i < tokens.Count - 1; i++)
            {
                var phrase = $"{tokens[i]} {tokens[i + 1]}";
                frequency[phrase] = frequency.GetValueOrDefault(phrase) + 1;
            }
        }

        foreach (var categoryTerm in BuildPrimaryCategoryTerms(primaryCategory))
            frequency[categoryTerm] = frequency.GetValueOrDefault(categoryTerm) + 3;

        return frequency
            .OrderByDescending(x => x.Value)
            .ThenBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
            .Take(12)
            .Select(x => x.Key)
            .ToList();
    }

    private static List<string> BuildServiceTownCombinations(IReadOnlyList<string> serviceKeywords, IReadOnlyList<string> locationKeywords, string? titleTag, IReadOnlyList<string> h1Texts, IReadOnlyList<string> h2Texts, string? visibleText)
    {
        var haystack = $"{titleTag} {string.Join(" ", h1Texts)} {string.Join(" ", h2Texts)} {visibleText}";
        var values = new List<string>();
        foreach (var serviceKeyword in serviceKeywords.Take(8))
        {
            foreach (var locationKeyword in locationKeywords.Take(8))
            {
                var phrase = $"{serviceKeyword} {locationKeyword}";
                if (ContainsOrdinalIgnoreCase(haystack, phrase))
                    values.Add(phrase);
            }
        }

        return NormalizeList(values, 12, 160);
    }

    private static List<string> CollectBrandNames(string? displayName, string? titleTag, IReadOnlyList<string> businessNames)
    {
        var values = new List<string>();
        AppendIfPresent(values, displayName);
        values.AddRange(businessNames);
        if (values.Count > 0)
            return NormalizeList(values, 8, 160);

        var title = NormalizeText(titleTag);
        if (title is not null)
        {
            foreach (var segment in title.Split(['|', '-', '•'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (segment.Length is > 1 and <= 160)
                    values.Add(segment);
            }
        }

        return NormalizeList(values, 8, 160);
    }

    private static IEnumerable<string> BuildPrimaryCategoryTerms(string? primaryCategory)
    {
        var normalized = NormalizeText(primaryCategory);
        if (normalized is null)
            return [];

        var values = new List<string> { normalized };
        values.AddRange(Tokenize(normalized).Where(x => x.Length >= 3 && !StopWords.Contains(x)));
        return NormalizeList(values, 8, 120);
    }

    private static List<string> NormalizeList(IEnumerable<string> values, int maxItems, int maxLength)
    {
        return values
            .Select(x => NormalizeText(x))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(maxItems)
            .Select(x => x!.Length <= maxLength ? x : x[..maxLength])
            .ToList();
    }

    private static List<string> NormalizeOrderedList(IEnumerable<string> values, int maxItems, int maxLength)
    {
        return values
            .Select(x => NormalizeText(x))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Take(maxItems)
            .Select(x => x!.Length <= maxLength ? x : x[..maxLength])
            .ToList();
    }

    private static List<string> DeduplicatePostalAddresses(IEnumerable<string> values, int maxItems, int maxLength)
    {
        var results = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var value in values)
        {
            var normalized = NormalizeText(value);
            if (normalized is null)
                continue;

            var key = Regex.Replace(normalized.ToLowerInvariant(), @"[^a-z0-9]", string.Empty);
            if (!seen.Add(key))
                continue;

            results.Add(normalized.Length <= maxLength ? normalized : normalized[..maxLength]);
            if (results.Count >= maxItems)
                break;
        }

        return results;
    }

    private static IEnumerable<string> Tokenize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            yield break;

        foreach (Match match in WordRegex.Matches(value.ToLowerInvariant()))
            yield return match.Value;
    }

    private static bool ContainsMixedContent(string loweredHtml)
        => loweredHtml.Contains("src=\"http://", StringComparison.Ordinal)
            || loweredHtml.Contains("src='http://", StringComparison.Ordinal)
            || loweredHtml.Contains("href=\"http://", StringComparison.Ordinal)
            || loweredHtml.Contains("href='http://", StringComparison.Ordinal)
            || loweredHtml.Contains("url(http://", StringComparison.Ordinal);

    private static int CountWords(string? text)
        => string.IsNullOrWhiteSpace(text) ? 0 : WordRegex.Matches(text).Count;

    private static Uri? TryCreateUri(string? value)
        => Uri.TryCreate((value ?? string.Empty).Trim(), UriKind.Absolute, out var uri) ? uri : null;

    private static string? ExtractFileName(string? value, Uri? baseUri)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (!Uri.TryCreate(baseUri, value, out var uri))
            return Path.GetFileName(value);

        var fileName = Path.GetFileName(uri.LocalPath);
        return NormalizeText(fileName);
    }

    private static string? NormalizeText(string? value)
    {
        var normalized = WhitespaceRegex.Replace((value ?? string.Empty).Trim(), " ");
        return normalized.Length == 0 ? null : normalized;
    }

    private static bool ContainsOrdinalIgnoreCase(string? haystack, string? needle)
        => !string.IsNullOrWhiteSpace(haystack)
            && !string.IsNullOrWhiteSpace(needle)
            && haystack.Contains(needle, StringComparison.OrdinalIgnoreCase);

    private static void AppendIfPresent(ICollection<string> values, string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is not null)
            values.Add(normalized);
    }

    private static IEnumerable<string> ExtractPostalAddressSnippets(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null || !UkPostcodeRegex.IsMatch(normalized.ToUpperInvariant()))
            yield break;

        foreach (Match match in AddressWithPostcodeRegex.Matches(normalized))
        {
            var candidate = CleanPostalAddressCandidate(match.Value);
            if (LooksLikePostalAddress(candidate))
                yield return candidate!;
        }

        if (normalized.Length <= 120)
        {
            var candidate = CleanPostalAddressCandidate(normalized);
            if (LooksLikePostalAddress(candidate))
                yield return candidate!;
        }
    }

    private static bool LooksLikePostalAddress(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            return false;
        if (!UkPostcodeRegex.IsMatch(normalized.ToUpperInvariant()))
            return false;
        if (normalized.Length < 15 || normalized.Length > 220)
            return false;

        var lowered = normalized.ToLowerInvariant();
        return normalized.Contains(",", StringComparison.Ordinal) || AddressIndicatorWords.Any(lowered.Contains);
    }

    private static bool IsLikelyCityName(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            return false;
        if (normalized.Length < 2 || normalized.Length > 40)
            return false;
        if (NonCityLocationWords.Contains(normalized.ToLowerInvariant()))
            return false;
        if (normalized.Any(char.IsDigit))
            return false;

        var tokens = Tokenize(normalized).ToList();
        if (tokens.Count is 0 or > 3)
            return false;

        return !tokens.Any(AddressIndicatorWords.Contains);
    }

    private static bool IsLikelyLocationKeyword(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            return false;
        if (UkPostcodeRegex.IsMatch(normalized.ToUpperInvariant()))
            return true;
        if (normalized.Any(char.IsDigit))
            return false;
        if (normalized.Length < 2 || normalized.Length > 40)
            return false;
        if (NonCityLocationWords.Contains(normalized.ToLowerInvariant()))
            return false;

        var tokens = Tokenize(normalized).ToList();
        if (tokens.Count is 0 or > 3)
            return false;

        return !tokens.Any(AddressIndicatorWords.Contains);
    }

    private static bool NamesLookRelated(string left, string right)
    {
        var normalizedLeft = NormalizeText(left)?.ToLowerInvariant();
        var normalizedRight = NormalizeText(right)?.ToLowerInvariant();
        if (normalizedLeft is null || normalizedRight is null)
            return false;

        return normalizedLeft.Contains(normalizedRight, StringComparison.Ordinal)
            || normalizedRight.Contains(normalizedLeft, StringComparison.Ordinal);
    }

    private static string? CleanPostalAddressCandidate(string? value)
    {
        var normalized = NormalizeText(value);
        if (normalized is null)
            return null;

        var postcodeMatch = UkPostcodeRegex.Match(normalized.ToUpperInvariant());
        if (!postcodeMatch.Success)
            return normalized;

        normalized = normalized[..postcodeMatch.Index] + normalized.Substring(postcodeMatch.Index, postcodeMatch.Length);
        normalized = Regex.Replace(normalized, @"^.*?(?=(?:\d+\s+[A-Za-z]|[A-Za-z][A-Za-z0-9&'.\- ]+,\s*[A-Za-z]))", string.Empty, RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"^(?:VAT\s+No:\s*)?[A-Z]{2}\s*\d{3}\s*\d{4}\s*\d{2}\s*", string.Empty, RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"^(?:Company\s+No:\s*)?\d+\s*", string.Empty, RegexOptions.IgnoreCase);
        normalized = normalized.Trim(' ', ',', '|');
        return NormalizeText(normalized);
    }

    private sealed class StructuredDataExtraction
    {
        public List<string> SchemaTypes { get; set; } = [];
        public bool HasLocalBusinessSchema { get; set; }
        public bool HasOrganizationSchema { get; set; }
        public bool HasProductSchema { get; set; }
        public bool HasFaqSchema { get; set; }
        public bool HasBreadcrumbSchema { get; set; }
        public bool HasNapInSchema { get; set; }
        public bool HasGeoCoordinatesInSchema { get; set; }
        public List<string> PhoneNumbers { get; set; } = [];
        public List<string> PostalAddresses { get; set; } = [];
        public List<string> Postcodes { get; set; } = [];
        public List<string> CityNames { get; set; } = [];
        public List<string> BusinessNames { get; set; } = [];
    }

    private sealed record InternalLinkExtraction(
        int InternalLinkCount,
        int ServicePageLinkCount,
        IReadOnlyList<string> AnchorTexts);

    private sealed record ImageExtraction(
        int ImageCount,
        int ImagesMissingAltCount,
        IReadOnlyList<string> ImageAltTexts,
        IReadOnlyList<string> ImageFileNames);
}

public sealed class HomePageAuditParseRequest
{
    public string Html { get; init; } = string.Empty;
    public string RequestedUrl { get; init; } = string.Empty;
    public string? FinalUrl { get; init; }
    public string? DisplayName { get; init; }
    public string? FormattedAddress { get; init; }
    public string? PrimaryCategory { get; init; }
    public string? SearchLocationName { get; init; }
}

public sealed class HomePageAuditParseResult
{
    public string? TitleTag { get; init; }
    public int? TitleTagLength { get; init; }
    public string? MetaDescription { get; init; }
    public int? MetaDescriptionLength { get; init; }
    public string? CanonicalUrl { get; init; }
    public string? RobotsMeta { get; init; }
    public string? HtmlLang { get; init; }
    public string? H1Text { get; init; }
    public int? H1Count { get; init; }
    public int? H2Count { get; init; }
    public int? H3Count { get; init; }
    public IReadOnlyList<string> H2Texts { get; init; } = [];
    public IReadOnlyList<string> H3Texts { get; init; } = [];
    public int? VisibleWordCount { get; init; }
    public int? ParagraphCount { get; init; }
    public int? BulletListCount { get; init; }
    public int? ContentSectionCount { get; init; }
    public bool HasPhoneNumber { get; init; }
    public IReadOnlyList<string> PhoneNumbers { get; init; } = [];
    public bool HasPostalAddress { get; init; }
    public IReadOnlyList<string> PostalAddresses { get; init; } = [];
    public bool HasPostcode { get; init; }
    public IReadOnlyList<string> Postcodes { get; init; } = [];
    public bool HasCityName { get; init; }
    public IReadOnlyList<string> CityNames { get; init; } = [];
    public bool HasBusinessName { get; init; }
    public IReadOnlyList<string> BusinessNames { get; init; } = [];
    public IReadOnlyList<string> SchemaTypes { get; init; } = [];
    public bool HasLocalBusinessSchema { get; init; }
    public bool HasOrganizationSchema { get; init; }
    public bool HasProductSchema { get; init; }
    public bool HasFaqSchema { get; init; }
    public bool HasBreadcrumbSchema { get; init; }
    public bool HasNapInSchema { get; init; }
    public bool HasGeoCoordinatesInSchema { get; init; }
    public string? PageScheme { get; init; }
    public string? CanonicalScheme { get; init; }
    public bool? RedirectsToHttps { get; init; }
    public bool? HasMixedContent { get; init; }
    public int? InternalLinkCount { get; init; }
    public int? ServicePageLinkCount { get; init; }
    public IReadOnlyList<string> InternalAnchorTexts { get; init; } = [];
    public int? ImageCount { get; init; }
    public int? ImagesMissingAltCount { get; init; }
    public IReadOnlyList<string> ImageAltTexts { get; init; } = [];
    public IReadOnlyList<string> ImageFileNames { get; init; } = [];
    public string? DetectedCms { get; init; }
    public string? GeneratorMetaTag { get; init; }
    public bool HasViewportMeta { get; init; }
    public bool HasResponsiveIndicators { get; init; }
    public bool HasFavicon { get; init; }
    public bool HasCookieBanner { get; init; }
    public IReadOnlyList<string> ServiceKeywords { get; init; } = [];
    public IReadOnlyList<string> LocationKeywords { get; init; } = [];
    public IReadOnlyList<string> ServiceTownCombinations { get; init; } = [];
    public IReadOnlyList<string> BrandNames { get; init; } = [];
}
