using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record AppleBingMapLookupRequest(
    string PlaceId,
    string? DisplayName,
    string? NationalPhoneNumber,
    decimal? Lat,
    decimal? Lng,
    string? FormattedAddress,
    string? SearchLocationName);

public sealed record AppleBingMapLookupResult(
    bool AppleChecked,
    bool AppleMatched,
    string? AppleUrl,
    bool BingChecked,
    bool BingMatched,
    string? BingUrl,
    string? AppleError,
    string? BingError);

public interface IAppleBingMapLinksService
{
    Task<AppleBingMapLookupResult> LookupAsync(AppleBingMapLookupRequest request, CancellationToken ct);
}

public sealed class AppleBingMapLinksService(
    IHttpClientFactory httpClientFactory,
    IOptions<AppleMapsOptions> appleOptions,
    IOptions<AzureMapsOptions> azureOptions,
    IAppleMapsLookupTraceRepository appleMapsLookupTraceRepository,
    ILogger<AppleBingMapLinksService> logger) : IAppleBingMapLinksService
{
    private const string AppleTokenEndpoint = "https://maps-api.apple.com/v1/token";
    private const string AppleSearchEndpoint = "https://maps-api.apple.com/v1/search";
    private const string AzureSearchEndpoint = "https://atlas.microsoft.com/search/fuzzy/json";
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan AppleJwtLifetime = TimeSpan.FromDays(180);
    private static readonly TimeSpan AppleJwtRefreshWindow = TimeSpan.FromDays(7);
    private static readonly object AppleJwtGate = new();
    private static string? cachedAppleJwt;
    private static DateTimeOffset cachedAppleJwtExpiryUtc;

    private static readonly HashSet<string> NameNoiseTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "the",
        "and",
        "ltd",
        "limited",
        "llc",
        "inc",
        "co",
        "company",
        "services"
    };
    private static readonly HashSet<string> BroadLocationNoise = new(StringComparer.OrdinalIgnoreCase)
    {
        "uk",
        "u.k.",
        "gb",
        "great britain",
        "england",
        "scotland",
        "wales",
        "northern ireland",
        "united kingdom"
    };
    private static readonly Regex UkPostcodeRegex = new(
        @"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public async Task<AppleBingMapLookupResult> LookupAsync(AppleBingMapLookupRequest request, CancellationToken ct)
    {
        var normalizedName = (request.DisplayName ?? string.Empty).Trim();
        if (normalizedName.Length == 0)
        {
            return new AppleBingMapLookupResult(
                AppleChecked: false,
                AppleMatched: false,
                AppleUrl: null,
                BingChecked: false,
                BingMatched: false,
                BingUrl: null,
                AppleError: "Display name is missing.",
                BingError: "Display name is missing.");
        }

        var appleQueries = BuildAppleQueryVariants(
            normalizedName,
            request.FormattedAddress,
            request.SearchLocationName,
            request.NationalPhoneNumber);
        var bingQueries = BuildAzureQueryVariants(normalizedName, request.FormattedAddress, request.SearchLocationName);

        var appleChecked = false;
        var appleMatched = false;
        string? appleUrl = null;
        string? appleError = null;
        try
        {
            var appleCandidates = await SearchAppleAsync(request.PlaceId, appleQueries, request.Lat, request.Lng, ct);
            appleChecked = true;
            var appleBest = SelectBestMatch(
                normalizedName,
                request.NationalPhoneNumber,
                request.Lat,
                request.Lng,
                appleCandidates);
            if (appleBest is not null)
            {
                appleMatched = true;
                appleUrl = !string.IsNullOrWhiteSpace(appleBest.Url)
                    ? appleBest.Url
                    : BuildAppleMapsFallbackUrl(
                        appleBest.Name,
                        request.Lat ?? appleBest.Lat,
                        request.Lng ?? appleBest.Lng);
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            appleError = Truncate(ex.Message, 500);
            logger.LogWarning(ex, "Apple Maps lookup failed for place {PlaceId}.", request.PlaceId);
        }

        var bingChecked = false;
        var bingMatched = false;
        string? bingUrl = null;
        string? bingError = null;
        try
        {
            var bingCandidates = await SearchAzureAsync(bingQueries, request.Lat, request.Lng, ct);
            bingChecked = true;
            var bingBest = SelectBestMatch(
                normalizedName,
                request.NationalPhoneNumber,
                request.Lat,
                request.Lng,
                bingCandidates);
            if (bingBest is not null)
            {
                bingMatched = true;
                bingUrl = !string.IsNullOrWhiteSpace(bingBest.Url)
                    ? bingBest.Url
                    : BuildBingMapsFallbackUrl(bingBest.Name, bingBest.Lat, bingBest.Lng);
            }
            else
            {
                bingUrl = BuildBingMapsFallbackUrl(normalizedName, request.Lat, request.Lng);
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            bingError = Truncate(ex.Message, 500);
            logger.LogWarning(ex, "Azure/Bing lookup failed for place {PlaceId}.", request.PlaceId);
        }

        return new AppleBingMapLookupResult(
            AppleChecked: appleChecked,
            AppleMatched: appleMatched,
            AppleUrl: appleUrl,
            BingChecked: bingChecked,
            BingMatched: bingMatched,
            BingUrl: bingUrl,
            AppleError: appleError,
            BingError: bingError);
    }

    private async Task<IReadOnlyList<MapCandidate>> SearchAppleAsync(
        string placeId,
        IReadOnlyList<string> queries,
        decimal? lat,
        decimal? lng,
        CancellationToken ct)
    {
        if (queries.Count == 0)
            return [];

        var accessToken = await GetAppleAccessTokenAsync(ct);
        var items = new List<MapCandidate>();
        for (var queryIndex = 0; queryIndex < queries.Count; queryIndex++)
        {
            var query = queries[queryIndex];
            var endpoint = new StringBuilder()
                .Append(AppleSearchEndpoint)
                .Append("?q=").Append(Uri.EscapeDataString(query))
                .Append("&limit=10");
            if (lat.HasValue && lng.HasValue)
            {
                endpoint
                    .Append("&userLocation=")
                    .Append(lat.Value.ToString(CultureInfo.InvariantCulture))
                    .Append(",")
                    .Append(lng.Value.ToString(CultureInfo.InvariantCulture));
            }

            var requestUrl = endpoint.ToString();
            string? responseBody = null;
            int? responseStatusCode = null;
            var traceWritten = false;

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                using var response = await SendAsync(request, ct);
                responseStatusCode = (int)response.StatusCode;
                responseBody = await response.Content.ReadAsStringAsync(ct);

                await WriteAppleLookupTraceAsync(
                    placeId,
                    queryIndex + 1,
                    query,
                    requestUrl,
                    responseStatusCode,
                    responseBody,
                    null,
                    ct);
                traceWritten = true;

                if (!response.IsSuccessStatusCode)
                    throw new HttpRequestException($"Apple Maps search failed with HTTP {(int)response.StatusCode}.");

                using var doc = JsonDocument.Parse(responseBody);
                if (!doc.RootElement.TryGetProperty("results", out var results)
                    || results.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var item in results.EnumerateArray())
                {
                    var name = FirstNonEmpty(
                        TryGetString(item, "displayName"),
                        TryGetString(item, "name"),
                        TryGetString(item, "title"));
                    if (string.IsNullOrWhiteSpace(name))
                        continue;

                    var phone = FirstNonEmpty(
                        TryGetString(item, "phone"),
                        TryGetString(item, "phoneNumber"),
                        TryGetNestedString(item, "poi", "phone"));
                    var applePlaceId = FirstNonEmpty(
                        TryGetString(item, "id"),
                        TryGetString(item, "placeId"),
                        TryGetNestedString(item, "poi", "id"));
                    var profileUrl = BuildAppleMapsProfileUrl(applePlaceId);
                    var rawUrl = FirstNonEmpty(
                        TryGetString(item, "url"),
                        TryGetNestedString(item, "poi", "url"));
                    if (string.IsNullOrWhiteSpace(profileUrl))
                    {
                        profileUrl = BuildAppleMapsProfileUrl(ExtractApplePlaceIdFromUrl(rawUrl));
                    }
                    var candidateLat = FirstNonNull(
                        TryGetNestedDecimal(item, "coordinate", "latitude"),
                        TryGetNestedDecimal(item, "position", "lat"));
                    var candidateLng = FirstNonNull(
                        TryGetNestedDecimal(item, "coordinate", "longitude"),
                        TryGetNestedDecimal(item, "position", "lon"));
                    var url = FirstNonEmpty(
                        profileUrl,
                        rawUrl);

                    items.Add(new MapCandidate(name.Trim(), phone, candidateLat, candidateLng, url));
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                if (!traceWritten)
                {
                    await WriteAppleLookupTraceAsync(
                        placeId,
                        queryIndex + 1,
                        query,
                        requestUrl,
                        responseStatusCode,
                        responseBody,
                        Truncate(ex.Message, 2000),
                        ct);
                }

                throw;
            }
        }

        return DeduplicateCandidates(items);
    }

    private async Task WriteAppleLookupTraceAsync(
        string placeId,
        int queryIndex,
        string queryText,
        string requestUrl,
        int? httpStatusCode,
        string? responseJson,
        string? errorMessage,
        CancellationToken ct)
    {
        try
        {
            await appleMapsLookupTraceRepository.InsertAsync(new AppleMapsLookupTraceWriteModel(
                PlaceId: placeId,
                QueryIndex: queryIndex,
                QueryText: queryText,
                RequestUrl: requestUrl,
                HttpStatusCode: httpStatusCode,
                ResponseJson: responseJson,
                ErrorMessage: errorMessage), ct);
        }
        catch (Exception traceEx) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(traceEx, "Failed to store Apple Maps lookup trace for place {PlaceId}.", placeId);
        }
    }

    private async Task<IReadOnlyList<MapCandidate>> SearchAzureAsync(IReadOnlyList<string> queries, decimal? lat, decimal? lng, CancellationToken ct)
    {
        if (queries.Count == 0)
            return [];

        var cfg = azureOptions.Value;
        var keys = new List<string>(2);
        if (!string.IsNullOrWhiteSpace(cfg.PrimaryKey))
            keys.Add(cfg.PrimaryKey);
        if (!string.IsNullOrWhiteSpace(cfg.SecondaryKey)
            && !string.Equals(cfg.SecondaryKey, cfg.PrimaryKey, StringComparison.Ordinal))
        {
            keys.Add(cfg.SecondaryKey);
        }

        if (keys.Count == 0)
            throw new InvalidOperationException("Azure Maps key is not configured.");

        Exception? lastError = null;
        var hasSuccessfulResponse = false;
        foreach (var key in keys)
        {
            var items = new List<MapCandidate>();
            foreach (var query in queries)
            {
                try
                {
                    var endpoint = BuildAzureSearchEndpoint(query, lat, lng);
                    using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                    request.Headers.TryAddWithoutValidation("subscription-key", key);
                    using var response = await SendAsync(request, ct);
                    if (!response.IsSuccessStatusCode)
                        throw new HttpRequestException($"Azure Maps search failed with HTTP {(int)response.StatusCode}.");

                    hasSuccessfulResponse = true;
                    await using var stream = await response.Content.ReadAsStreamAsync(ct);
                    using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
                    if (!doc.RootElement.TryGetProperty("results", out var results)
                        || results.ValueKind != JsonValueKind.Array)
                    {
                        logger.LogDebug("Azure Maps query '{Query}' returned no results payload.", query);
                        continue;
                    }
                    logger.LogDebug("Azure Maps query '{Query}' returned {Count} candidates.", query, results.GetArrayLength());

                    foreach (var item in results.EnumerateArray())
                    {
                        var poi = item.TryGetProperty("poi", out var poiEl) ? poiEl : default;
                        var name = FirstNonEmpty(
                            TryGetString(poi, "name"),
                            TryGetString(item, "name"));
                        if (string.IsNullOrWhiteSpace(name))
                            continue;

                        var position = item.TryGetProperty("position", out var positionEl) ? positionEl : default;
                        var candidateLat = TryGetDecimal(position, "lat");
                        var candidateLng = TryGetDecimal(position, "lon");
                        var phone = TryGetString(poi, "phone");
                        var url = FirstNonEmpty(
                            TryGetString(poi, "url"),
                            BuildBingMapsFallbackUrl(name, candidateLat, candidateLng));

                        items.Add(new MapCandidate(name.Trim(), phone, candidateLat, candidateLng, url));
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    lastError = ex;
                }
            }

            if (items.Count > 0)
                return DeduplicateCandidates(items);
        }

        if (hasSuccessfulResponse)
            return [];

        throw lastError ?? new InvalidOperationException("Azure Maps search failed.");
    }

    private async Task<string> GetAppleAccessTokenAsync(CancellationToken ct)
    {
        var jwt = GetOrCreateAppleJwt(forceRefresh: false);
        var firstTry = await RequestAppleAccessTokenAsync(jwt, ct);
        if (!string.IsNullOrWhiteSpace(firstTry))
            return firstTry;

        jwt = GetOrCreateAppleJwt(forceRefresh: true);
        var secondTry = await RequestAppleAccessTokenAsync(jwt, ct);
        if (!string.IsNullOrWhiteSpace(secondTry))
            return secondTry;

        throw new InvalidOperationException("Apple Maps token response did not contain access token.");
    }

    private async Task<string?> RequestAppleAccessTokenAsync(string jwt, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, AppleTokenEndpoint);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", jwt);
        using var response = await SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
            return null;

        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var token = FirstNonEmpty(
            TryGetString(doc.RootElement, "accessToken"),
            TryGetString(doc.RootElement, "token"));
        return string.IsNullOrWhiteSpace(token) ? null : token.Trim();
    }

    private string GetOrCreateAppleJwt(bool forceRefresh)
    {
        var now = DateTimeOffset.UtcNow;
        lock (AppleJwtGate)
        {
            if (!forceRefresh
                && !string.IsNullOrWhiteSpace(cachedAppleJwt)
                && cachedAppleJwtExpiryUtc - now > AppleJwtRefreshWindow)
            {
                return cachedAppleJwt;
            }

            var cfg = appleOptions.Value;
            if (string.IsNullOrWhiteSpace(cfg.P8Path) || !File.Exists(cfg.P8Path))
                throw new InvalidOperationException("Apple .p8 key file is not configured.");

            var p8Pem = File.ReadAllText(cfg.P8Path);
            using var ecdsa = ECDsa.Create();
            ecdsa.ImportFromPem(p8Pem);

            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.Add(AppleJwtLifetime);

            var headerJson = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["alg"] = "ES256",
                ["kid"] = (cfg.KeyId ?? string.Empty).Trim(),
                ["typ"] = "JWT"
            });

            var payloadJson = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["iss"] = (cfg.TeamId ?? string.Empty).Trim(),
                ["iat"] = issuedAt.ToUnixTimeSeconds(),
                ["exp"] = expiresAt.ToUnixTimeSeconds()
            });

            var headerPart = Base64UrlEncode(Encoding.UTF8.GetBytes(headerJson));
            var payloadPart = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
            var signingInput = $"{headerPart}.{payloadPart}";
            var signature = ecdsa.SignData(
                Encoding.ASCII.GetBytes(signingInput),
                HashAlgorithmName.SHA256,
                DSASignatureFormat.IeeeP1363FixedFieldConcatenation);

            cachedAppleJwt = $"{signingInput}.{Base64UrlEncode(signature)}";
            cachedAppleJwtExpiryUtc = expiresAt;
            return cachedAppleJwt;
        }
    }

    private string BuildAzureSearchEndpoint(string query, decimal? lat, decimal? lng)
    {
        var sb = new StringBuilder();
        sb.Append(AzureSearchEndpoint);
        sb.Append("?api-version=1.0");
        sb.Append("&query=").Append(Uri.EscapeDataString(query));
        sb.Append("&limit=20");
        sb.Append("&idxSet=POI");
        if (lat.HasValue && lng.HasValue)
        {
            sb.Append("&lat=").Append(lat.Value.ToString(CultureInfo.InvariantCulture));
            sb.Append("&lon=").Append(lng.Value.ToString(CultureInfo.InvariantCulture));
            sb.Append("&radius=5000");
        }

        return sb.ToString();
    }

    private static string BuildAppleQuery(string displayName, string? formattedAddress, string? location)
    {
        var locality = ExtractLocalityFromAddress(formattedAddress);
        if (!string.IsNullOrWhiteSpace(locality))
            return $"{displayName} {locality}";

        var locationPart = NormalizeLocation(location);
        if (!string.IsNullOrWhiteSpace(locationPart) && !IsBroadLocation(locationPart))
            return $"{displayName} {locationPart}";

        return displayName;
    }

    private static IReadOnlyList<string> BuildAppleQueryVariants(string displayName, string? formattedAddress, string? location, string? nationalPhoneNumber)
    {
        var variants = new List<string>(6);
        AddQueryVariant(variants, BuildAppleQuery(displayName, formattedAddress, location));

        var locality = ExtractLocalityFromAddress(formattedAddress);
        var postcode = ExtractUkPostcode(formattedAddress);
        if (!string.IsNullOrWhiteSpace(postcode))
            AddQueryVariant(variants, $"{displayName} {postcode}");
        if (!string.IsNullOrWhiteSpace(locality) && !string.IsNullOrWhiteSpace(postcode))
            AddQueryVariant(variants, $"{displayName} {locality} {postcode}");

        var phoneQuery = NormalizePhoneForQuery(nationalPhoneNumber);
        if (!string.IsNullOrWhiteSpace(phoneQuery))
        {
            AddQueryVariant(variants, $"{displayName} {phoneQuery}");
            if (!string.IsNullOrWhiteSpace(locality))
                AddQueryVariant(variants, $"{displayName} {locality} {phoneQuery}");
        }

        return variants;
    }

    private static IReadOnlyList<string> BuildAzureQueryVariants(string displayName, string? formattedAddress, string? location)
    {
        var variants = new List<string>(6);
        AddQueryVariant(variants, displayName);

        var locality = ExtractLocalityFromAddress(formattedAddress);
        var postcode = ExtractUkPostcode(formattedAddress);
        var locationPart = NormalizeLocation(location);

        if (!string.IsNullOrWhiteSpace(locality))
            AddQueryVariant(variants, $"{displayName} {locality}");
        if (!string.IsNullOrWhiteSpace(postcode))
            AddQueryVariant(variants, $"{displayName} {postcode}");
        if (!string.IsNullOrWhiteSpace(locality) && !string.IsNullOrWhiteSpace(postcode))
            AddQueryVariant(variants, $"{displayName} {locality} {postcode}");
        if (!string.IsNullOrWhiteSpace(locationPart) && !IsBroadLocation(locationPart))
            AddQueryVariant(variants, $"{displayName} {locationPart}");

        return variants;
    }

    private static void AddQueryVariant(List<string> variants, string? query)
    {
        var normalized = (query ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return;
        if (variants.Any(x => string.Equals(x, normalized, StringComparison.OrdinalIgnoreCase)))
            return;
        variants.Add(normalized);
    }

    private static string? NormalizeLocation(string? value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private static string? NormalizePhoneForQuery(string? value)
    {
        var normalized = Regex.Replace(value ?? string.Empty, "\\s+", " ").Trim();
        if (normalized.Length == 0)
            return null;
        return DigitsOnly(normalized).Length >= 6 ? normalized : null;
    }

    private static bool IsBroadLocation(string value)
    {
        var normalized = value.Trim();
        if (normalized.Length == 0)
            return true;
        return BroadLocationNoise.Contains(normalized);
    }

    private static string? ExtractUkPostcode(string? formattedAddress)
    {
        var value = (formattedAddress ?? string.Empty).Trim();
        if (value.Length == 0)
            return null;
        var match = UkPostcodeRegex.Match(value);
        if (!match.Success)
            return null;
        return match.Value.Trim().ToUpperInvariant();
    }

    private static string? ExtractLocalityFromAddress(string? formattedAddress)
    {
        var value = (formattedAddress ?? string.Empty).Trim();
        if (value.Length == 0)
            return null;

        var segments = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = segments.Length - 1; i >= 0; i--)
        {
            var segment = UkPostcodeRegex.Replace(segments[i], " ").Trim();
            if (segment.Length == 0)
                continue;
            if (!segment.Any(char.IsLetter))
                continue;
            if (IsBroadLocation(segment))
                continue;
            return segment;
        }

        return null;
    }

    private static IReadOnlyList<MapCandidate> DeduplicateCandidates(IReadOnlyList<MapCandidate> candidates)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var unique = new List<MapCandidate>(candidates.Count);
        foreach (var candidate in candidates)
        {
            var key = string.Join("|",
                candidate.Name.Trim(),
                candidate.Lat?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                candidate.Lng?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
                DigitsOnly(candidate.Phone));
            if (!seen.Add(key))
                continue;
            unique.Add(candidate);
        }

        return unique;
    }

    private static MapCandidate? SelectBestMatch(
        string sourceName,
        string? sourcePhone,
        decimal? sourceLat,
        decimal? sourceLng,
        IReadOnlyList<MapCandidate> candidates)
    {
        MapCandidate? best = null;
        var bestScore = 0.0;
        foreach (var candidate in candidates)
        {
            var nameSimilarity = ComputeNameSimilarity(sourceName, candidate.Name);
            var phoneSimilarity = ComputePhoneSimilarity(sourcePhone, candidate.Phone);
            var distanceMeters = ComputeDistanceMeters(sourceLat, sourceLng, candidate.Lat, candidate.Lng);

            if (!IsPositiveMatch(nameSimilarity, phoneSimilarity, distanceMeters))
                continue;

            var score = (nameSimilarity * 0.70)
                        + ((phoneSimilarity ?? 0d) * 0.20)
                        + (DistanceScore(distanceMeters) * 0.10);
            if (score <= bestScore)
                continue;

            best = candidate;
            bestScore = score;
        }

        return best;
    }

    private static bool IsPositiveMatch(double nameSimilarity, double? phoneSimilarity, double? distanceMeters)
    {
        if (nameSimilarity < 0.55)
            return false;
        if (phoneSimilarity.HasValue && phoneSimilarity.Value >= 0.60)
            return true;
        if (!distanceMeters.HasValue)
            return nameSimilarity >= 0.82;
        if (distanceMeters.Value <= 300 && nameSimilarity >= 0.60)
            return true;
        if (distanceMeters.Value <= 900 && nameSimilarity >= 0.72)
            return true;
        return false;
    }

    private static double DistanceScore(double? distanceMeters)
    {
        if (!distanceMeters.HasValue)
            return 0;
        if (distanceMeters.Value <= 150) return 1.0;
        if (distanceMeters.Value <= 300) return 0.8;
        if (distanceMeters.Value <= 600) return 0.6;
        if (distanceMeters.Value <= 1000) return 0.4;
        if (distanceMeters.Value <= 2000) return 0.2;
        return 0;
    }

    private static double ComputeNameSimilarity(string left, string right)
    {
        var leftTokens = TokenizeName(left);
        var rightTokens = TokenizeName(right);
        if (leftTokens.Count == 0 || rightTokens.Count == 0)
            return 0;

        var leftSet = leftTokens.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var rightSet = rightTokens.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var intersection = leftSet.Intersect(rightSet, StringComparer.OrdinalIgnoreCase).Count();
        var union = leftSet.Union(rightSet, StringComparer.OrdinalIgnoreCase).Count();
        if (union == 0)
            return 0;
        return intersection / (double)union;
    }

    private static IReadOnlyList<string> TokenizeName(string value)
    {
        return Regex.Matches(value.ToLowerInvariant(), "[a-z0-9]+")
            .Select(x => x.Value)
            .Where(x => x.Length > 1 && !NameNoiseTokens.Contains(x))
            .ToList();
    }

    private static double? ComputePhoneSimilarity(string? left, string? right)
    {
        var a = DigitsOnly(left);
        var b = DigitsOnly(right);
        if (a.Length < 6 || b.Length < 6)
            return null;
        if (string.Equals(a, b, StringComparison.Ordinal))
            return 1.0;

        if (a.Length >= 7 && b.Length >= 7)
        {
            var a7 = a[^7..];
            var b7 = b[^7..];
            if (string.Equals(a7, b7, StringComparison.Ordinal))
                return 0.9;
        }

        if (a.Contains(b, StringComparison.Ordinal) || b.Contains(a, StringComparison.Ordinal))
            return 0.7;

        var max = Math.Min(a.Length, b.Length);
        var commonSuffix = 0;
        while (commonSuffix < max && a[a.Length - 1 - commonSuffix] == b[b.Length - 1 - commonSuffix])
            commonSuffix++;

        return commonSuffix / (double)Math.Max(a.Length, b.Length);
    }

    private static string DigitsOnly(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        return new string(value.Where(char.IsDigit).ToArray());
    }

    private static double? ComputeDistanceMeters(decimal? lat1, decimal? lng1, decimal? lat2, decimal? lng2)
    {
        if (!lat1.HasValue || !lng1.HasValue || !lat2.HasValue || !lng2.HasValue)
            return null;

        var r = 6371000d;
        var phi1 = DegreesToRadians((double)lat1.Value);
        var phi2 = DegreesToRadians((double)lat2.Value);
        var deltaPhi = DegreesToRadians((double)(lat2.Value - lat1.Value));
        var deltaLambda = DegreesToRadians((double)(lng2.Value - lng1.Value));
        var a = Math.Sin(deltaPhi / 2d) * Math.Sin(deltaPhi / 2d)
                + Math.Cos(phi1) * Math.Cos(phi2)
                * Math.Sin(deltaLambda / 2d) * Math.Sin(deltaLambda / 2d);
        var c = 2d * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1d - a));
        return r * c;
    }

    private static double DegreesToRadians(double degrees) => degrees * (Math.PI / 180d);

    private static string BuildAppleMapsFallbackUrl(string? name, decimal? lat, decimal? lng)
    {
        if (lat.HasValue && lng.HasValue)
        {
            return $"https://maps.apple.com/?ll={lat.Value.ToString(CultureInfo.InvariantCulture)},{lng.Value.ToString(CultureInfo.InvariantCulture)}&q={Uri.EscapeDataString((name ?? string.Empty).Trim())}";
        }

        return $"https://maps.apple.com/?q={Uri.EscapeDataString((name ?? string.Empty).Trim())}";
    }

    private static string? BuildAppleMapsProfileUrl(string? applePlaceId)
    {
        var normalizedPlaceId = (applePlaceId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            return null;

        return $"https://maps.apple.com/place?place-id={Uri.EscapeDataString(normalizedPlaceId)}";
    }

    private static string? ExtractApplePlaceIdFromUrl(string? url)
    {
        var normalizedUrl = (url ?? string.Empty).Trim();
        if (normalizedUrl.Length == 0)
            return null;
        if (!Uri.TryCreate(normalizedUrl, UriKind.Absolute, out var uri))
            return null;
        if (!string.Equals(uri.Host, "maps.apple.com", StringComparison.OrdinalIgnoreCase))
            return null;

        var placeId = ExtractQueryValue(uri.Query, "place-id", "auid");
        return string.IsNullOrWhiteSpace(placeId) ? null : placeId.Trim();
    }

    private static string? ExtractQueryValue(string query, params string[] keys)
    {
        var normalizedQuery = (query ?? string.Empty).Trim();
        if (normalizedQuery.StartsWith("?", StringComparison.Ordinal))
            normalizedQuery = normalizedQuery[1..];
        if (normalizedQuery.Length == 0 || keys.Length == 0)
            return null;

        var keySet = new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);
        foreach (var segment in normalizedQuery.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = segment.Split('=', 2);
            if (parts.Length == 0)
                continue;
            var key = Uri.UnescapeDataString(parts[0] ?? string.Empty);
            if (!keySet.Contains(key))
                continue;

            var rawValue = parts.Length > 1 ? parts[1] : string.Empty;
            var decodedValue = Uri.UnescapeDataString(rawValue.Replace("+", "%20", StringComparison.Ordinal));
            return decodedValue;
        }

        return null;
    }

    private static string BuildBingMapsFallbackUrl(string? name, decimal? lat, decimal? lng)
    {
        var query = Uri.EscapeDataString((name ?? string.Empty).Trim());
        if (lat.HasValue && lng.HasValue)
        {
            return $"https://www.bing.com/maps/search?style=r&q={query}&cp={lat.Value.ToString(CultureInfo.InvariantCulture)}~{lng.Value.ToString(CultureInfo.InvariantCulture)}&lvl=16";
        }

        return $"https://www.bing.com/maps/search?style=r&q={query}";
    }

    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var client = httpClientFactory.CreateClient();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(RequestTimeout);
        return await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token);
    }

    private static string? FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }

    private static decimal? FirstNonNull(params decimal?[] values)
    {
        foreach (var value in values)
        {
            if (value.HasValue)
                return value;
        }

        return null;
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return null;
        if (!element.TryGetProperty(propertyName, out var value))
            return null;
        return value.ValueKind == JsonValueKind.String ? value.GetString() : null;
    }

    private static string? TryGetNestedString(JsonElement element, string objectName, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return null;
        if (!element.TryGetProperty(objectName, out var child))
            return null;
        return TryGetString(child, propertyName);
    }

    private static string? TryGetJoinedStringArray(JsonElement element, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return null;
        if (!element.TryGetProperty(propertyName, out var array))
            return null;
        if (array.ValueKind != JsonValueKind.Array)
            return null;

        var values = new List<string>(8);
        foreach (var item in array.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
                continue;
            var value = (item.GetString() ?? string.Empty).Trim();
            if (value.Length == 0)
                continue;
            values.Add(value);
        }

        return values.Count == 0 ? null : string.Join(", ", values);
    }

    private static decimal? TryGetDecimal(JsonElement element, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return null;
        if (!element.TryGetProperty(propertyName, out var value))
            return null;
        if (value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var number))
            return number;
        return null;
    }

    private static decimal? TryGetNestedDecimal(JsonElement element, string objectName, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return null;
        if (!element.TryGetProperty(objectName, out var child))
            return null;
        return TryGetDecimal(child, propertyName);
    }

    private static string Truncate(string value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string Base64UrlEncode(byte[] bytes)
    {
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private sealed record MapCandidate(
        string Name,
        string? Phone,
        decimal? Lat,
        decimal? Lng,
        string? Url);
}
