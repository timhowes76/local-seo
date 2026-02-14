using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IGooglePlacesClient
{
    Task<IReadOnlyList<GooglePlace>> SearchAsync(string seedKeyword, string locationName, decimal? centerLat, decimal? centerLng, int radiusMeters, int limit, CancellationToken ct);
    Task<GooglePlace?> GetPlaceDetailsAsync(string placeId, CancellationToken ct);
    Task<(decimal Lat, decimal Lng)?> GeocodeAsync(string locationName, string? countryCode, CancellationToken ct);
}

public sealed class GooglePlacesClient(IHttpClientFactory factory, IOptions<GoogleOptions> options, ILogger<GooglePlacesClient> logger) : IGooglePlacesClient
{
    private const string FieldMask =
        "places.id,places.displayName,places.primaryType,places.primaryTypeDisplayName,places.googleMapsTypeLabel,places.types,places.rating,places.userRatingCount,places.formattedAddress,places.location,places.nationalPhoneNumber,places.websiteUri,places.editorialSummary,places.photos,places.pureServiceAreaBusiness";
    private const string PlaceDetailsFieldMask =
        "id,displayName,primaryType,primaryTypeDisplayName,googleMapsTypeLabel,types,formattedAddress,location,nationalPhoneNumber,websiteUri,editorialSummary,photos,pureServiceAreaBusiness,businessStatus,regularOpeningHours";

    public async Task<IReadOnlyList<GooglePlace>> SearchAsync(string seedKeyword, string locationName, decimal? centerLat, decimal? centerLng, int radiusMeters, int limit, CancellationToken ct)
    {
        var apiKey = options.Value.ApiKey;
        if (string.IsNullOrWhiteSpace(apiKey)) throw new InvalidOperationException("Google API key missing.");

        var body = new Dictionary<string, object?> { ["textQuery"] = $"{seedKeyword} in {locationName}" };
        if (centerLat.HasValue && centerLng.HasValue)
        {
            body["locationBias"] = new
            {
                circle = new
                {
                    center = new { latitude = centerLat.Value, longitude = centerLng.Value },
                    radius = radiusMeters
                }
            };
        }

        var client = factory.CreateClient();
        for (var attempt = 1; attempt <= 5; attempt++)
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, "https://places.googleapis.com/v1/places:searchText");
            req.Headers.Add("X-Goog-Api-Key", apiKey);
            req.Headers.Add("X-Goog-FieldMask", FieldMask);
            req.Content = JsonContent.Create(body);

            using var resp = await client.SendAsync(req, ct);
            if (resp.IsSuccessStatusCode)
            {
                await using var stream = await resp.Content.ReadAsStreamAsync(ct);
                var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
                var places = doc.RootElement.TryGetProperty("places", out var p) ? p : default;
                if (places.ValueKind != JsonValueKind.Array) return [];
                var list = new List<GooglePlace>();
                foreach (var item in places.EnumerateArray().Take(limit))
                {
                    var placeId = item.GetProperty("id").GetString()!;
                    var details = await TryGetPlaceDetailsAsync(client, apiKey, placeId, ct);

                    var types = item.TryGetProperty("types", out var t) && t.ValueKind == JsonValueKind.Array
                        ? string.Join(",", t.EnumerateArray().Select(x => x.GetString()).Where(x => !string.IsNullOrWhiteSpace(x)))
                        : string.Empty;
                    if (details?.TypesCsv is { Length: > 0 })
                        types = details.TypesCsv;

                    string? display = PreferNonEmpty(GetLocalizedText(item, "displayName"), details?.DisplayName);
                    string? primaryCategory = PreferSpecificCategory(
                        GetPrimaryCategoryLabel(item),
                        details?.PrimaryCategory);
                    string? description = PreferLongerText(
                        GetLocalizedText(item, "editorialSummary"),
                        details?.Description);
                    decimal? lat = item.TryGetProperty("location", out var loc) && loc.TryGetProperty("latitude", out var latEl) ? latEl.GetDecimal() : null;
                    decimal? lng = item.TryGetProperty("location", out var loc2) && loc2.TryGetProperty("longitude", out var lngEl) ? lngEl.GetDecimal() : null;
                    lat ??= details?.Lat;
                    lng ??= details?.Lng;
                    decimal? rating = item.TryGetProperty("rating", out var r) ? r.GetDecimal() : null;
                    int? cnt = item.TryGetProperty("userRatingCount", out var c) ? c.GetInt32() : null;
                    int? photoCount = item.TryGetProperty("photos", out var photos) && photos.ValueKind == JsonValueKind.Array
                        ? photos.GetArrayLength()
                        : null;
                    photoCount ??= details?.PhotoCount;
                    var openingHours = GetWeekdayDescriptions(item);
                    if (openingHours.Count == 0 && details is not null)
                        openingHours = details.RegularOpeningHours;
                    var primaryType = PreferSpecificType(
                        item.TryGetProperty("primaryType", out var pt) ? pt.GetString() : null,
                        details?.PrimaryType);
                    var phone = PreferNonEmpty(
                        item.TryGetProperty("nationalPhoneNumber", out var phoneEl) ? phoneEl.GetString() : null,
                        details?.NationalPhoneNumber);
                    var website = PreferNonEmpty(
                        item.TryGetProperty("websiteUri", out var websiteEl) ? websiteEl.GetString() : null,
                        details?.WebsiteUri);
                    bool? isServiceAreaBusiness = item.TryGetProperty("pureServiceAreaBusiness", out var sab) ? sab.GetBoolean() : null;
                    isServiceAreaBusiness ??= details?.IsServiceAreaBusiness;
                    var businessStatus = item.TryGetProperty("businessStatus", out var status) ? status.GetString() : null;
                    businessStatus ??= details?.BusinessStatus;

                    list.Add(new GooglePlace(
                        placeId,
                        display,
                        primaryType,
                        primaryCategory,
                        types,
                        rating,
                        cnt,
                        PreferNonEmpty(item.TryGetProperty("formattedAddress", out var fa) ? fa.GetString() : null, details?.FormattedAddress),
                        lat,
                        lng,
                        phone,
                        website,
                        description,
                        photoCount,
                        isServiceAreaBusiness,
                        businessStatus,
                        openingHours));
                }
                return list;
            }

            if (!IsTransient(resp.StatusCode) || attempt == 5)
            {
                var bodyText = await resp.Content.ReadAsStringAsync(ct);
                throw new HttpRequestException($"Google Places error {(int)resp.StatusCode}: {bodyText}");
            }

            var delay = TimeSpan.FromMilliseconds(Math.Pow(2, attempt) * 250 + Random.Shared.Next(0, 250));
            logger.LogWarning("Transient Google API error {StatusCode}; retrying in {DelayMs}ms", (int)resp.StatusCode, delay.TotalMilliseconds);
            await Task.Delay(delay, ct);
        }

        return [];
    }

    public async Task<GooglePlace?> GetPlaceDetailsAsync(string placeId, CancellationToken ct)
    {
        var apiKey = options.Value.ApiKey;
        if (string.IsNullOrWhiteSpace(apiKey))
            return null;

        var client = factory.CreateClient();
        var details = await TryGetPlaceDetailsAsync(client, apiKey, placeId, ct);
        if (details is null)
            return null;

        return new GooglePlace(
            placeId,
            details.DisplayName,
            details.PrimaryType,
            details.PrimaryCategory,
            details.TypesCsv,
            null,
            null,
            details.FormattedAddress,
            details.Lat,
            details.Lng,
            details.NationalPhoneNumber,
            details.WebsiteUri,
            details.Description,
            details.PhotoCount,
            details.IsServiceAreaBusiness,
            details.BusinessStatus,
            details.RegularOpeningHours);
    }

    public async Task<(decimal Lat, decimal Lng)?> GeocodeAsync(string locationName, string? countryCode, CancellationToken ct)
    {
        var apiKey = options.Value.ApiKey;
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new InvalidOperationException("Google API key missing.");
        if (string.IsNullOrWhiteSpace(locationName))
            return null;

        var query = $"address={Uri.EscapeDataString(locationName)}";
        if (!string.IsNullOrWhiteSpace(countryCode))
            query += $"&components={Uri.EscapeDataString($"country:{countryCode}")}";
        var url = $"https://maps.googleapis.com/maps/api/geocode/json?{query}&key={Uri.EscapeDataString(apiKey)}";

        var client = factory.CreateClient();
        using var resp = await client.GetAsync(url, ct);
        if (!resp.IsSuccessStatusCode)
        {
            var bodyText = await resp.Content.ReadAsStringAsync(ct);
            throw new HttpRequestException($"Google Geocoding error {(int)resp.StatusCode}: {bodyText}");
        }

        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var root = doc.RootElement;
        var status = root.TryGetProperty("status", out var statusNode) ? statusNode.GetString() : null;

        if (!string.Equals(status, "OK", StringComparison.OrdinalIgnoreCase))
        {
            if (string.Equals(status, "ZERO_RESULTS", StringComparison.OrdinalIgnoreCase))
                return null;

            var errorMessage = root.TryGetProperty("error_message", out var err) ? err.GetString() : null;
            throw new InvalidOperationException($"Google geocoding failed with status '{status}'{(string.IsNullOrWhiteSpace(errorMessage) ? string.Empty : $": {errorMessage}")}.");
        }

        if (!root.TryGetProperty("results", out var results) || results.ValueKind != JsonValueKind.Array || results.GetArrayLength() == 0)
            return null;

        var first = results[0];
        if (!first.TryGetProperty("geometry", out var geometry) || geometry.ValueKind != JsonValueKind.Object)
            return null;
        if (!geometry.TryGetProperty("location", out var location) || location.ValueKind != JsonValueKind.Object)
            return null;
        if (!location.TryGetProperty("lat", out var latNode) || !location.TryGetProperty("lng", out var lngNode))
            return null;

        var lat = (decimal)latNode.GetDouble();
        var lng = (decimal)lngNode.GetDouble();
        return (lat, lng);
    }

    private static bool IsTransient(HttpStatusCode statusCode)
        => statusCode == HttpStatusCode.TooManyRequests || (int)statusCode >= 500;

    private static string? GetLocalizedText(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node) || node.ValueKind != JsonValueKind.Object)
            return null;
        return node.TryGetProperty("text", out var text) ? text.GetString() : null;
    }

    private static string? GetPrimaryCategoryLabel(JsonElement parent)
        => PreferSpecificCategory(
            GetLocalizedText(parent, "googleMapsTypeLabel"),
            GetLocalizedText(parent, "primaryTypeDisplayName"));

    private static string? PreferNonEmpty(string? preferred, string? fallback)
        => !string.IsNullOrWhiteSpace(preferred) ? preferred : fallback;

    private static string? PreferLongerText(string? first, string? second)
    {
        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        return second.Length > first.Length ? second : first;
    }

    private static string? PreferSpecificType(string? first, string? second)
    {
        var firstGeneric = IsGenericType(first);
        var secondGeneric = IsGenericType(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static string? PreferSpecificCategory(string? first, string? second)
    {
        var firstGeneric = IsGenericCategory(first);
        var secondGeneric = IsGenericCategory(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static bool IsGenericType(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;
        var normalized = NormalizeKey(value);
        if (GenericTypes.Contains(normalized))
            return true;
        return HasServiceToken(normalized) && normalized != "web_designer";
    }

    private static bool IsGenericCategory(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;
        var normalized = NormalizeLabel(value);
        if (GenericCategories.Contains(normalized))
            return true;
        return normalized.Contains("service");
    }

    private static bool HasServiceToken(string value)
    {
        var normalized = NormalizeKey(value);
        return normalized.Contains("_service")
            || normalized.Contains("_services")
            || normalized == "service"
            || normalized == "services";
    }

    private static string NormalizeKey(string value)
        => value.Trim().ToLowerInvariant();

    private static string NormalizeLabel(string value)
        => value.Trim().ToLowerInvariant();

    private static IReadOnlyList<string> GetWeekdayDescriptions(JsonElement parent)
    {
        if (!parent.TryGetProperty("regularOpeningHours", out var hours) || hours.ValueKind != JsonValueKind.Object)
            return [];
        if (!hours.TryGetProperty("weekdayDescriptions", out var weekdays) || weekdays.ValueKind != JsonValueKind.Array)
            return [];

        return weekdays
            .EnumerateArray()
            .Select(x => x.GetString())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x!.Trim())
            .ToList();
    }

    private async Task<PlaceDetailsEnvelope?> TryGetPlaceDetailsAsync(HttpClient client, string apiKey, string placeId, CancellationToken ct)
    {
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Get, $"https://places.googleapis.com/v1/places/{Uri.EscapeDataString(placeId)}");
            req.Headers.Add("X-Goog-Api-Key", apiKey);
            req.Headers.Add("X-Goog-FieldMask", PlaceDetailsFieldMask);

            using var resp = await client.SendAsync(req, ct);
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(ct);
                logger.LogWarning("Place details request failed for {PlaceId} with status {StatusCode}. Body: {Body}", placeId, (int)resp.StatusCode, body);
                return null;
            }

            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            var place = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
            var root = place.RootElement;

            var types = root.TryGetProperty("types", out var t) && t.ValueKind == JsonValueKind.Array
                ? string.Join(",", t.EnumerateArray().Select(x => x.GetString()).Where(x => !string.IsNullOrWhiteSpace(x)))
                : string.Empty;

            int? photoCount = root.TryGetProperty("photos", out var photos) && photos.ValueKind == JsonValueKind.Array
                ? photos.GetArrayLength()
                : null;

            return new PlaceDetailsEnvelope(
                GetLocalizedText(root, "displayName"),
                root.TryGetProperty("primaryType", out var primaryType) ? primaryType.GetString() : null,
                GetPrimaryCategoryLabel(root),
                types,
                root.TryGetProperty("formattedAddress", out var formattedAddress) ? formattedAddress.GetString() : null,
                root.TryGetProperty("location", out var loc) && loc.TryGetProperty("latitude", out var latEl) ? latEl.GetDecimal() : null,
                root.TryGetProperty("location", out var loc2) && loc2.TryGetProperty("longitude", out var lngEl) ? lngEl.GetDecimal() : null,
                root.TryGetProperty("nationalPhoneNumber", out var phone) ? phone.GetString() : null,
                root.TryGetProperty("websiteUri", out var website) ? website.GetString() : null,
                GetLocalizedText(root, "editorialSummary"),
                photoCount,
                root.TryGetProperty("pureServiceAreaBusiness", out var sab) ? sab.GetBoolean() : null,
                root.TryGetProperty("businessStatus", out var status) ? status.GetString() : null,
                GetWeekdayDescriptions(root));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Place details request failed for {PlaceId}.", placeId);
            return null;
        }
    }

    private sealed record PlaceDetailsEnvelope(
        string? DisplayName,
        string? PrimaryType,
        string? PrimaryCategory,
        string TypesCsv,
        string? FormattedAddress,
        decimal? Lat,
        decimal? Lng,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        string? Description,
        int? PhotoCount,
        bool? IsServiceAreaBusiness,
        string? BusinessStatus,
        IReadOnlyList<string> RegularOpeningHours);

    private static readonly HashSet<string> GenericTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "point_of_interest",
        "establishment",
        "service",
        "services",
        "professional_services",
        "business_to_business_service",
        "local_services"
    };

    private static readonly HashSet<string> GenericCategories = new(StringComparer.OrdinalIgnoreCase)
    {
        "point of interest",
        "establishment",
        "service",
        "services",
        "business",
        "professional services",
        "local services"
    };
}
