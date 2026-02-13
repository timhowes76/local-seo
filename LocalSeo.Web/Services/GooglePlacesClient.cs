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
}

public sealed class GooglePlacesClient(IHttpClientFactory factory, IOptions<GoogleOptions> options, ILogger<GooglePlacesClient> logger) : IGooglePlacesClient
{
    private const string FieldMask = "places.id,places.displayName,places.primaryType,places.types,places.rating,places.userRatingCount,places.formattedAddress,places.location";

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
                    var types = item.TryGetProperty("types", out var t) && t.ValueKind == JsonValueKind.Array
                        ? string.Join(",", t.EnumerateArray().Select(x => x.GetString()).Where(x => !string.IsNullOrWhiteSpace(x)))
                        : string.Empty;
                    string? display = item.TryGetProperty("displayName", out var dn) && dn.TryGetProperty("text", out var txt) ? txt.GetString() : null;
                    decimal? lat = item.TryGetProperty("location", out var loc) && loc.TryGetProperty("latitude", out var latEl) ? latEl.GetDecimal() : null;
                    decimal? lng = item.TryGetProperty("location", out var loc2) && loc2.TryGetProperty("longitude", out var lngEl) ? lngEl.GetDecimal() : null;
                    decimal? rating = item.TryGetProperty("rating", out var r) ? r.GetDecimal() : null;
                    int? cnt = item.TryGetProperty("userRatingCount", out var c) ? c.GetInt32() : null;
                    list.Add(new GooglePlace(
                        item.GetProperty("id").GetString()!,
                        display,
                        item.TryGetProperty("primaryType", out var pt) ? pt.GetString() : null,
                        types,
                        rating,
                        cnt,
                        item.TryGetProperty("formattedAddress", out var fa) ? fa.GetString() : null,
                        lat,
                        lng));
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

    private static bool IsTransient(HttpStatusCode statusCode)
        => statusCode == HttpStatusCode.TooManyRequests || (int)statusCode >= 500;
}
