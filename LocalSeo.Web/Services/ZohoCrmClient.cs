using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface IZohoCrmClient
{
    Task<JsonDocument> SearchLeadsAsync(string criteria, CancellationToken ct = default);
    Task<JsonDocument> CreateLeadAsync(object leadPayload, CancellationToken ct = default);
    Task<JsonDocument> PingAsync(CancellationToken ct = default);
}

public sealed class ZohoCrmClient(
    HttpClient httpClient,
    IZohoTokenService tokenService,
    ILogger<ZohoCrmClient> logger) : IZohoCrmClient
{
    public Task<JsonDocument> SearchLeadsAsync(string criteria, CancellationToken ct = default)
    {
        var normalizedCriteria = (criteria ?? string.Empty).Trim();
        if (normalizedCriteria.Length == 0)
            throw new InvalidOperationException("Lead search criteria is required.");

        var encodedCriteria = Uri.EscapeDataString(normalizedCriteria);
        return SendWithAuthRetryAsync(
            () => new HttpRequestMessage(HttpMethod.Get, $"Leads/search?criteria={encodedCriteria}"),
            ct);
    }

    public Task<JsonDocument> CreateLeadAsync(object leadPayload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(leadPayload);

        return SendWithAuthRetryAsync(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "Leads");
            request.Content = JsonContent.Create(new { data = new[] { leadPayload } });
            return request;
        }, ct);
    }

    public Task<JsonDocument> PingAsync(CancellationToken ct = default)
        => SendWithAuthRetryAsync(() => new HttpRequestMessage(HttpMethod.Get, "Leads?per_page=1&page=1"), ct);

    private async Task<JsonDocument> SendWithAuthRetryAsync(Func<HttpRequestMessage> requestFactory, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(requestFactory);

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var accessToken = attempt == 1
                ? await tokenService.GetValidAccessTokenAsync(ct)
                : await tokenService.ForceRefreshAccessTokenAsync(ct);

            using var request = requestFactory();
            request.Headers.Authorization = new AuthenticationHeaderValue("Zoho-oauthtoken", accessToken);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            using var response = await httpClient.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (response.IsSuccessStatusCode)
                return ParseJsonOrDefault(body);

            var requestId = GetRequestId(response);
            if (attempt == 1 && IsInvalidTokenResponse(response.StatusCode, body))
            {
                logger.LogWarning(
                    "Zoho CRM returned an invalid token response. Status {StatusCode}; RequestId {RequestId}; Body {Body}",
                    (int)response.StatusCode,
                    requestId ?? "n/a",
                    SummarizeBody(body));
                continue;
            }

            logger.LogWarning(
                "Zoho CRM request failed. Status {StatusCode}; RequestId {RequestId}; Body {Body}",
                (int)response.StatusCode,
                requestId ?? "n/a",
                SummarizeBody(body));
            throw new InvalidOperationException($"Zoho CRM request failed with HTTP {(int)response.StatusCode}.");
        }

        throw new InvalidOperationException("Zoho CRM request failed after token refresh retry.");
    }

    private static JsonDocument ParseJsonOrDefault(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return JsonDocument.Parse("{}");
        return JsonDocument.Parse(body);
    }

    private static bool IsInvalidTokenResponse(HttpStatusCode statusCode, string body)
    {
        if (statusCode == HttpStatusCode.Unauthorized)
            return true;
        if (body.Contains("INVALID_TOKEN", StringComparison.OrdinalIgnoreCase))
            return true;

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("code", out var code) &&
                code.ValueKind == JsonValueKind.String &&
                string.Equals(code.GetString(), "INVALID_TOKEN", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }

    private static string? GetRequestId(HttpResponseMessage response)
    {
        if (TryGetHeader(response, "X-REQUEST-ID", out var requestId))
            return requestId;
        if (TryGetHeader(response, "X-ZOHO-REQUEST-ID", out requestId))
            return requestId;

        foreach (var header in response.Headers)
        {
            if (!header.Key.Contains("request", StringComparison.OrdinalIgnoreCase))
                continue;

            var value = header.Value.FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }

    private static bool TryGetHeader(HttpResponseMessage response, string key, out string? value)
    {
        value = null;
        if (!response.Headers.TryGetValues(key, out var values))
            return false;

        value = values.FirstOrDefault();
        return !string.IsNullOrWhiteSpace(value);
    }

    private static string SummarizeBody(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return string.Empty;
        var compact = body.Replace(Environment.NewLine, " ").Trim();
        return compact.Length <= 500 ? compact : $"{compact[..500]}...";
    }
}
