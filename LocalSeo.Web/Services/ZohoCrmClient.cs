using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface IZohoCrmClient
{
    Task<JsonDocument> SearchLeadsAsync(string criteria, CancellationToken ct = default);
    Task<JsonDocument> CreateLeadAsync(object leadPayload, CancellationToken ct = default);
    Task<JsonDocument> UpsertLeadAsync(object leadPayload, IReadOnlyList<string> duplicateCheckFields, CancellationToken ct = default);
    Task<JsonDocument> UpdateLeadAsync(string leadId, object leadPayload, CancellationToken ct = default);
    Task<JsonDocument> GetLeadByIdAsync(string leadId, CancellationToken ct = default);
    Task<JsonDocument> GetLeadFieldsAsync(int page = 1, int perPage = 200, CancellationToken ct = default);
    Task UploadLeadPhotoAsync(string leadId, byte[] content, string fileName, string contentType, CancellationToken ct = default);
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

    public Task<JsonDocument> UpsertLeadAsync(object leadPayload, IReadOnlyList<string> duplicateCheckFields, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(leadPayload);
        ArgumentNullException.ThrowIfNull(duplicateCheckFields);

        var fields = duplicateCheckFields
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (fields.Length == 0)
            throw new InvalidOperationException("At least one duplicate-check field is required for Zoho lead upsert.");

        return SendWithAuthRetryAsync(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "Leads/upsert");
            request.Content = JsonContent.Create(new
            {
                data = new[] { leadPayload },
                duplicate_check_fields = fields
            });
            return request;
        }, ct);
    }

    public Task<JsonDocument> UpdateLeadAsync(string leadId, object leadPayload, CancellationToken ct = default)
    {
        var normalizedLeadId = (leadId ?? string.Empty).Trim();
        if (normalizedLeadId.Length == 0)
            throw new InvalidOperationException("Zoho lead ID is required.");

        ArgumentNullException.ThrowIfNull(leadPayload);
        var payload = BuildUpdatePayload(normalizedLeadId, leadPayload);

        return SendWithAuthRetryAsync(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Put, "Leads");
            request.Content = JsonContent.Create(new { data = new[] { payload } });
            return request;
        }, ct);
    }

    public Task<JsonDocument> GetLeadByIdAsync(string leadId, CancellationToken ct = default)
    {
        var normalizedLeadId = (leadId ?? string.Empty).Trim();
        if (normalizedLeadId.Length == 0)
            throw new InvalidOperationException("Zoho lead ID is required.");

        var encodedLeadId = Uri.EscapeDataString(normalizedLeadId);
        return SendWithAuthRetryAsync(() => new HttpRequestMessage(HttpMethod.Get, $"Leads/{encodedLeadId}"), ct);
    }

    public Task<JsonDocument> GetLeadFieldsAsync(int page = 1, int perPage = 200, CancellationToken ct = default)
    {
        var normalizedPage = Math.Max(1, page);
        var normalizedPerPage = Math.Clamp(perPage, 1, 200);
        return SendWithAuthRetryAsync(
            () => new HttpRequestMessage(HttpMethod.Get, $"settings/fields?module=Leads&page={normalizedPage}&per_page={normalizedPerPage}"),
            ct);
    }

    public Task<JsonDocument> PingAsync(CancellationToken ct = default)
        => SendWithAuthRetryAsync(() => new HttpRequestMessage(HttpMethod.Get, "Leads?per_page=1&page=1"), ct);

    public Task UploadLeadPhotoAsync(string leadId, byte[] content, string fileName, string contentType, CancellationToken ct = default)
    {
        var normalizedLeadId = (leadId ?? string.Empty).Trim();
        if (normalizedLeadId.Length == 0)
            throw new InvalidOperationException("Zoho lead ID is required.");
        if (content is null || content.Length == 0)
            throw new InvalidOperationException("Lead photo content is required.");

        var encodedLeadId = Uri.EscapeDataString(normalizedLeadId);
        var normalizedFileName = string.IsNullOrWhiteSpace(fileName) ? "company-logo.jpg" : fileName.Trim();
        var normalizedContentType = string.IsNullOrWhiteSpace(contentType) ? "image/jpeg" : contentType.Trim();
        return SendWithAuthRetryNoResponseAsync(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"Leads/{encodedLeadId}/photo");
            var multipart = new MultipartFormDataContent();
            var fileContent = new ByteArrayContent(content);
            fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse(normalizedContentType);
            multipart.Add(fileContent, "file", normalizedFileName);
            request.Content = multipart;
            return request;
        }, ct);
    }

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
            throw new InvalidOperationException($"Zoho CRM request failed with HTTP {(int)response.StatusCode}. {SummarizeBody(body)}");
        }

        throw new InvalidOperationException("Zoho CRM request failed after token refresh retry.");
    }

    private async Task SendWithAuthRetryNoResponseAsync(Func<HttpRequestMessage> requestFactory, CancellationToken ct)
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
                return;

            var requestId = GetRequestId(response);
            if (attempt == 1 && IsInvalidTokenResponse(response.StatusCode, body))
            {
                logger.LogWarning(
                    "Zoho CRM photo upload returned an invalid token response. Status {StatusCode}; RequestId {RequestId}; Body {Body}",
                    (int)response.StatusCode,
                    requestId ?? "n/a",
                    SummarizeBody(body));
                continue;
            }

            logger.LogWarning(
                "Zoho CRM photo upload failed. Status {StatusCode}; RequestId {RequestId}; Body {Body}",
                (int)response.StatusCode,
                requestId ?? "n/a",
                SummarizeBody(body));
            throw new InvalidOperationException($"Zoho CRM photo upload failed with HTTP {(int)response.StatusCode}. {SummarizeBody(body)}");
        }

        throw new InvalidOperationException("Zoho CRM photo upload failed after token refresh retry.");
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

    private static Dictionary<string, object?> BuildUpdatePayload(string leadId, object leadPayload)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["id"] = leadId
        };

        switch (leadPayload)
        {
            case IReadOnlyDictionary<string, object> readOnlyPayload:
                foreach (var pair in readOnlyPayload)
                    payload[pair.Key] = pair.Value;
                break;
            case IDictionary<string, object> payloadDictionary:
                foreach (var pair in payloadDictionary)
                    payload[pair.Key] = pair.Value;
                break;
            default:
                throw new InvalidOperationException("Zoho lead update payload must be a dictionary.");
        }

        return payload;
    }
}
