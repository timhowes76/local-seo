using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IDataForSeoAccountStatusService
{
    Task<HomeDashboardViewModel> GetDashboardAsync(CancellationToken ct);
}

public sealed class DataForSeoAccountStatusService(
    IHttpClientFactory httpClientFactory,
    IOptions<DataForSeoOptions> options,
    IMemoryCache memoryCache,
    ILogger<DataForSeoAccountStatusService> logger) : IDataForSeoAccountStatusService
{
    private const string CacheKey = "home:dataforseo-dashboard";
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(1);

    public async Task<HomeDashboardViewModel> GetDashboardAsync(CancellationToken ct)
    {
        if (memoryCache.TryGetValue<HomeDashboardViewModel>(CacheKey, out var cached) && cached is not null)
            return cached;

        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.Login) || string.IsNullOrWhiteSpace(cfg.Password))
        {
            return new HomeDashboardViewModel
            {
                AccountError = "DataForSEO credentials are not configured.",
                ApiStatuses =
                [
                    new DataForSeoApiStatusRow(
                        "DataForSEO API",
                        "Unavailable",
                        "DataForSEO credentials are not configured.",
                        null,
                        null)
                ],
                RetrievedAtUtc = DateTime.UtcNow
            };
        }

        var balanceTask = GetBalanceAsync(cfg, ct);
        var statusTask = GetStatusesAsync(cfg, ct);

        await Task.WhenAll(balanceTask, statusTask);
        var (balance, balanceError) = balanceTask.Result;
        var statuses = statusTask.Result;

        var vm = new HomeDashboardViewModel
        {
            DataForSeoBalanceUsd = balance,
            DataForSeoBalanceDisplay = balance.HasValue ? balance.Value.ToString("0.00", CultureInfo.InvariantCulture) : null,
            AccountError = balanceError,
            ApiStatuses = statuses,
            RetrievedAtUtc = DateTime.UtcNow
        };

        memoryCache.Set(CacheKey, vm, CacheDuration);
        return vm;
    }

    private async Task<(decimal? Balance, string? Error)> GetBalanceAsync(DataForSeoOptions cfg, CancellationToken ct)
    {
        try
        {
            var client = httpClientFactory.CreateClient();
            using var request = CreateRequest(HttpMethod.Get, BuildApiUrl(cfg.BaseUrl, "/v3/appendix/user_data"), cfg);
            using var response = await client.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
                return (null, $"Balance endpoint failed with HTTP {(int)response.StatusCode}.");

            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array || tasks.GetArrayLength() == 0)
                return (null, "Balance endpoint returned no tasks.");

            var task = tasks[0];
            if (!task.TryGetProperty("result", out var result) || result.ValueKind != JsonValueKind.Array || result.GetArrayLength() == 0)
                return (null, "Balance endpoint returned no result.");

            var moneyNode = result[0].TryGetProperty("money", out var money) ? money : default;
            if (moneyNode.ValueKind != JsonValueKind.Object)
                return (null, "Balance endpoint returned no money object.");

            if (!moneyNode.TryGetProperty("balance", out var balanceNode))
                return (null, "Balance endpoint returned no balance value.");

            if (balanceNode.TryGetDecimal(out var dec))
                return (dec, null);

            if (decimal.TryParse(balanceNode.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
                return (parsed, null);

            return (null, "Balance value could not be parsed.");
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Failed to retrieve DataForSEO balance.");
            return (null, "Could not retrieve DataForSEO account balance.");
        }
    }

    private async Task<IReadOnlyList<DataForSeoApiStatusRow>> GetStatusesAsync(DataForSeoOptions cfg, CancellationToken ct)
    {
        try
        {
            var client = httpClientFactory.CreateClient();
            using var request = CreateRequest(HttpMethod.Get, BuildApiUrl(cfg.BaseUrl, "/v3/appendix/status"), cfg);
            using var response = await client.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                var desc = GetStatusDescription((int)response.StatusCode, response.ReasonPhrase);
                return [new DataForSeoApiStatusRow("DataForSEO API", response.ReasonPhrase ?? "Error", desc, (int)response.StatusCode, response.ReasonPhrase)];
            }

            using var doc = JsonDocument.Parse(body);
            var rows = ParseStatusRows(doc.RootElement);
            if (rows.Count == 0)
                rows.Add(new DataForSeoApiStatusRow("DataForSEO API", "Unknown", "The API response did not include a status payload.", null, null));

            return rows;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Failed to retrieve DataForSEO API status.");
            return [new DataForSeoApiStatusRow("DataForSEO API", "Unavailable", "Sorry, we could not retrieve DataForSEO status right now. Please try again later.", null, null)];
        }
    }

    private static List<DataForSeoApiStatusRow> ParseStatusRows(JsonElement root)
    {
        var rows = new List<DataForSeoApiStatusRow>();

        if (!root.TryGetProperty("tasks", out var tasks) || tasks.ValueKind != JsonValueKind.Array)
            return rows;

        foreach (var task in tasks.EnumerateArray())
        {
            if (!task.TryGetProperty("result", out var resultArray) || resultArray.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var item in resultArray.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                var serviceName = FirstNonEmpty(
                    GetString(item, "api"),
                    GetString(item, "service"),
                    GetString(item, "name"),
                    GetString(item, "se"),
                    "DataForSEO");
                var statusLabel = FirstNonEmpty(
                    GetString(item, "status"),
                    GetString(item, "status_message"),
                    GetString(item, "message"),
                    "Unknown");
                var statusCode = GetInt(item, "status_code") ?? GetInt(item, "code") ?? GetInt(item, "http_code");
                var description = GetStatusDescription(statusCode, statusLabel);

                rows.Add(new DataForSeoApiStatusRow(
                    serviceName,
                    statusLabel,
                    description,
                    statusCode,
                    FirstNonEmpty(GetString(item, "status_message"), GetString(item, "message"))));
            }
        }

        return rows;
    }

    private static string GetStatusDescription(int? statusCode, string? statusLabel)
    {
        if (statusCode.HasValue)
        {
            return statusCode.Value switch
            {
                200 => "Everything is working normally.",
                400 => "Sorry, the request was invalid. Please verify inputs and try again.",
                401 => "Sorry, authentication failed. Please verify API credentials.",
                403 => "Sorry, access was denied for this request.",
                404 => "Sorry, the requested endpoint could not be found.",
                408 => "Sorry, the request timed out. Please try again.",
                409 => "Sorry, there is a request conflict. Please retry shortly.",
                422 => "Sorry, the request could not be processed due to invalid data.",
                429 => "Sorry, too many requests were sent. Please wait and try again.",
                500 => "Sorry, we could not process your request due to the internal server error. Please, try again later.",
                502 => "Sorry, the upstream gateway returned an invalid response. Please try again later.",
                503 => "Sorry, the service is temporarily unavailable. Please try again later.",
                504 => "Sorry, the upstream service timed out. Please try again later.",
                _ => $"Status code {statusCode.Value} returned by the API."
            };
        }

        var text = (statusLabel ?? string.Empty).Trim();
        if (text.Length == 0)
            return "The API did not provide a status description.";

        return text.ToLowerInvariant() switch
        {
            "ok" => "Everything is working normally.",
            "internal server error" => "Sorry, we could not process your request due to the internal server error. Please, try again later.",
            "service unavailable" => "Sorry, the service is temporarily unavailable. Please try again later.",
            "bad gateway" => "Sorry, the upstream gateway returned an invalid response. Please try again later.",
            "gateway timeout" => "Sorry, the upstream service timed out. Please try again later.",
            "too many requests" => "Sorry, too many requests were sent. Please wait and try again.",
            "unauthorized" => "Sorry, authentication failed. Please verify API credentials.",
            "forbidden" => "Sorry, access was denied for this request.",
            "not found" => "Sorry, the requested endpoint could not be found.",
            "bad request" => "Sorry, the request was invalid. Please verify inputs and try again.",
            _ => $"Status reported: {text}."
        };
    }

    private static HttpRequestMessage CreateRequest(HttpMethod method, string url, DataForSeoOptions cfg)
    {
        var request = new HttpRequestMessage(method, url);
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{cfg.Login}:{cfg.Password}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        return request;
    }

    private static string BuildApiUrl(string? baseUrl, string path)
    {
        var trimmedBase = (baseUrl ?? string.Empty).Trim().TrimEnd('/');
        return $"{trimmedBase}/{path.TrimStart('/')}";
    }

    private static string? GetString(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;

        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Number => node.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private static int? GetInt(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;

        if (node.TryGetInt32(out var intValue))
            return intValue;

        if (int.TryParse(node.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            return parsed;

        return null;
    }

    private static string FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
                return value.Trim();
        }

        return string.Empty;
    }
}
