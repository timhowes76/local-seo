using System.Net;
using System.Net.Http.Headers;
using System.Text;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class CompaniesHousePingApiStatusCheck(
    IHttpClientFactory httpClientFactory,
    IOptions<CompaniesHouseOptions> options) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "companieshouse.ping",
        DisplayName: "Companies House API",
        Category: "Company Data",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: 2000);

    public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var cfg = options.Value;
        var apiKey = (cfg.ApiKey ?? string.Empty).Trim();
        var baseUrl = (cfg.BaseUrl ?? string.Empty).Trim().TrimEnd('/');
        if (apiKey.Length == 0 || baseUrl.Length == 0)
        {
            return new ApiCheckRunResult(
                ApiHealthStatus.Unknown,
                (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                "Not configured",
                null,
                null,
                null,
                null);
        }

        var client = httpClientFactory.CreateClient();
        var url = $"{baseUrl}/search/companies?q=limited&items_per_page=1";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:")));
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        using var response = await client.SendAsync(request, ct);
        var latencyMs = (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds);
        var httpStatus = (int)response.StatusCode;

        if (response.IsSuccessStatusCode)
        {
            return new ApiCheckRunResult(
                ApiHealthStatus.Up,
                latencyMs,
                "Reachable",
                null,
                httpStatus,
                null,
                null);
        }

        var mappedStatus = response.StatusCode == HttpStatusCode.TooManyRequests
            ? ApiHealthStatus.Degraded
            : ApiHealthStatus.Down;
        var message = response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden
            ? "Authentication failed"
            : $"HTTP {httpStatus}";

        return new ApiCheckRunResult(
            mappedStatus,
            latencyMs,
            message,
            null,
            httpStatus,
            "HttpFailure",
            $"Companies House returned HTTP {httpStatus}.");
    }
}

