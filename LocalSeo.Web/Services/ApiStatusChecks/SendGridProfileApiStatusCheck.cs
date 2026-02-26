using System.Net;
using System.Net.Http.Headers;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class SendGridProfileApiStatusCheck(
    IHttpClientFactory httpClientFactory,
    IOptions<SendGridOptions> options) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "sendgrid.profile",
        DisplayName: "SendGrid API",
        Category: "Email",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: 2000);

    public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var apiKey = (options.Value.ApiKey ?? string.Empty).Trim();
        if (apiKey.Length == 0)
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
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://api.sendgrid.com/v3/user/profile");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

        using var response = await client.SendAsync(request, ct);
        var latencyMs = (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds);
        var statusCode = (int)response.StatusCode;
        if (response.IsSuccessStatusCode)
        {
            return new ApiCheckRunResult(
                ApiHealthStatus.Up,
                latencyMs,
                "Reachable",
                null,
                statusCode,
                null,
                null);
        }

        var status = response.StatusCode == HttpStatusCode.TooManyRequests
            ? ApiHealthStatus.Degraded
            : ApiHealthStatus.Down;
        var message = response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden
            ? "Authentication failed"
            : $"HTTP {statusCode}";

        return new ApiCheckRunResult(
            status,
            latencyMs,
            message,
            null,
            statusCode,
            "HttpFailure",
            $"SendGrid returned HTTP {statusCode}.");
    }
}

