using System.Diagnostics;
using System.Net;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed class AzureMapsStatusChecker(
    IHttpClientFactory httpClientFactory,
    IOptions<AzureMapsOptions> options) : IExternalApiStatusChecker
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(8);
    private const string Endpoint = "https://atlas.microsoft.com/search/fuzzy/json?api-version=1.0&query=test&limit=1";

    public string Name => "Azure Maps Search API";

    public async Task<ApiStatusResult> CheckAsync(CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();

        var cfg = options.Value;
        var keys = new List<string>(capacity: 2);
        if (!string.IsNullOrWhiteSpace(cfg.PrimaryKey))
            keys.Add(cfg.PrimaryKey);
        if (!string.IsNullOrWhiteSpace(cfg.SecondaryKey)
            && !string.Equals(cfg.SecondaryKey, cfg.PrimaryKey, StringComparison.Ordinal))
            keys.Add(cfg.SecondaryKey);

        if (keys.Count == 0)
        {
            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: Endpoint,
                HttpStatusCode: null,
                Error: "Azure Maps key is not configured.");
        }

        ApiStatusResult? lastFailure = null;
        foreach (var key in keys)
        {
            var firstAttempt = await CheckOnceAsync(key, stopwatch, ct);
            if (firstAttempt.IsUp)
                return firstAttempt;

            lastFailure = firstAttempt;
            if (firstAttempt.Error?.StartsWith("Request timed out", StringComparison.OrdinalIgnoreCase) == true)
            {
                var retryAttempt = await CheckOnceAsync(key, stopwatch, ct);
                if (retryAttempt.IsUp)
                    return retryAttempt;

                lastFailure = retryAttempt;
            }
        }

        return lastFailure ?? new ApiStatusResult(
            Name,
            IsUp: false,
            IsDegraded: false,
            CheckedAtUtc: DateTime.UtcNow,
            LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
            EndpointCalled: Endpoint,
            HttpStatusCode: null,
            Error: "Azure Maps check failed.");
    }

    private async Task<ApiStatusResult> CheckOnceAsync(string key, Stopwatch stopwatch, CancellationToken ct)
    {
        try
        {
            var client = httpClientFactory.CreateClient();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);

            using var request = new HttpRequestMessage(HttpMethod.Get, Endpoint);
            request.Headers.TryAddWithoutValidation("subscription-key", key);

            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token);
            var statusCode = (int)response.StatusCode;

            if (response.IsSuccessStatusCode)
            {
                return new ApiStatusResult(
                    Name,
                    IsUp: true,
                    IsDegraded: false,
                    CheckedAtUtc: DateTime.UtcNow,
                    LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                    EndpointCalled: Endpoint,
                    HttpStatusCode: statusCode,
                    Error: null);
            }

            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: response.StatusCode == HttpStatusCode.TooManyRequests,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: Endpoint,
                HttpStatusCode: statusCode,
                Error: $"HTTP {statusCode}: {response.ReasonPhrase}");
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: Endpoint,
                HttpStatusCode: null,
                Error: $"Request timed out after {(int)RequestTimeout.TotalSeconds} seconds.");
        }
        catch (Exception ex)
        {
            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: Endpoint,
                HttpStatusCode: null,
                Error: ex.Message);
        }
    }
}
