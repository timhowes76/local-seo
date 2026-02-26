using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed class AppleMapsStatusChecker(
    IHttpClientFactory httpClientFactory,
    IOptions<AppleMapsOptions> options) : IExternalApiStatusChecker
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan TokenLifetime = TimeSpan.FromDays(180);
    private static readonly TimeSpan TokenRefreshWindow = TimeSpan.FromDays(7);
    private static readonly object TokenGate = new();

    private string? cachedToken;
    private DateTimeOffset cachedTokenExpiryUtc;
    private const string TokenEndpoint = "https://maps-api.apple.com/v1/token";
    private const string SearchEndpoint = "https://maps-api.apple.com/v1/search?q=test&limit=1";

    public string Name => "Apple Maps Server API";

    public async Task<ApiStatusResult> CheckAsync(CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var authToken = GetOrCreateToken(forceRefresh: false);
            var tokenResult = await SendTokenRequestAsync(authToken, ct);
            if (tokenResult.Response.StatusCode == HttpStatusCode.Unauthorized)
            {
                tokenResult.Response.Dispose();
                authToken = GetOrCreateToken(forceRefresh: true);
                tokenResult = await SendTokenRequestAsync(authToken, ct);
            }

            using var tokenResponse = tokenResult.Response;
            if (!tokenResponse.IsSuccessStatusCode)
                return BuildResult(tokenResponse, TokenEndpoint, stopwatch.ElapsedMilliseconds);

            if (!string.IsNullOrWhiteSpace(tokenResult.Error))
            {
                return new ApiStatusResult(
                    Name,
                    IsUp: false,
                    IsDegraded: false,
                    CheckedAtUtc: DateTime.UtcNow,
                    LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                    EndpointCalled: TokenEndpoint,
                    HttpStatusCode: (int)tokenResponse.StatusCode,
                    Error: tokenResult.Error);
            }

            using var searchResponse = await SendRequestAsync(SearchEndpoint, tokenResult.AccessToken!, ct);
            return BuildResult(searchResponse, SearchEndpoint, stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: SearchEndpoint,
                HttpStatusCode: null,
                Error: "Request timed out after 3 seconds.");
        }
        catch (Exception)
        {
            return new ApiStatusResult(
                Name,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, stopwatch.ElapsedMilliseconds),
                EndpointCalled: SearchEndpoint,
                HttpStatusCode: null,
                Error: "Apple Maps check failed.");
        }
    }

    private ApiStatusResult BuildResult(HttpResponseMessage response, string endpoint, long latencyMs)
    {
        var statusCode = (int)response.StatusCode;
        if (response.IsSuccessStatusCode)
        {
            return new ApiStatusResult(
                Name,
                IsUp: true,
                IsDegraded: false,
                CheckedAtUtc: DateTime.UtcNow,
                LatencyMs: (int)Math.Max(0, latencyMs),
                EndpointCalled: endpoint,
                HttpStatusCode: statusCode,
                Error: null);
        }

        return new ApiStatusResult(
            Name,
            IsUp: false,
            IsDegraded: response.StatusCode == HttpStatusCode.TooManyRequests,
            CheckedAtUtc: DateTime.UtcNow,
            LatencyMs: (int)Math.Max(0, latencyMs),
            EndpointCalled: endpoint,
            HttpStatusCode: statusCode,
            Error: $"HTTP {statusCode}: {response.ReasonPhrase}");
    }

    private async Task<HttpResponseMessage> SendRequestAsync(string endpoint, string jwt, CancellationToken ct)
    {
        var client = httpClientFactory.CreateClient();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(RequestTimeout);

        using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", jwt);

        return await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token);
    }

    private async Task<(HttpResponseMessage Response, string? AccessToken, string? Error)> SendTokenRequestAsync(string jwt, CancellationToken ct)
    {
        var response = await SendRequestAsync(TokenEndpoint, jwt, ct);
        if (!response.IsSuccessStatusCode)
            return (response, null, null);

        var payload = await response.Content.ReadAsStringAsync(ct);
        try
        {
            using var doc = JsonDocument.Parse(payload);
            if (doc.RootElement.TryGetProperty("accessToken", out var node)
                && node.ValueKind == JsonValueKind.String
                && !string.IsNullOrWhiteSpace(node.GetString()))
            {
                return (response, node.GetString(), null);
            }

            return (response, null, "Apple Maps token response did not include accessToken.");
        }
        catch (JsonException)
        {
            return (response, null, "Apple Maps token response was not valid JSON.");
        }
    }

    private string GetOrCreateToken(bool forceRefresh)
    {
        var now = DateTimeOffset.UtcNow;
        lock (TokenGate)
        {
            if (!forceRefresh
                && !string.IsNullOrWhiteSpace(cachedToken)
                && cachedTokenExpiryUtc - now > TokenRefreshWindow)
            {
                return cachedToken;
            }

            var cfg = options.Value;
            var p8Pem = File.ReadAllText(cfg.P8Path);

            using var ecdsa = ECDsa.Create();
            ecdsa.ImportFromPem(p8Pem);

            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.Add(TokenLifetime);

            var headerJson = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["alg"] = "ES256",
                ["kid"] = cfg.KeyId,
                ["typ"] = "JWT"
            });

            var payloadJson = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["iss"] = cfg.TeamId,
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

            cachedToken = $"{signingInput}.{Base64UrlEncode(signature)}";
            cachedTokenExpiryUtc = expiresAt;
            return cachedToken;
        }
    }

    private static string Base64UrlEncode(byte[] bytes)
    {
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
