using System.Net;
using System.Text.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class GoogleOAuthApiStatusCheck(
    IHttpClientFactory httpClientFactory,
    IGoogleBusinessProfileOAuthService googleBusinessProfileOAuthService,
    IOptions<GoogleOptions> googleOptions,
    IWebHostEnvironment environment) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "google.oauth",
        DisplayName: "Google OAuth Token Refresh",
        Category: "Google",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: 2500);

    public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var clientId = (googleOptions.Value.ClientId ?? string.Empty).Trim();
        var clientSecret = (googleOptions.Value.ClientSecret ?? string.Empty).Trim();
        var refreshToken = (await googleBusinessProfileOAuthService.GetRefreshTokenAsync(ct) ?? string.Empty).Trim();
        var configuredToken = (googleOptions.Value.BusinessProfileOAuthRefreshToken ?? string.Empty).Trim();
        var localTokenPath = Path.Combine(environment.ContentRootPath, "App_Data", "google-business-profile-refresh-token.json");
        var hasLocalTokenFile = File.Exists(localTokenPath);
        if (clientId.Length == 0 || clientSecret.Length == 0 || refreshToken.Length == 0)
        {
            if (clientId.Length > 0
                && clientSecret.Length > 0
                && refreshToken.Length == 0
                && (configuredToken.Length > 0 || hasLocalTokenFile))
            {
                return new ApiCheckRunResult(
                    ApiHealthStatus.Down,
                    (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                    "Stored token is unreadable",
                    null,
                    null,
                    "SecretUnavailable",
                    "Google OAuth refresh token appears to exist but could not be read on this host.");
            }

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
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://oauth2.googleapis.com/token")
        {
            Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["refresh_token"] = refreshToken,
                ["grant_type"] = "refresh_token"
            })
        };

        using var response = await client.SendAsync(request, ct);
        var latencyMs = (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds);
        var statusCode = (int)response.StatusCode;
        if (!response.IsSuccessStatusCode)
        {
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
                $"Google OAuth token endpoint returned HTTP {statusCode}.");
        }

        var body = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(string.IsNullOrWhiteSpace(body) ? "{}" : body);
        var hasToken =
            doc.RootElement.TryGetProperty("access_token", out var tokenNode)
            && tokenNode.ValueKind == JsonValueKind.String
            && !string.IsNullOrWhiteSpace(tokenNode.GetString());

        return new ApiCheckRunResult(
            hasToken ? ApiHealthStatus.Up : ApiHealthStatus.Down,
            latencyMs,
            hasToken ? "Token refresh succeeded" : "Token refresh failed",
            null,
            statusCode,
            hasToken ? null : "InvalidResponse",
            hasToken ? null : "Google OAuth response did not include an access token.");
    }
}
