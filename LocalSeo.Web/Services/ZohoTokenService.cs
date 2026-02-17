using System.Text.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IZohoTokenService
{
    Task<string> GetValidAccessTokenAsync(CancellationToken ct);
    Task<string> ForceRefreshAccessTokenAsync(CancellationToken ct);
}

public sealed class ZohoTokenService(
    IHttpClientFactory httpClientFactory,
    IOptions<ZohoOAuthOptions> options,
    IZohoTokenStore tokenStore,
    ILogger<ZohoTokenService> logger) : IZohoTokenService
{
    private static readonly TimeSpan ExpirySafetySkew = TimeSpan.FromMinutes(2);
    private readonly SemaphoreSlim refreshLock = new(1, 1);

    public async Task<string> GetValidAccessTokenAsync(CancellationToken ct)
    {
        var tokens = await tokenStore.LoadAsync(ct)
            ?? throw new InvalidOperationException("Zoho OAuth tokens are missing. Complete /integrations/zoho/connect first.");

        if (HasUsableAccessToken(tokens, DateTime.UtcNow))
            return tokens.AccessToken!;

        return await RefreshWithLockAsync(forceRefresh: false, ct);
    }

    public Task<string> ForceRefreshAccessTokenAsync(CancellationToken ct)
        => RefreshWithLockAsync(forceRefresh: true, ct);

    private async Task<string> RefreshWithLockAsync(bool forceRefresh, CancellationToken ct)
    {
        await refreshLock.WaitAsync(ct);
        try
        {
            var tokens = await tokenStore.LoadAsync(ct)
                ?? throw new InvalidOperationException("Zoho OAuth tokens are missing. Complete /integrations/zoho/connect first.");

            if (!forceRefresh && HasUsableAccessToken(tokens, DateTime.UtcNow))
                return tokens.AccessToken!;

            if (string.IsNullOrWhiteSpace(tokens.RefreshToken))
                throw new InvalidOperationException("Zoho refresh token is missing. Reconnect via /integrations/zoho/connect.");

            return await RefreshAccessTokenAsync(tokens.RefreshToken!, ct);
        }
        finally
        {
            refreshLock.Release();
        }
    }

    private async Task<string> RefreshAccessTokenAsync(string refreshToken, CancellationToken ct)
    {
        var cfg = options.Value;
        var accountsBaseUrl = RequireConfigured(cfg.AccountsBaseUrl, "ZohoOAuth:AccountsBaseUrl");
        var clientId = RequireConfigured(cfg.ClientId, "ZohoOAuth:ClientId");
        var clientSecret = RequireConfigured(cfg.ClientSecret, "ZohoOAuth:ClientSecret");
        var tokenEndpoint = $"{accountsBaseUrl.TrimEnd('/')}/oauth/v2/token";

        var query = new Dictionary<string, string>
        {
            ["refresh_token"] = refreshToken,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["grant_type"] = "refresh_token"
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, $"{tokenEndpoint}?{ToQueryString(query)}");
        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Zoho access token refresh failed with HTTP {(int)response.StatusCode}. {SummarizeBody(body)}");

        using var doc = JsonDocument.Parse(body);
        ThrowIfZohoError(doc.RootElement, "Zoho access token refresh failed");
        var accessToken = ReadRequiredString(doc.RootElement, "access_token", "Zoho token refresh did not return access_token.");
        var expiresInSeconds = ReadExpiresInSeconds(doc.RootElement);
        var expiresAtUtc = DateTime.UtcNow.AddSeconds(expiresInSeconds);

        await tokenStore.SaveAsync(new ZohoTokenSnapshot(refreshToken, accessToken, expiresAtUtc), ct);
        logger.LogInformation("Zoho access token refreshed. Expires at {ExpiresAtUtc}.", expiresAtUtc);
        return accessToken;
    }

    private static bool HasUsableAccessToken(ZohoTokenSnapshot snapshot, DateTime utcNow)
    {
        if (string.IsNullOrWhiteSpace(snapshot.AccessToken) || !snapshot.AccessTokenExpiresAtUtc.HasValue)
            return false;
        return snapshot.AccessTokenExpiresAtUtc.Value > utcNow.Add(ExpirySafetySkew);
    }

    private static string ReadRequiredString(JsonElement root, string propertyName, string errorMessage)
    {
        if (!root.TryGetProperty(propertyName, out var node) || node.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException(errorMessage);
        var value = (node.GetString() ?? string.Empty).Trim();
        if (value.Length == 0)
            throw new InvalidOperationException(errorMessage);
        return value;
    }

    private static void ThrowIfZohoError(JsonElement root, string context)
    {
        var error = ReadOptionalString(root, "error") ?? ReadOptionalString(root, "code");
        if (string.IsNullOrWhiteSpace(error))
            return;

        var description = ReadOptionalString(root, "error_description")
            ?? ReadOptionalString(root, "message");
        var details = string.IsNullOrWhiteSpace(description) ? error : $"{error} ({description})";
        throw new InvalidOperationException($"{context}: {details}.");
    }

    private static string? ReadOptionalString(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return null;
        if (node.ValueKind != JsonValueKind.String)
            return null;

        var value = (node.GetString() ?? string.Empty).Trim();
        return value.Length == 0 ? null : value;
    }

    private static int ReadExpiresInSeconds(JsonElement root)
    {
        if (TryReadPositiveInt(root, "expires_in", out var expiresIn))
            return expiresIn;
        if (TryReadPositiveInt(root, "expires_in_sec", out var expiresInSec))
            return expiresInSec;
        return 3600;
    }

    private static bool TryReadPositiveInt(JsonElement root, string propertyName, out int value)
    {
        value = 0;
        if (!root.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return false;

        if (node.ValueKind == JsonValueKind.Number && node.TryGetInt32(out value))
            return value > 0;
        if (node.ValueKind == JsonValueKind.String && int.TryParse(node.GetString(), out value))
            return value > 0;
        return false;
    }

    private static string RequireConfigured(string? value, string keyName)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length == 0)
            throw new InvalidOperationException($"{keyName} is missing.");
        return normalized;
    }

    private static string ToQueryString(IReadOnlyDictionary<string, string> values)
        => string.Join("&", values.Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value)}"));

    private static string SummarizeBody(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return string.Empty;
        var compact = body.Replace(Environment.NewLine, " ").Trim();
        return compact.Length <= 300 ? compact : $"{compact[..300]}...";
    }
}
