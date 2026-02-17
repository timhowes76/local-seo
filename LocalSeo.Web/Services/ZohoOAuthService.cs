using System.Security.Cryptography;
using System.Text.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IZohoOAuthService
{
    string BuildConnectUrl(string userIdentity);
    Task<ZohoOAuthConnectionResult> CompleteConnectionAsync(string code, string state, string userIdentity, string? location, CancellationToken ct);
}

public sealed record ZohoOAuthConnectionResult(
    bool Success,
    string Message,
    DateTime AccessTokenExpiresAtUtc,
    string RefreshTokenHint,
    string? Location);

public sealed class ZohoOAuthService(
    IHttpClientFactory httpClientFactory,
    IOptions<ZohoOAuthOptions> options,
    IZohoTokenStore tokenStore,
    IMemoryCache cache,
    ILogger<ZohoOAuthService> logger) : IZohoOAuthService
{
    private static readonly TimeSpan StateTtl = TimeSpan.FromMinutes(15);

    public string BuildConnectUrl(string userIdentity)
    {
        var cfg = options.Value;
        var accountsBaseUrl = RequireConfigured(cfg.AccountsBaseUrl, "ZohoOAuth:AccountsBaseUrl");
        var clientId = RequireConfigured(cfg.ClientId, "ZohoOAuth:ClientId");
        var redirectUri = RequireConfigured(cfg.RedirectUri, "ZohoOAuth:RedirectUri");
        var scopes = RequireConfigured(cfg.Scopes, "ZohoOAuth:Scopes");

        var state = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        cache.Set(BuildStateCacheKey(state), new OAuthStateRecord(NormalizeUserIdentity(userIdentity)), StateTtl);

        var query = new Dictionary<string, string>
        {
            ["client_id"] = clientId,
            ["scope"] = scopes,
            ["response_type"] = "code",
            ["access_type"] = "offline",
            ["redirect_uri"] = redirectUri,
            ["state"] = state
        };

        return $"{BuildUrl(accountsBaseUrl, "/oauth/v2/auth")}?{ToQueryString(query)}";
    }

    public async Task<ZohoOAuthConnectionResult> CompleteConnectionAsync(string code, string state, string userIdentity, string? location, CancellationToken ct)
    {
        var cfg = options.Value;
        var accountsBaseUrl = RequireConfigured(cfg.AccountsBaseUrl, "ZohoOAuth:AccountsBaseUrl");
        var clientId = RequireConfigured(cfg.ClientId, "ZohoOAuth:ClientId");
        var clientSecret = RequireConfigured(cfg.ClientSecret, "ZohoOAuth:ClientSecret");
        var redirectUri = RequireConfigured(cfg.RedirectUri, "ZohoOAuth:RedirectUri");

        var normalizedCode = NormalizeRequired(code, "Zoho OAuth code is missing.");
        var normalizedState = NormalizeRequired(state, "Zoho OAuth state is missing.");
        var normalizedUser = NormalizeUserIdentity(userIdentity);

        var cacheKey = BuildStateCacheKey(normalizedState);
        if (!cache.TryGetValue<OAuthStateRecord>(cacheKey, out var stateRecord) || stateRecord is null)
            throw new InvalidOperationException("Invalid or expired Zoho OAuth state. Retry /integrations/zoho/connect.");
        cache.Remove(cacheKey);

        if (!string.Equals(stateRecord.UserIdentity, normalizedUser, StringComparison.Ordinal))
            throw new InvalidOperationException("Zoho OAuth state does not match the current user.");

        var tokenEndpoint = BuildUrl(accountsBaseUrl, "/oauth/v2/token");
        var tokenQuery = new Dictionary<string, string>
        {
            ["code"] = normalizedCode,
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["redirect_uri"] = redirectUri,
            ["grant_type"] = "authorization_code"
        };

        using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, $"{tokenEndpoint}?{ToQueryString(tokenQuery)}");
        var client = httpClientFactory.CreateClient();
        using var tokenResponse = await client.SendAsync(tokenRequest, ct);
        var body = await tokenResponse.Content.ReadAsStringAsync(ct);
        if (!tokenResponse.IsSuccessStatusCode)
            throw new InvalidOperationException($"Zoho token exchange failed with HTTP {(int)tokenResponse.StatusCode}. {SummarizeBody(body)}");

        using var doc = JsonDocument.Parse(body);
        ThrowIfZohoError(doc.RootElement, "Zoho token exchange failed");
        var accessToken = ReadRequiredString(doc.RootElement, "access_token", "Zoho did not return access_token.");
        var refreshToken = ReadOptionalString(doc.RootElement, "refresh_token");
        var expiresInSeconds = ReadExpiresInSeconds(doc.RootElement);
        var expiresAtUtc = DateTime.UtcNow.AddSeconds(expiresInSeconds);

        if (string.IsNullOrWhiteSpace(refreshToken))
            refreshToken = (await tokenStore.LoadAsync(ct))?.RefreshToken;
        if (string.IsNullOrWhiteSpace(refreshToken))
            throw new InvalidOperationException("Zoho did not return a refresh token and no stored refresh token exists.");

        await tokenStore.SaveAsync(new ZohoTokenSnapshot(refreshToken, accessToken, expiresAtUtc), ct);
        logger.LogInformation("Zoho OAuth connection completed. Access token expires at {ExpiresAtUtc}.", expiresAtUtc);

        return new ZohoOAuthConnectionResult(
            true,
            "Connected successfully.",
            expiresAtUtc,
            MaskToken(refreshToken),
            NormalizeOrNull(location));
    }

    private static string ReadRequiredString(JsonElement root, string propertyName, string errorMessage)
    {
        var value = ReadOptionalString(root, propertyName);
        if (!string.IsNullOrWhiteSpace(value))
            return value;
        throw new InvalidOperationException(errorMessage);
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
        return NormalizeOrNull(node.GetString());
    }

    private static int ReadExpiresInSeconds(JsonElement root)
    {
        if (TryReadInt(root, "expires_in", out var expiresIn) && expiresIn > 0)
            return expiresIn;
        if (TryReadInt(root, "expires_in_sec", out var expiresInSec) && expiresInSec > 0)
            return expiresInSec;
        return 3600;
    }

    private static bool TryReadInt(JsonElement root, string propertyName, out int value)
    {
        value = 0;
        if (!root.TryGetProperty(propertyName, out var node) || node.ValueKind == JsonValueKind.Null)
            return false;
        if (node.ValueKind == JsonValueKind.Number && node.TryGetInt32(out value))
            return true;
        if (node.ValueKind == JsonValueKind.String && int.TryParse(node.GetString(), out value))
            return true;
        return false;
    }

    private static string BuildStateCacheKey(string state)
        => $"zoho-oauth-state:{state}";

    private static string RequireConfigured(string? value, string keyName)
    {
        var normalized = NormalizeOrNull(value);
        if (normalized is not null)
            return normalized;
        throw new InvalidOperationException($"{keyName} is missing.");
    }

    private static string NormalizeRequired(string? value, string errorMessage)
    {
        var normalized = NormalizeOrNull(value);
        if (normalized is not null)
            return normalized;
        throw new InvalidOperationException(errorMessage);
    }

    private static string NormalizeUserIdentity(string userIdentity)
        => (userIdentity ?? string.Empty).Trim().ToLowerInvariant();

    private static string BuildUrl(string baseUrl, string path)
        => $"{baseUrl.Trim().TrimEnd('/')}/{path.TrimStart('/')}";

    private static string ToQueryString(IReadOnlyDictionary<string, string> values)
        => string.Join("&", values.Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value)}"));

    private static string? NormalizeOrNull(string? value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private static string MaskToken(string token)
    {
        if (token.Length <= 8)
            return "****";
        return $"{token[..4]}...{token[^4..]}";
    }

    private static string SummarizeBody(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return string.Empty;
        var compact = body.Replace(Environment.NewLine, " ").Trim();
        return compact.Length <= 300 ? compact : $"{compact[..300]}...";
    }

    private sealed record OAuthStateRecord(string UserIdentity);
}
