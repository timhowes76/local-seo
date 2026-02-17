using System.Security.Cryptography;
using System.Text.Json;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IGoogleBusinessProfileOAuthService
{
    string BuildConnectUrl(string userIdentity);
    Task<GoogleOAuthConnectionResult> CompleteConnectionAsync(string code, string state, string userIdentity, CancellationToken ct);
    Task<string?> GetRefreshTokenAsync(CancellationToken ct);
}

public interface IGoogleBusinessProfileRefreshTokenStore
{
    Task<string?> GetAsync(CancellationToken ct);
    Task SaveAsync(string refreshToken, CancellationToken ct);
}

public sealed class GoogleBusinessProfileOAuthService(
    IHttpClientFactory httpClientFactory,
    IOptions<GoogleOptions> googleOptions,
    IGoogleBusinessProfileRefreshTokenStore refreshTokenStore,
    IMemoryCache cache) : IGoogleBusinessProfileOAuthService
{
    private const string Scope = "https://www.googleapis.com/auth/business.manage";
    private const string AuthorizeEndpoint = "https://accounts.google.com/o/oauth2/v2/auth";
    private const string TokenEndpoint = "https://oauth2.googleapis.com/token";
    private static readonly TimeSpan StateTtl = TimeSpan.FromMinutes(15);

    public string BuildConnectUrl(string userIdentity)
    {
        var clientId = (googleOptions.Value.ClientId ?? string.Empty).Trim();
        var redirectBaseUrl = (googleOptions.Value.RedirectBaseUrl ?? string.Empty).Trim().TrimEnd('/');

        if (string.IsNullOrWhiteSpace(clientId))
            throw new InvalidOperationException("Google ClientId is missing.");
        if (string.IsNullOrWhiteSpace(redirectBaseUrl))
            throw new InvalidOperationException("Google RedirectBaseUrl is missing.");

        var callbackUri = $"{redirectBaseUrl}/admin/google/oauth/callback";
        var state = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var normalizedUser = NormalizeUserIdentity(userIdentity);

        cache.Set(BuildStateCacheKey(state), new OAuthStateRecord(normalizedUser), StateTtl);

        var query = new Dictionary<string, string>
        {
            ["client_id"] = clientId,
            ["redirect_uri"] = callbackUri,
            ["response_type"] = "code",
            ["scope"] = Scope,
            ["access_type"] = "offline",
            ["prompt"] = "consent",
            ["include_granted_scopes"] = "true",
            ["state"] = state
        };

        return $"{AuthorizeEndpoint}?{ToQueryString(query)}";
    }

    public async Task<GoogleOAuthConnectionResult> CompleteConnectionAsync(string code, string state, string userIdentity, CancellationToken ct)
    {
        var normalizedCode = (code ?? string.Empty).Trim();
        var normalizedState = (state ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedCode))
            throw new InvalidOperationException("Google OAuth code is missing.");
        if (string.IsNullOrWhiteSpace(normalizedState))
            throw new InvalidOperationException("Google OAuth state is missing.");

        var stateCacheKey = BuildStateCacheKey(normalizedState);
        if (!cache.TryGetValue<OAuthStateRecord>(stateCacheKey, out var stateRecord) || stateRecord is null)
            throw new InvalidOperationException("Invalid or expired Google OAuth state. Please retry /admin/google/connect.");
        cache.Remove(stateCacheKey);

        var normalizedUser = NormalizeUserIdentity(userIdentity);
        if (!string.Equals(stateRecord.UserIdentity, normalizedUser, StringComparison.Ordinal))
            throw new InvalidOperationException("Google OAuth state does not match the current user.");

        var clientId = (googleOptions.Value.ClientId ?? string.Empty).Trim();
        var clientSecret = (googleOptions.Value.ClientSecret ?? string.Empty).Trim();
        var redirectBaseUrl = (googleOptions.Value.RedirectBaseUrl ?? string.Empty).Trim().TrimEnd('/');
        if (string.IsNullOrWhiteSpace(clientId))
            throw new InvalidOperationException("Google ClientId is missing.");
        if (string.IsNullOrWhiteSpace(clientSecret))
            throw new InvalidOperationException("Google ClientSecret is missing.");
        if (string.IsNullOrWhiteSpace(redirectBaseUrl))
            throw new InvalidOperationException("Google RedirectBaseUrl is missing.");

        var callbackUri = $"{redirectBaseUrl}/admin/google/oauth/callback";
        var client = httpClientFactory.CreateClient();
        using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, TokenEndpoint)
        {
            Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["code"] = normalizedCode,
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["redirect_uri"] = callbackUri,
                ["grant_type"] = "authorization_code"
            })
        };

        using var tokenResponse = await client.SendAsync(tokenRequest, ct);
        var body = await tokenResponse.Content.ReadAsStringAsync(ct);
        if (!tokenResponse.IsSuccessStatusCode)
            throw new InvalidOperationException($"Google token exchange failed with HTTP {(int)tokenResponse.StatusCode}.");

        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("refresh_token", out var refreshTokenNode) || refreshTokenNode.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("Google did not return a refresh token. Retry connect with consent.");

        var refreshToken = (refreshTokenNode.GetString() ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(refreshToken))
            throw new InvalidOperationException("Google returned an empty refresh token.");

        await refreshTokenStore.SaveAsync(refreshToken, ct);
        return new GoogleOAuthConnectionResult(
            true,
            "Connected successfully.",
            MaskToken(refreshToken));
    }

    public async Task<string?> GetRefreshTokenAsync(CancellationToken ct)
    {
        var configured = (googleOptions.Value.BusinessProfileOAuthRefreshToken ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        return await refreshTokenStore.GetAsync(ct);
    }

    private static string BuildStateCacheKey(string state)
        => $"google-oauth-state:{state}";

    private static string NormalizeUserIdentity(string userIdentity)
        => (userIdentity ?? string.Empty).Trim().ToLowerInvariant();

    private static string ToQueryString(IReadOnlyDictionary<string, string> values)
        => string.Join("&", values.Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value)}"));

    private static string MaskToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
            return "****";
        if (token.Length <= 8)
            return "****";
        return $"{token[..4]}...{token[^4..]}";
    }

    private sealed record OAuthStateRecord(string UserIdentity);
}

public sealed class LocalSecureGoogleRefreshTokenStore(
    IDataProtectionProvider dataProtectionProvider,
    IWebHostEnvironment environment) : IGoogleBusinessProfileRefreshTokenStore
{
    private readonly IDataProtector protector = dataProtectionProvider.CreateProtector("LocalSeo.Web.GoogleBusinessProfile.RefreshToken.v1");
    private readonly string storePath = Path.Combine(environment.ContentRootPath, "App_Data", "google-business-profile-refresh-token.json");

    public async Task<string?> GetAsync(CancellationToken ct)
    {
        if (!File.Exists(storePath))
            return null;

        var json = await File.ReadAllTextAsync(storePath, ct);
        if (string.IsNullOrWhiteSpace(json))
            return null;

        PersistedRefreshTokenPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<PersistedRefreshTokenPayload>(json);
        }
        catch (JsonException)
        {
            return null;
        }

        if (payload is null || string.IsNullOrWhiteSpace(payload.ProtectedRefreshToken))
            return null;

        try
        {
            var token = protector.Unprotect(payload.ProtectedRefreshToken);
            return string.IsNullOrWhiteSpace(token) ? null : token;
        }
        catch
        {
            return null;
        }
    }

    public async Task SaveAsync(string refreshToken, CancellationToken ct)
    {
        var normalized = (refreshToken ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalized))
            throw new InvalidOperationException("Refresh token is required.");

        var directory = Path.GetDirectoryName(storePath) ?? environment.ContentRootPath;
        Directory.CreateDirectory(directory);

        var payload = new PersistedRefreshTokenPayload(
            protector.Protect(normalized),
            DateTime.UtcNow);
        var json = JsonSerializer.Serialize(payload);
        var tempPath = $"{storePath}.tmp";

        await File.WriteAllTextAsync(tempPath, json, ct);
        File.Move(tempPath, storePath, true);
    }

    private sealed record PersistedRefreshTokenPayload(
        string ProtectedRefreshToken,
        DateTime UpdatedAtUtc);
}
