using Dapper;
using LocalSeo.Web.Data;
using Microsoft.AspNetCore.DataProtection;

namespace LocalSeo.Web.Services;

public sealed record ZohoTokenSnapshot(
    string? RefreshToken,
    string? AccessToken,
    DateTime? AccessTokenExpiresAtUtc);

public interface IZohoTokenStore
{
    Task<ZohoTokenSnapshot?> LoadAsync(CancellationToken ct);
    Task SaveAsync(ZohoTokenSnapshot snapshot, CancellationToken ct);
}

public sealed class SqlZohoTokenStore(
    ISqlConnectionFactory connectionFactory,
    IDataProtectionProvider dataProtectionProvider,
    ILogger<SqlZohoTokenStore> logger) : IZohoTokenStore
{
    private const string TokenKey = "zoho-crm";
    private readonly IDataProtector protector = dataProtectionProvider.CreateProtector("LocalSeo.Web.ZohoOAuth.RefreshToken.v1");

    public async Task<ZohoTokenSnapshot?> LoadAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<PersistedTokenRow>(new CommandDefinition(@"
SELECT TOP 1
  TokenKey,
  ProtectedRefreshToken,
  AccessToken,
  AccessTokenExpiresAtUtc
FROM dbo.ZohoOAuthToken
WHERE TokenKey = @TokenKey;", new { TokenKey }, cancellationToken: ct));
        if (row is null)
            return null;

        return new ZohoTokenSnapshot(
            UnprotectRefreshToken(row.ProtectedRefreshToken),
            NormalizeOrNull(row.AccessToken),
            row.AccessTokenExpiresAtUtc);
    }

    public async Task SaveAsync(ZohoTokenSnapshot snapshot, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var protectedRefreshToken = ProtectRefreshToken(snapshot.RefreshToken);
        var accessToken = NormalizeOrNull(snapshot.AccessToken);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
IF EXISTS (SELECT 1 FROM dbo.ZohoOAuthToken WHERE TokenKey = @TokenKey)
BEGIN
  UPDATE dbo.ZohoOAuthToken
  SET
    ProtectedRefreshToken = COALESCE(@ProtectedRefreshToken, ProtectedRefreshToken),
    AccessToken = @AccessToken,
    AccessTokenExpiresAtUtc = @AccessTokenExpiresAtUtc,
    UpdatedAtUtc = SYSUTCDATETIME()
  WHERE TokenKey = @TokenKey;
END
ELSE
BEGIN
  INSERT INTO dbo.ZohoOAuthToken(
    TokenKey,
    ProtectedRefreshToken,
    AccessToken,
    AccessTokenExpiresAtUtc,
    UpdatedAtUtc
  )
  VALUES(
    @TokenKey,
    @ProtectedRefreshToken,
    @AccessToken,
    @AccessTokenExpiresAtUtc,
    SYSUTCDATETIME()
  );
END;", new
        {
            TokenKey,
            ProtectedRefreshToken = protectedRefreshToken,
            AccessToken = accessToken,
            AccessTokenExpiresAtUtc = snapshot.AccessTokenExpiresAtUtc
        }, cancellationToken: ct));
    }

    private string? UnprotectRefreshToken(string? protectedRefreshToken)
    {
        if (string.IsNullOrWhiteSpace(protectedRefreshToken))
            return null;

        try
        {
            var refreshToken = protector.Unprotect(protectedRefreshToken);
            return NormalizeOrNull(refreshToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Unable to unprotect Zoho refresh token from store.");
            return null;
        }
    }

    private string? ProtectRefreshToken(string? refreshToken)
    {
        var normalized = NormalizeOrNull(refreshToken);
        if (normalized is null)
            return null;

        return protector.Protect(normalized);
    }

    private static string? NormalizeOrNull(string? value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private sealed record PersistedTokenRow(
        string TokenKey,
        string? ProtectedRefreshToken,
        string? AccessToken,
        DateTime? AccessTokenExpiresAtUtc);
}
