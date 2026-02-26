using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class ZohoCrmPingApiStatusCheck(
    IZohoCrmClient zohoCrmClient,
    IZohoTokenStore zohoTokenStore,
    IOptions<ZohoOAuthOptions> options,
    ISqlConnectionFactory connectionFactory) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "zoho.crm",
        DisplayName: "Zoho CRM API",
        Category: "CRM",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: 2500);

    public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.AccountsBaseUrl)
            || string.IsNullOrWhiteSpace(cfg.CrmApiBaseUrl)
            || string.IsNullOrWhiteSpace(cfg.ClientId)
            || string.IsNullOrWhiteSpace(cfg.ClientSecret))
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

        var tokenSnapshot = await zohoTokenStore.LoadAsync(ct);
        if (tokenSnapshot is null || string.IsNullOrWhiteSpace(tokenSnapshot.RefreshToken))
        {
            var hasStoredProtectedToken = await HasStoredProtectedTokenAsync(ct);
            if (hasStoredProtectedToken)
            {
                return new ApiCheckRunResult(
                    ApiHealthStatus.Down,
                    (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                    "Stored token is unreadable",
                    null,
                    null,
                    "SecretUnavailable",
                    "A Zoho refresh token exists in storage but could not be read on this host.");
            }

            return new ApiCheckRunResult(
                ApiHealthStatus.Unknown,
                (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                "Not connected",
                null,
                null,
                null,
                null);
        }

        using var response = await zohoCrmClient.PingAsync(ct);
        return new ApiCheckRunResult(
            ApiHealthStatus.Up,
            (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
            "Reachable",
            null,
            200,
            null,
            null);
    }

    private async Task<bool> HasStoredProtectedTokenAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var len = await conn.ExecuteScalarAsync<int?>(new CommandDefinition(
            """
SELECT TOP 1 LEN(ISNULL(ProtectedRefreshToken, N''))
FROM dbo.ZohoOAuthToken
WHERE TokenKey = N'zoho-crm';
""",
            cancellationToken: ct));
        return len.HasValue && len.Value > 0;
    }
}
