using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record EmailCodeCreateRequest(
    EmailCodePurpose Purpose,
    string Email,
    string EmailNormalized,
    byte[] CodeHash,
    byte[] Salt,
    DateTime ExpiresAtUtc,
    string? RequestedFromIp,
    string? RequestedUserAgent);

public interface IEmailCodeRepository
{
    Task<int> CreateAsync(EmailCodeCreateRequest request, CancellationToken ct);
    Task<EmailCodeRecord?> GetByIdAsync(int emailCodeId, CancellationToken ct);
    Task IncrementFailedAttemptsAsync(int emailCodeId, CancellationToken ct);
    Task<bool> TryMarkUsedAsync(int emailCodeId, CancellationToken ct);
    Task<DateTime?> GetLatestCreatedAtUtcAsync(string emailNormalized, CancellationToken ct);
    Task<int> CountCreatedInLastHourForEmailAsync(string emailNormalized, DateTime sinceUtc, CancellationToken ct);
    Task<int> CountCreatedInLastHourForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct);
}

public sealed class EmailCodeRepository(ISqlConnectionFactory connectionFactory) : IEmailCodeRepository
{
    public async Task<int> CreateAsync(EmailCodeCreateRequest request, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.EmailCodes(
  Purpose,
  Email,
  EmailNormalized,
  CodeHash,
  Salt,
  ExpiresAtUtc,
  RequestedFromIp,
  RequestedUserAgent)
OUTPUT INSERTED.EmailCodeId
VALUES(
  @Purpose,
  @Email,
  @EmailNormalized,
  @CodeHash,
  @Salt,
  @ExpiresAtUtc,
  @RequestedFromIp,
  @RequestedUserAgent);",
            new
            {
                Purpose = (byte)request.Purpose,
                request.Email,
                request.EmailNormalized,
                request.CodeHash,
                request.Salt,
                request.ExpiresAtUtc,
                RequestedFromIp = Truncate(request.RequestedFromIp, 45),
                RequestedUserAgent = Truncate(request.RequestedUserAgent, 256)
            },
            cancellationToken: ct));
    }

    public async Task<EmailCodeRecord?> GetByIdAsync(int emailCodeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<EmailCodeRecord>(new CommandDefinition(@"
SELECT TOP 1
  EmailCodeId,
  CAST(Purpose AS tinyint) AS Purpose,
  Email,
  EmailNormalized,
  CodeHash,
  Salt,
  ExpiresAtUtc,
  CreatedAtUtc,
  FailedAttempts,
  IsUsed
FROM dbo.EmailCodes
WHERE EmailCodeId = @EmailCodeId;",
            new { EmailCodeId = emailCodeId },
            cancellationToken: ct));
    }

    public async Task IncrementFailedAttemptsAsync(int emailCodeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailCodes
SET FailedAttempts = FailedAttempts + 1
WHERE EmailCodeId = @EmailCodeId;",
            new { EmailCodeId = emailCodeId },
            cancellationToken: ct));
    }

    public async Task<bool> TryMarkUsedAsync(int emailCodeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailCodes
SET IsUsed = 1
WHERE EmailCodeId = @EmailCodeId
  AND IsUsed = 0;",
            new { EmailCodeId = emailCodeId },
            cancellationToken: ct));
        return updated > 0;
    }

    public async Task<DateTime?> GetLatestCreatedAtUtcAsync(string emailNormalized, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<DateTime?>(new CommandDefinition(@"
SELECT MAX(CreatedAtUtc)
FROM dbo.EmailCodes
WHERE EmailNormalized = @EmailNormalized;",
            new { EmailNormalized = emailNormalized },
            cancellationToken: ct));
    }

    public async Task<int> CountCreatedInLastHourForEmailAsync(string emailNormalized, DateTime sinceUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.EmailCodes
WHERE EmailNormalized = @EmailNormalized
  AND CreatedAtUtc >= @SinceUtc;",
            new { EmailNormalized = emailNormalized, SinceUtc = sinceUtc },
            cancellationToken: ct));
    }

    public async Task<int> CountCreatedInLastHourForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.EmailCodes
WHERE RequestedFromIp = @RequestedFromIp
  AND CreatedAtUtc >= @SinceUtc;",
            new { RequestedFromIp = requestedFromIp, SinceUtc = sinceUtc },
            cancellationToken: ct));
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
