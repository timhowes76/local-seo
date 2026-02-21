using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record UserOtpCreateRequest(
    int UserId,
    string Purpose,
    byte[] CodeHash,
    DateTime ExpiresAtUtc,
    DateTime SentAtUtc,
    string CorrelationId,
    string? RequestedFromIp);

public interface IUserOtpRepository
{
    Task<long> CreateOtpAsync(UserOtpCreateRequest request, CancellationToken ct);
    Task<DateTime?> GetLatestSentAtUtcAsync(int userId, string purpose, CancellationToken ct);
    Task<int> CountSentSinceAsync(int userId, string purpose, DateTime sinceUtc, CancellationToken ct);
    Task<int> CountSentSinceForIpAsync(string requestedFromIp, string purpose, DateTime sinceUtc, CancellationToken ct);
    Task<UserOtpRecord?> GetLatestByCorrelationAsync(int userId, string purpose, string correlationId, CancellationToken ct);
    Task<int> RevokeActiveByCorrelationAsync(int userId, string purpose, string correlationId, DateTime nowUtc, CancellationToken ct);
    Task<int> RevokeActiveByUserPurposeAsync(int userId, string purpose, DateTime nowUtc, CancellationToken ct);
    Task MarkAttemptFailureAsync(long userOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct);
    Task<bool> MarkUsedAsync(long userOtpId, DateTime nowUtc, CancellationToken ct);
}

public sealed class UserOtpRepository(ISqlConnectionFactory connectionFactory) : IUserOtpRepository
{
    public async Task<long> CreateOtpAsync(UserOtpCreateRequest request, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.UserOtp(
  UserId,
  Purpose,
  CodeHash,
  ExpiresAtUtc,
  UsedAtUtc,
  SentAtUtc,
  AttemptCount,
  LockedUntilUtc,
  CorrelationId,
  RequestedFromIp)
OUTPUT INSERTED.UserOtpId
VALUES(
  @UserId,
  @Purpose,
  @CodeHash,
  @ExpiresAtUtc,
  NULL,
  @SentAtUtc,
  0,
  NULL,
  @CorrelationId,
  @RequestedFromIp);",
            new
            {
                request.UserId,
                Purpose = Truncate(request.Purpose, 30),
                request.CodeHash,
                request.ExpiresAtUtc,
                request.SentAtUtc,
                CorrelationId = Truncate(request.CorrelationId, 64),
                RequestedFromIp = Truncate(request.RequestedFromIp, 45)
            },
            cancellationToken: ct));
    }

    public async Task<DateTime?> GetLatestSentAtUtcAsync(int userId, string purpose, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<DateTime?>(new CommandDefinition(@"
SELECT MAX(SentAtUtc)
FROM dbo.UserOtp
WHERE UserId = @UserId
  AND Purpose = @Purpose;",
            new { UserId = userId, Purpose = Truncate(purpose, 30) },
            cancellationToken: ct));
    }

    public async Task<int> CountSentSinceAsync(int userId, string purpose, DateTime sinceUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.UserOtp
WHERE UserId = @UserId
  AND Purpose = @Purpose
  AND SentAtUtc >= @SinceUtc;",
            new
            {
                UserId = userId,
                Purpose = Truncate(purpose, 30),
                SinceUtc = sinceUtc
            },
            cancellationToken: ct));
    }

    public async Task<int> CountSentSinceForIpAsync(string requestedFromIp, string purpose, DateTime sinceUtc, CancellationToken ct)
    {
        var normalizedIp = Truncate(requestedFromIp, 45);
        if (string.IsNullOrWhiteSpace(normalizedIp))
            return 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.UserOtp
WHERE RequestedFromIp = @RequestedFromIp
  AND Purpose = @Purpose
  AND SentAtUtc >= @SinceUtc;",
            new
            {
                RequestedFromIp = normalizedIp,
                Purpose = Truncate(purpose, 30),
                SinceUtc = sinceUtc
            },
            cancellationToken: ct));
    }

    public async Task<UserOtpRecord?> GetLatestByCorrelationAsync(int userId, string purpose, string correlationId, CancellationToken ct)
    {
        var normalizedCorrelation = Truncate(correlationId, 64);
        if (string.IsNullOrWhiteSpace(normalizedCorrelation))
            return null;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<UserOtpRecord>(new CommandDefinition(@"
SELECT TOP 1
  UserOtpId,
  UserId,
  Purpose,
  CodeHash,
  ExpiresAtUtc,
  UsedAtUtc,
  SentAtUtc,
  AttemptCount,
  LockedUntilUtc,
  CorrelationId,
  RequestedFromIp
FROM dbo.UserOtp
WHERE UserId = @UserId
  AND Purpose = @Purpose
  AND CorrelationId = @CorrelationId
ORDER BY SentAtUtc DESC, UserOtpId DESC;",
            new
            {
                UserId = userId,
                Purpose = Truncate(purpose, 30),
                CorrelationId = normalizedCorrelation
            },
            cancellationToken: ct));
    }

    public async Task<int> RevokeActiveByCorrelationAsync(int userId, string purpose, string correlationId, DateTime nowUtc, CancellationToken ct)
    {
        var normalizedCorrelation = Truncate(correlationId, 64);
        if (string.IsNullOrWhiteSpace(normalizedCorrelation))
            return 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserOtp
SET UsedAtUtc = COALESCE(UsedAtUtc, @NowUtc)
WHERE UserId = @UserId
  AND Purpose = @Purpose
  AND CorrelationId = @CorrelationId
  AND UsedAtUtc IS NULL;",
            new
            {
                UserId = userId,
                Purpose = Truncate(purpose, 30),
                CorrelationId = normalizedCorrelation,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task<int> RevokeActiveByUserPurposeAsync(int userId, string purpose, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserOtp
SET UsedAtUtc = COALESCE(UsedAtUtc, @NowUtc)
WHERE UserId = @UserId
  AND Purpose = @Purpose
  AND UsedAtUtc IS NULL;",
            new
            {
                UserId = userId,
                Purpose = Truncate(purpose, 30),
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task MarkAttemptFailureAsync(long userOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserOtp
SET
  AttemptCount = AttemptCount + 1,
  LockedUntilUtc = CASE
      WHEN AttemptCount + 1 >= @MaxAttempts THEN DATEADD(minute, @LockMinutes, @NowUtc)
      ELSE LockedUntilUtc
  END
WHERE UserOtpId = @UserOtpId
  AND UsedAtUtc IS NULL;",
            new
            {
                UserOtpId = userOtpId,
                MaxAttempts = Math.Max(1, maxAttempts),
                LockMinutes = Math.Max(1, lockMinutes),
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task<bool> MarkUsedAsync(long userOtpId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserOtp
SET UsedAtUtc = @NowUtc
WHERE UserOtpId = @UserOtpId
  AND UsedAtUtc IS NULL;",
            new { UserOtpId = userOtpId, NowUtc = nowUtc },
            cancellationToken: ct));
        return updated == 1;
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
