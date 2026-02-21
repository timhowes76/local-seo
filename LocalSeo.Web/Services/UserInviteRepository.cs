using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record UserInviteCreateRequest(
    int UserId,
    string EmailNormalized,
    byte[] TokenHash,
    DateTime ExpiresAtUtc,
    int? CreatedByUserId,
    DateTime CreatedAtUtc);

public sealed record InviteOtpCreateRequest(
    long UserInviteId,
    byte[] CodeHash,
    DateTime ExpiresAtUtc,
    DateTime SentAtUtc,
    string? RequestedFromIp);

public interface IUserInviteRepository
{
    Task<int> CreatePendingUserAsync(string firstName, string lastName, string emailAddress, string emailAddressNormalized, DateTime nowUtc, CancellationToken ct);
    Task<long> CreateInviteAsync(UserInviteCreateRequest request, CancellationToken ct);
    Task<int> RevokeActiveInvitesForUserAsync(int userId, DateTime nowUtc, CancellationToken ct);
    Task<UserInviteRecord?> GetInviteByTokenHashAsync(byte[] tokenHash, CancellationToken ct);
    Task<UserInviteRecord?> GetLatestInviteByUserIdAsync(int userId, CancellationToken ct);
    Task MarkInviteExpiredAsync(long userInviteId, DateTime nowUtc, CancellationToken ct);
    Task MarkInviteAttemptFailureAsync(long userInviteId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct);
    Task MarkInviteOtpVerifiedAsync(long userInviteId, DateTime nowUtc, CancellationToken ct);
    Task<DateTime?> GetLatestOtpSentAtUtcAsync(long userInviteId, CancellationToken ct);
    Task<int> CountOtpSentSinceAsync(long userInviteId, DateTime sinceUtc, CancellationToken ct);
    Task<int> CountOtpSentSinceForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct);
    Task<long> CreateInviteOtpAsync(InviteOtpCreateRequest request, CancellationToken ct);
    Task<InviteOtpRecord?> GetLatestInviteOtpAsync(long userInviteId, DateTime nowUtc, CancellationToken ct);
    Task MarkInviteOtpAttemptFailureAsync(long inviteOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct);
    Task<bool> MarkInviteOtpUsedAsync(long inviteOtpId, DateTime nowUtc, CancellationToken ct);
    Task<bool> CompleteInviteAsync(long userInviteId, int userId, byte[] passwordHash, byte passwordHashVersion, bool useGravatar, DateTime nowUtc, CancellationToken ct);
}

public sealed class UserInviteRepository(ISqlConnectionFactory connectionFactory) : IUserInviteRepository
{
    public async Task<int> CreatePendingUserAsync(string firstName, string lastName, string emailAddress, string emailAddressNormalized, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.[User](
  FirstName,
  LastName,
  EmailAddress,
  EmailAddressNormalized,
  PasswordHash,
  Salt,
  PasswordHashVersion,
  IsActive,
  IsAdmin,
  InviteStatus,
  DateCreatedAtUtc,
  DatePasswordLastSetUtc,
  LastLoginAtUtc,
  FailedPasswordAttempts,
  LockedoutUntilUtc)
OUTPUT INSERTED.Id
VALUES(
  @FirstName,
  @LastName,
  @EmailAddress,
  @EmailAddressNormalized,
  NULL,
  NULL,
  1,
  0,
  0,
  @InviteStatus,
  @NowUtc,
  NULL,
  NULL,
  0,
  NULL);",
            new
            {
                FirstName = Truncate(firstName, 100),
                LastName = Truncate(lastName, 100),
                EmailAddress = Truncate(emailAddress, 320),
                EmailAddressNormalized = Truncate(emailAddressNormalized, 320),
                InviteStatus = (byte)UserLifecycleStatus.Pending,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task<long> CreateInviteAsync(UserInviteCreateRequest request, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.UserInvite(
  UserId,
  EmailNormalized,
  TokenHash,
  ExpiresAtUtc,
  UsedAtUtc,
  CreatedAtUtc,
  CreatedByUserId,
  Status,
  AttemptCount)
OUTPUT INSERTED.UserInviteId
VALUES(
  @UserId,
  @EmailNormalized,
  @TokenHash,
  @ExpiresAtUtc,
  NULL,
  @CreatedAtUtc,
  @CreatedByUserId,
  @Status,
  0);",
            new
            {
                request.UserId,
                EmailNormalized = Truncate(request.EmailNormalized, 320),
                request.TokenHash,
                request.ExpiresAtUtc,
                request.CreatedAtUtc,
                request.CreatedByUserId,
                Status = (byte)UserInviteStatus.Active
            },
            cancellationToken: ct));
    }

    public async Task<int> RevokeActiveInvitesForUserAsync(int userId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET
  Status = @RevokedStatus,
  ResentAtUtc = @NowUtc
WHERE UserId = @UserId
  AND Status = @ActiveStatus
  AND UsedAtUtc IS NULL;",
            new
            {
                UserId = userId,
                NowUtc = nowUtc,
                ActiveStatus = (byte)UserInviteStatus.Active,
                RevokedStatus = (byte)UserInviteStatus.Revoked
            },
            cancellationToken: ct));
    }

    public async Task<UserInviteRecord?> GetInviteByTokenHashAsync(byte[] tokenHash, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<UserInviteRecord>(new CommandDefinition(@"
SELECT TOP 1
  ui.UserInviteId,
  ui.UserId,
  u.FirstName,
  u.LastName,
  u.EmailAddress,
  ui.EmailNormalized,
  ui.TokenHash,
  ui.ExpiresAtUtc,
  ui.UsedAtUtc,
  ui.CreatedAtUtc,
  ui.CreatedByUserId,
  ui.ResentAtUtc,
  CAST(ui.Status AS tinyint) AS Status,
  ui.AttemptCount,
  ui.LastAttemptAtUtc,
  ui.LockedUntilUtc,
  ui.OtpVerifiedAtUtc,
  ui.LastOtpSentAtUtc
FROM dbo.UserInvite ui
JOIN dbo.[User] u ON u.Id = ui.UserId
WHERE ui.TokenHash = @TokenHash;",
            new { TokenHash = tokenHash },
            cancellationToken: ct));
    }

    public async Task<UserInviteRecord?> GetLatestInviteByUserIdAsync(int userId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<UserInviteRecord>(new CommandDefinition(@"
SELECT TOP 1
  ui.UserInviteId,
  ui.UserId,
  u.FirstName,
  u.LastName,
  u.EmailAddress,
  ui.EmailNormalized,
  ui.TokenHash,
  ui.ExpiresAtUtc,
  ui.UsedAtUtc,
  ui.CreatedAtUtc,
  ui.CreatedByUserId,
  ui.ResentAtUtc,
  CAST(ui.Status AS tinyint) AS Status,
  ui.AttemptCount,
  ui.LastAttemptAtUtc,
  ui.LockedUntilUtc,
  ui.OtpVerifiedAtUtc,
  ui.LastOtpSentAtUtc
FROM dbo.UserInvite ui
JOIN dbo.[User] u ON u.Id = ui.UserId
WHERE ui.UserId = @UserId
ORDER BY ui.CreatedAtUtc DESC, ui.UserInviteId DESC;",
            new { UserId = userId },
            cancellationToken: ct));
    }

    public async Task MarkInviteExpiredAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET Status = @ExpiredStatus,
    LastAttemptAtUtc = @NowUtc
WHERE UserInviteId = @UserInviteId
  AND Status = @ActiveStatus;",
            new
            {
                UserInviteId = userInviteId,
                NowUtc = nowUtc,
                ActiveStatus = (byte)UserInviteStatus.Active,
                ExpiredStatus = (byte)UserInviteStatus.Expired
            },
            cancellationToken: ct));
    }

    public async Task MarkInviteAttemptFailureAsync(long userInviteId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET
  AttemptCount = AttemptCount + 1,
  LastAttemptAtUtc = @NowUtc,
  LockedUntilUtc = CASE
      WHEN AttemptCount + 1 >= @MaxAttempts THEN DATEADD(minute, @LockMinutes, @NowUtc)
      ELSE LockedUntilUtc
  END
WHERE UserInviteId = @UserInviteId;",
            new
            {
                UserInviteId = userInviteId,
                NowUtc = nowUtc,
                MaxAttempts = Math.Max(1, maxAttempts),
                LockMinutes = Math.Max(1, lockMinutes)
            },
            cancellationToken: ct));
    }

    public async Task MarkInviteOtpVerifiedAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET OtpVerifiedAtUtc = COALESCE(OtpVerifiedAtUtc, @NowUtc)
WHERE UserInviteId = @UserInviteId;",
            new { UserInviteId = userInviteId, NowUtc = nowUtc },
            cancellationToken: ct));
    }

    public async Task<DateTime?> GetLatestOtpSentAtUtcAsync(long userInviteId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<DateTime?>(new CommandDefinition(@"
SELECT MAX(SentAtUtc)
FROM dbo.InviteOtp
WHERE UserInviteId = @UserInviteId;",
            new { UserInviteId = userInviteId },
            cancellationToken: ct));
    }

    public async Task<int> CountOtpSentSinceAsync(long userInviteId, DateTime sinceUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.InviteOtp
WHERE UserInviteId = @UserInviteId
  AND SentAtUtc >= @SinceUtc;",
            new { UserInviteId = userInviteId, SinceUtc = sinceUtc },
            cancellationToken: ct));
    }

    public async Task<int> CountOtpSentSinceForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct)
    {
        var normalizedIp = Truncate(requestedFromIp, 45);
        if (string.IsNullOrWhiteSpace(normalizedIp))
            return 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.InviteOtp
WHERE RequestedFromIp = @RequestedFromIp
  AND SentAtUtc >= @SinceUtc;",
            new { RequestedFromIp = normalizedIp, SinceUtc = sinceUtc },
            cancellationToken: ct));
    }

    public async Task<long> CreateInviteOtpAsync(InviteOtpCreateRequest request, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var inviteOtpId = await conn.QuerySingleAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.InviteOtp(
  UserInviteId,
  CodeHash,
  ExpiresAtUtc,
  SentAtUtc,
  AttemptCount,
  LockedUntilUtc,
  UsedAtUtc,
  RequestedFromIp)
OUTPUT INSERTED.InviteOtpId
VALUES(
  @UserInviteId,
  @CodeHash,
  @ExpiresAtUtc,
  @SentAtUtc,
  0,
  NULL,
  NULL,
  @RequestedFromIp);",
            new
            {
                request.UserInviteId,
                request.CodeHash,
                request.ExpiresAtUtc,
                request.SentAtUtc,
                RequestedFromIp = Truncate(request.RequestedFromIp, 45)
            },
            cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET LastOtpSentAtUtc = @SentAtUtc
WHERE UserInviteId = @UserInviteId;",
            new { request.UserInviteId, request.SentAtUtc },
            cancellationToken: ct));

        return inviteOtpId;
    }

    public async Task<InviteOtpRecord?> GetLatestInviteOtpAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<InviteOtpRecord>(new CommandDefinition(@"
SELECT TOP 1
  InviteOtpId,
  UserInviteId,
  CodeHash,
  ExpiresAtUtc,
  SentAtUtc,
  AttemptCount,
  LockedUntilUtc,
  UsedAtUtc
FROM dbo.InviteOtp
WHERE UserInviteId = @UserInviteId
  AND ExpiresAtUtc >= @NowUtc
  AND UsedAtUtc IS NULL
ORDER BY SentAtUtc DESC, InviteOtpId DESC;",
            new { UserInviteId = userInviteId, NowUtc = nowUtc },
            cancellationToken: ct));
    }

    public async Task MarkInviteOtpAttemptFailureAsync(long inviteOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.InviteOtp
SET
  AttemptCount = AttemptCount + 1,
  LockedUntilUtc = CASE
      WHEN AttemptCount + 1 >= @MaxAttempts THEN DATEADD(minute, @LockMinutes, @NowUtc)
      ELSE LockedUntilUtc
  END
WHERE InviteOtpId = @InviteOtpId;",
            new
            {
                InviteOtpId = inviteOtpId,
                MaxAttempts = Math.Max(1, maxAttempts),
                LockMinutes = Math.Max(1, lockMinutes),
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task<bool> MarkInviteOtpUsedAsync(long inviteOtpId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.InviteOtp
SET UsedAtUtc = @NowUtc
WHERE InviteOtpId = @InviteOtpId
  AND UsedAtUtc IS NULL;",
            new { InviteOtpId = inviteOtpId, NowUtc = nowUtc },
            cancellationToken: ct));
        return updated > 0;
    }

    public async Task<bool> CompleteInviteAsync(long userInviteId, int userId, byte[] passwordHash, byte passwordHashVersion, bool useGravatar, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var inviteUpdated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET
  Status = @UsedStatus,
  UsedAtUtc = @NowUtc
WHERE UserInviteId = @UserInviteId
  AND UserId = @UserId
  AND Status = @ActiveStatus
  AND UsedAtUtc IS NULL
  AND ExpiresAtUtc >= @NowUtc
  AND OtpVerifiedAtUtc IS NOT NULL;",
            new
            {
                UserInviteId = userInviteId,
                UserId = userId,
                NowUtc = nowUtc,
                ActiveStatus = (byte)UserInviteStatus.Active,
                UsedStatus = (byte)UserInviteStatus.Used
            },
            tx,
            cancellationToken: ct));

        if (inviteUpdated != 1)
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        var userUpdated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET
  PasswordHash = @PasswordHash,
  Salt = NULL,
  PasswordHashVersion = @PasswordHashVersion,
  DatePasswordLastSetUtc = @NowUtc,
  FailedPasswordAttempts = 0,
  LockedoutUntilUtc = NULL,
  UseGravatar = @UseGravatar,
  IsActive = 1,
  InviteStatus = @InviteStatusActive
WHERE Id = @UserId;",
            new
            {
                UserId = userId,
                PasswordHash = passwordHash,
                PasswordHashVersion = passwordHashVersion,
                UseGravatar = useGravatar,
                NowUtc = nowUtc,
                InviteStatusActive = (byte)UserLifecycleStatus.Active
            },
            tx,
            cancellationToken: ct));

        if (userUpdated != 1)
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.UserInvite
SET Status = @RevokedStatus
WHERE UserId = @UserId
  AND UserInviteId <> @UserInviteId
  AND Status = @ActiveStatus
  AND UsedAtUtc IS NULL;",
            new
            {
                UserId = userId,
                UserInviteId = userInviteId,
                ActiveStatus = (byte)UserInviteStatus.Active,
                RevokedStatus = (byte)UserInviteStatus.Revoked
            },
            tx,
            cancellationToken: ct));

        await tx.CommitAsync(ct);
        return true;
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
