using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IUserRepository
{
    Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct);
    Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct);
    Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, CancellationToken ct);
    Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct);
    Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct);
    Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct);
    Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct);
}

public sealed class UserRepository(ISqlConnectionFactory connectionFactory) : IUserRepository
{
    public async Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<UserRecord>(new CommandDefinition(@"
SELECT TOP 1
  Id,
  FirstName,
  LastName,
  EmailAddress,
  EmailAddressNormalized,
  PasswordHash,
  PasswordHashVersion,
  IsActive,
  IsAdmin,
  DateCreatedAtUtc,
  DatePasswordLastSetUtc,
  LastLoginAtUtc,
  FailedPasswordAttempts,
  LockedoutUntilUtc
FROM dbo.[User]
WHERE EmailAddressNormalized = @EmailAddressNormalized;",
            new { EmailAddressNormalized = emailNormalized },
            cancellationToken: ct));
    }

    public async Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<UserRecord>(new CommandDefinition(@"
SELECT TOP 1
  Id,
  FirstName,
  LastName,
  EmailAddress,
  EmailAddressNormalized,
  PasswordHash,
  PasswordHashVersion,
  IsActive,
  IsAdmin,
  DateCreatedAtUtc,
  DatePasswordLastSetUtc,
  LastLoginAtUtc,
  FailedPasswordAttempts,
  LockedoutUntilUtc
FROM dbo.[User]
WHERE Id = @Id;",
            new { Id = id },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, CancellationToken ct)
    {
        var where = filter switch
        {
            UserStatusFilter.Inactive => "WHERE IsActive = 0",
            UserStatusFilter.All => string.Empty,
            _ => "WHERE IsActive = 1"
        };

        var sql = $@"
SELECT
  Id,
  CONCAT(FirstName, ' ', LastName) AS Name,
  EmailAddress,
  DateCreatedAtUtc,
  IsActive
FROM dbo.[User]
{where}
ORDER BY DateCreatedAtUtc DESC, Id DESC;";

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AdminUserListRow>(new CommandDefinition(sql, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET
  FailedPasswordAttempts = FailedPasswordAttempts + 1,
  LockedoutUntilUtc = CASE
      WHEN FailedPasswordAttempts + 1 >= @LockoutThreshold THEN DATEADD(minute, @LockoutMinutes, @NowUtc)
      ELSE LockedoutUntilUtc
  END
WHERE Id = @Id;",
            new
            {
                Id = userId,
                LockoutThreshold = lockoutThreshold,
                LockoutMinutes = lockoutMinutes,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET FailedPasswordAttempts = 0,
    LockedoutUntilUtc = NULL
WHERE Id = @Id;",
            new { Id = userId },
            cancellationToken: ct));
    }

    public async Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET LastLoginAtUtc = @NowUtc
WHERE Id = @Id;",
            new { Id = userId, NowUtc = nowUtc },
            cancellationToken: ct));
    }

    public async Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET PasswordHash = @PasswordHash,
    Salt = NULL,
    PasswordHashVersion = @PasswordHashVersion,
    DatePasswordLastSetUtc = @NowUtc,
    FailedPasswordAttempts = 0,
    LockedoutUntilUtc = NULL
WHERE Id = @Id;",
            new
            {
                Id = userId,
                PasswordHash = passwordHash,
                PasswordHashVersion = passwordHashVersion,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }
}
