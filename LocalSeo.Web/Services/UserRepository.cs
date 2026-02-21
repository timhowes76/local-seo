using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IUserRepository
{
    Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct);
    Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct);
    Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, string? searchTerm, CancellationToken ct);
    Task<bool> UpdateProfileAsync(int userId, string firstName, string lastName, bool useGravatar, CancellationToken ct);
    Task<bool> UpdateUserAsync(int userId, string firstName, string lastName, string emailAddress, string emailAddressNormalized, bool isAdmin, UserLifecycleStatus inviteStatus, CancellationToken ct);
    Task<bool> DeleteUserAsync(int userId, CancellationToken ct);
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
  LockedoutUntilUtc,
  CAST(InviteStatus AS tinyint) AS InviteStatus,
  UseGravatar
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
  LockedoutUntilUtc,
  CAST(InviteStatus AS tinyint) AS InviteStatus,
  UseGravatar
FROM dbo.[User]
WHERE Id = @Id;",
            new { Id = id },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, string? searchTerm, CancellationToken ct)
    {
        var whereParts = new List<string>();
        switch (filter)
        {
            case UserStatusFilter.Pending:
                whereParts.Add("u.InviteStatus = @PendingStatus");
                break;
            case UserStatusFilter.Disabled:
                whereParts.Add("u.InviteStatus = @DisabledStatus");
                break;
            case UserStatusFilter.All:
                break;
            default:
                whereParts.Add("u.InviteStatus = @ActiveStatus");
                break;
        }

        var searchPattern = BuildLikePattern(searchTerm);
        if (!string.IsNullOrWhiteSpace(searchPattern))
        {
            whereParts.Add(@"(
  u.FirstName LIKE @SearchPattern ESCAPE '\'
  OR u.LastName LIKE @SearchPattern ESCAPE '\'
  OR CONCAT(u.FirstName, ' ', u.LastName) LIKE @SearchPattern ESCAPE '\'
  OR u.EmailAddress LIKE @SearchPattern ESCAPE '\'
)");
        }

        var where = whereParts.Count == 0
            ? string.Empty
            : $"WHERE {string.Join(" AND ", whereParts)}";

        var sql = $@"
SELECT
  u.Id,
  CONCAT(u.FirstName, ' ', u.LastName) AS Name,
  u.EmailAddress,
  u.DateCreatedAtUtc,
  u.IsActive,
  CAST(u.InviteStatus AS tinyint) AS InviteStatus,
  latestInvite.LastInviteCreatedAtUtc,
  COALESCE(latestInvite.HasActiveInvite, CAST(0 AS bit)) AS HasActiveInvite
FROM dbo.[User] u
OUTER APPLY (
  SELECT TOP 1
    ui.CreatedAtUtc AS LastInviteCreatedAtUtc,
    CASE
      WHEN ui.Status = @UserInviteActiveStatus
        AND ui.UsedAtUtc IS NULL
        AND ui.ExpiresAtUtc >= SYSUTCDATETIME()
      THEN CAST(1 AS bit)
      ELSE CAST(0 AS bit)
    END AS HasActiveInvite
  FROM dbo.UserInvite ui
  WHERE ui.UserId = u.Id
  ORDER BY ui.CreatedAtUtc DESC, ui.UserInviteId DESC
) latestInvite
{where}
ORDER BY u.DateCreatedAtUtc DESC, u.Id DESC;";

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AdminUserListRow>(new CommandDefinition(sql,
            new
            {
                ActiveStatus = (byte)UserLifecycleStatus.Active,
                PendingStatus = (byte)UserLifecycleStatus.Pending,
                DisabledStatus = (byte)UserLifecycleStatus.Disabled,
                UserInviteActiveStatus = (byte)UserInviteStatus.Active,
                SearchPattern = searchPattern
            },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<bool> DeleteUserAsync(int userId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var emailNormalized = await conn.QuerySingleOrDefaultAsync<string?>(new CommandDefinition(@"
SELECT EmailAddressNormalized
FROM dbo.[User]
WHERE Id = @Id;",
            new { Id = userId },
            tx,
            cancellationToken: ct));

        if (emailNormalized is null)
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.UserInvite
WHERE UserId = @UserId;",
            new { UserId = userId },
            tx,
            cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.EmailCodes
WHERE EmailNormalized = @EmailNormalized;",
            new { EmailNormalized = emailNormalized },
            tx,
            cancellationToken: ct));

        var deleted = await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.[User]
WHERE Id = @Id;",
            new { Id = userId },
            tx,
            cancellationToken: ct));

        if (deleted != 1)
        {
            await tx.RollbackAsync(ct);
            return false;
        }

        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<bool> UpdateProfileAsync(int userId, string firstName, string lastName, bool useGravatar, CancellationToken ct)
    {
        var normalizedFirstName = Truncate(firstName, 100);
        var normalizedLastName = Truncate(lastName, 100);
        if (string.IsNullOrWhiteSpace(normalizedFirstName) || string.IsNullOrWhiteSpace(normalizedLastName))
            return false;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET
  FirstName = @FirstName,
  LastName = @LastName,
  UseGravatar = @UseGravatar
WHERE Id = @Id;",
            new
            {
                Id = userId,
                FirstName = normalizedFirstName,
                LastName = normalizedLastName,
                UseGravatar = useGravatar
            },
            cancellationToken: ct));
        return updated == 1;
    }

    public async Task<bool> UpdateUserAsync(int userId, string firstName, string lastName, string emailAddress, string emailAddressNormalized, bool isAdmin, UserLifecycleStatus inviteStatus, CancellationToken ct)
    {
        var normalizedFirstName = Truncate(firstName, 100);
        var normalizedLastName = Truncate(lastName, 100);
        var normalizedEmail = Truncate(emailAddress, 320);
        var normalizedEmailLookup = Truncate(emailAddressNormalized, 320);
        if (string.IsNullOrWhiteSpace(normalizedFirstName)
            || string.IsNullOrWhiteSpace(normalizedLastName)
            || string.IsNullOrWhiteSpace(normalizedEmail)
            || string.IsNullOrWhiteSpace(normalizedEmailLookup))
            return false;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.[User]
SET
  FirstName = @FirstName,
  LastName = @LastName,
  EmailAddress = @EmailAddress,
  EmailAddressNormalized = @EmailAddressNormalized,
  IsAdmin = @IsAdmin,
  InviteStatus = @InviteStatus,
  IsActive = @IsActive
WHERE Id = @Id;",
            new
            {
                Id = userId,
                FirstName = normalizedFirstName,
                LastName = normalizedLastName,
                EmailAddress = normalizedEmail,
                EmailAddressNormalized = normalizedEmailLookup,
                IsAdmin = isAdmin,
                InviteStatus = (byte)inviteStatus,
                IsActive = inviteStatus == UserLifecycleStatus.Active
            },
            cancellationToken: ct));
        return updated == 1;
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

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? BuildLikePattern(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var trimmed = raw.Trim();
        if (trimmed.Length == 0)
            return null;

        if (trimmed.Length > 200)
            trimmed = trimmed[..200];

        var escaped = trimmed
            .Replace(@"\", @"\\", StringComparison.Ordinal)
            .Replace("%", @"\%", StringComparison.Ordinal)
            .Replace("_", @"\_", StringComparison.Ordinal)
            .Replace("[", @"\[", StringComparison.Ordinal);

        return $"%{escaped}%";
    }
}
