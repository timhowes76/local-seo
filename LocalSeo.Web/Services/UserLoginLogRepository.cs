using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IUserLoginLogRepository
{
    Task InsertAsync(UserLoginAttempt attempt, CancellationToken ct);
    Task<PagedResult<UserLoginLogRow>> SearchAsync(LoginLogQuery query, CancellationToken ct);
}

public sealed class UserLoginLogRepository(ISqlConnectionFactory connectionFactory) : IUserLoginLogRepository
{
    public async Task InsertAsync(UserLoginAttempt attempt, CancellationToken ct)
    {
        var attemptedAtUtc = attempt.AttemptedAtUtc == default ? DateTime.UtcNow : attempt.AttemptedAtUtc;
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.UserLogins(
  AttemptedAtUtc,
  EmailEntered,
  EmailNormalized,
  UserId,
  IpAddress,
  Succeeded,
  FailureReason,
  AuthStage,
  UserAgent,
  CorrelationId)
VALUES(
  @AttemptedAtUtc,
  @EmailEntered,
  @EmailNormalized,
  @UserId,
  @IpAddress,
  @Succeeded,
  @FailureReason,
  @AuthStage,
  @UserAgent,
  @CorrelationId);",
            new
            {
                AttemptedAtUtc = attemptedAtUtc,
                EmailEntered = Truncate(attempt.EmailEntered, 320),
                EmailNormalized = Truncate(attempt.EmailNormalized, 320),
                attempt.UserId,
                IpAddress = NormalizeIpAddress(attempt.IpAddress),
                attempt.Succeeded,
                FailureReason = Truncate(attempt.FailureReason, 50),
                AuthStage = Truncate(attempt.AuthStage, 20),
                UserAgent = Truncate(attempt.UserAgent, 512),
                CorrelationId = Truncate(attempt.CorrelationId, 64)
            },
            cancellationToken: ct));
    }

    public async Task<PagedResult<UserLoginLogRow>> SearchAsync(LoginLogQuery query, CancellationToken ct)
    {
        var pageSize = NormalizePageSize(query.PageSize);
        var pageNumber = Math.Max(1, query.PageNumber);
        var normalizedSearch = NormalizeSearchText(query.SearchText);
        var searchPattern = string.IsNullOrWhiteSpace(normalizedSearch)
            ? null
            : $"%{EscapeLike(normalizedSearch)}%";
        var prioritizeIp = !string.IsNullOrWhiteSpace(normalizedSearch)
            && (normalizedSearch.Contains('.', StringComparison.Ordinal) || normalizedSearch.Contains(':', StringComparison.Ordinal));

        var whereParts = new List<string>();
        if (query.SucceededFilter == LoginLogSucceededFilter.Succeeded)
            whereParts.Add("ul.Succeeded = 1");
        else if (query.SucceededFilter == LoginLogSucceededFilter.Failed)
            whereParts.Add("ul.Succeeded = 0");

        if (!string.IsNullOrWhiteSpace(searchPattern))
        {
            whereParts.Add(@"(
  ul.EmailEntered LIKE @SearchPattern ESCAPE '\'
  OR ul.EmailNormalized LIKE @SearchPattern ESCAPE '\'
  OR ul.IpAddress LIKE @SearchPattern ESCAPE '\'
)");
        }

        var whereSql = whereParts.Count == 0
            ? string.Empty
            : $"WHERE {string.Join(" AND ", whereParts)}";

        var orderSql = prioritizeIp
            ? @"ORDER BY
  CASE WHEN ul.IpAddress LIKE @SearchPattern ESCAPE '\' THEN 0 ELSE 1 END,
  ul.AttemptedAtUtc DESC,
  ul.UserLoginId DESC"
            : @"ORDER BY ul.AttemptedAtUtc DESC, ul.UserLoginId DESC";

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition($@"
SELECT COUNT(1)
FROM dbo.UserLogins ul
{whereSql};",
            new { SearchPattern = searchPattern },
            cancellationToken: ct));

        var totalPages = totalCount <= 0
            ? 1
            : Math.Max(1, (int)Math.Ceiling(totalCount / (double)pageSize));
        if (pageNumber > totalPages)
            pageNumber = totalPages;

        var offset = (pageNumber - 1) * pageSize;
        var rows = (await conn.QueryAsync<UserLoginLogRow>(new CommandDefinition($@"
SELECT
  ul.UserLoginId,
  ul.AttemptedAtUtc,
  ul.EmailEntered,
  ul.EmailNormalized,
  ul.UserId,
  ul.IpAddress,
  ul.Succeeded,
  ul.FailureReason,
  ul.AuthStage,
  ul.UserAgent,
  ul.CorrelationId
FROM dbo.UserLogins ul
{whereSql}
{orderSql}
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;",
            new
            {
                SearchPattern = searchPattern,
                Offset = offset,
                PageSize = pageSize
            },
            cancellationToken: ct))).ToList();

        return new PagedResult<UserLoginLogRow>(rows, totalCount);
    }

    private static int NormalizePageSize(int value)
    {
        return value switch
        {
            25 => 25,
            50 => 50,
            100 => 100,
            500 => 500,
            1000 => 1000,
            _ => 100
        };
    }

    private static string NormalizeIpAddress(string? ipAddress)
    {
        var trimmed = (ipAddress ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return "unknown";

        return trimmed.Length <= 45 ? trimmed : trimmed[..45];
    }

    private static string NormalizeSearchText(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        return trimmed.Length <= 200 ? trimmed : trimmed[..200];
    }

    private static string EscapeLike(string input)
    {
        return input
            .Replace(@"\", @"\\", StringComparison.Ordinal)
            .Replace("%", @"\%", StringComparison.Ordinal)
            .Replace("_", @"\_", StringComparison.Ordinal)
            .Replace("[", @"\[", StringComparison.Ordinal);
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
