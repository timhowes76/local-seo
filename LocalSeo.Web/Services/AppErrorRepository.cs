using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IAppErrorRepository
{
    Task InsertAsync(AppErrorRow row, CancellationToken ct);
    Task<PagedResult<AppErrorRow>> GetPagedAsync(int page, int pageSize, CancellationToken ct);
    Task<AppErrorRow?> GetByIdAsync(long appErrorId, CancellationToken ct);
    Task DeleteAllAsync(CancellationToken ct);
}

public sealed class AppErrorRepository(ISqlConnectionFactory connectionFactory) : IAppErrorRepository
{
    public async Task InsertAsync(AppErrorRow row, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(row);

        var createdUtc = row.CreatedUtc == default ? DateTime.UtcNow : row.CreatedUtc;
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.AppError(
  CreatedUtc,
  UserId,
  IpAddress,
  TraceId,
  HttpMethod,
  StatusCode,
  FullUrl,
  Referrer,
  UserAgentRaw,
  BrowserName,
  BrowserVersion,
  OsName,
  OsVersion,
  DeviceType,
  ExceptionType,
  ExceptionMessage,
  ExceptionDetail)
VALUES(
  @CreatedUtc,
  @UserId,
  @IpAddress,
  @TraceId,
  @HttpMethod,
  @StatusCode,
  @FullUrl,
  @Referrer,
  @UserAgentRaw,
  @BrowserName,
  @BrowserVersion,
  @OsName,
  @OsVersion,
  @DeviceType,
  @ExceptionType,
  @ExceptionMessage,
  @ExceptionDetail);",
            new
            {
                CreatedUtc = createdUtc,
                UserId = NormalizeUserId(row.UserId),
                IpAddress = Truncate(row.IpAddress, 64),
                TraceId = Truncate(row.TraceId, 64),
                HttpMethod = Truncate(row.HttpMethod, 16),
                StatusCode = row.StatusCode <= 0 ? 500 : row.StatusCode,
                FullUrl = Truncate(row.FullUrl, 2048),
                Referrer = Truncate(row.Referrer, 2048),
                UserAgentRaw = Truncate(row.UserAgentRaw, 512),
                BrowserName = Truncate(row.BrowserName, 64),
                BrowserVersion = Truncate(row.BrowserVersion, 32),
                OsName = Truncate(row.OsName, 64),
                OsVersion = Truncate(row.OsVersion, 32),
                DeviceType = Truncate(row.DeviceType, 32),
                ExceptionType = Truncate(row.ExceptionType, 256),
                row.ExceptionMessage,
                row.ExceptionDetail
            },
            cancellationToken: ct));
    }

    public async Task<PagedResult<AppErrorRow>> GetPagedAsync(int page, int pageSize, CancellationToken ct)
    {
        var normalizedPageSize = NormalizePageSize(pageSize);
        var normalizedPage = Math.Max(1, page);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.AppError;",
            cancellationToken: ct));

        var totalPages = totalCount <= 0
            ? 1
            : Math.Max(1, (int)Math.Ceiling(totalCount / (double)normalizedPageSize));
        if (normalizedPage > totalPages)
            normalizedPage = totalPages;

        var offset = (normalizedPage - 1) * normalizedPageSize;
        var rows = (await conn.QueryAsync<AppErrorRow>(new CommandDefinition(@"
SELECT
  AppErrorId,
  CreatedUtc,
  UserId,
  u.FirstName AS UserFirstName,
  u.LastName AS UserLastName,
  IpAddress,
  TraceId,
  HttpMethod,
  StatusCode,
  FullUrl,
  Referrer,
  UserAgentRaw,
  BrowserName,
  BrowserVersion,
  OsName,
  OsVersion,
  DeviceType,
  ExceptionType,
  ExceptionMessage,
  ExceptionDetail
FROM dbo.AppError ae
LEFT JOIN dbo.[User] u ON u.Id = ae.UserId
ORDER BY ae.CreatedUtc DESC, ae.AppErrorId DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;",
            new
            {
                Offset = offset,
                PageSize = normalizedPageSize
            },
            cancellationToken: ct))).ToList();

        return new PagedResult<AppErrorRow>(rows, totalCount);
    }

    public async Task<AppErrorRow?> GetByIdAsync(long appErrorId, CancellationToken ct)
    {
        if (appErrorId <= 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QueryFirstOrDefaultAsync<AppErrorRow>(new CommandDefinition(@"
SELECT
  AppErrorId,
  CreatedUtc,
  UserId,
  u.FirstName AS UserFirstName,
  u.LastName AS UserLastName,
  IpAddress,
  TraceId,
  HttpMethod,
  StatusCode,
  FullUrl,
  Referrer,
  UserAgentRaw,
  BrowserName,
  BrowserVersion,
  OsName,
  OsVersion,
  DeviceType,
  ExceptionType,
  ExceptionMessage,
  ExceptionDetail
FROM dbo.AppError ae
LEFT JOIN dbo.[User] u ON u.Id = ae.UserId
WHERE ae.AppErrorId = @AppErrorId;",
            new { AppErrorId = appErrorId },
            cancellationToken: ct));
    }

    public async Task DeleteAllAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.AppError;",
            cancellationToken: ct));
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
            _ => 50
        };
    }

    private static int? NormalizeUserId(int? userId)
    {
        if (!userId.HasValue || userId.Value <= 0)
            return null;
        return userId.Value;
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
