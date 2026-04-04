using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface ICloudflareWorkerService
{
    Task<bool> IsAvailableAsync(CancellationToken ct);
    Task<IReadOnlyList<CloudflareWorkerListRowModel>> GetListAsync(string? search, CancellationToken ct);
    Task<CloudflareWorkerEditModel?> GetEditModelAsync(int cloudflareWorkerId, CancellationToken ct);
    Task<CloudflareWorkerRuntimeModel?> GetByKeyAsync(string workerKey, CancellationToken ct);
    string? BuildRequestUrl(CloudflareWorkerRuntimeModel worker);
    bool IsWorkerEnabled(CloudflareWorkerRuntimeModel? worker);
    Task<(bool Success, string Message, int? CloudflareWorkerId)> CreateAsync(CloudflareWorkerEditModel model, CancellationToken ct);
    Task<(bool Success, string Message)> UpdateAsync(int cloudflareWorkerId, CloudflareWorkerEditModel model, CancellationToken ct);
}

public sealed class CloudflareWorkerService(
    ISqlConnectionFactory connectionFactory) : ICloudflareWorkerService
{
    private const string SchemaUnavailableMessage = "Cloudflare worker settings schema is not available yet. Run the homepage analysis migration or startup schema bootstrap first.";

    public async Task<bool> IsAvailableAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await CloudflareWorkerTableExistsAsync(conn, ct);
    }

    public async Task<IReadOnlyList<CloudflareWorkerListRowModel>> GetListAsync(string? search, CancellationToken ct)
    {
        var normalizedSearch = NormalizeSearch(search);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (!await CloudflareWorkerTableExistsAsync(conn, ct))
            return [];

        var rows = await conn.QueryAsync<CloudflareWorkerListRowModel>(new CommandDefinition(@"
SELECT
  CloudflareWorkerId,
  [Name],
  WorkerKey,
  BaseUrl,
  RoutePath,
  IsEnabled,
  TimeoutSeconds,
  DisplayOrder,
  UpdatedUtc
FROM dbo.CloudflareWorker
WHERE @Search IS NULL
   OR [Name] LIKE @SearchPattern
   OR WorkerKey LIKE @SearchPattern
ORDER BY DisplayOrder ASC, [Name] ASC, CloudflareWorkerId ASC;",
            new
            {
                Search = normalizedSearch,
                SearchPattern = normalizedSearch is null ? null : $"%{normalizedSearch}%"
            },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<CloudflareWorkerEditModel?> GetEditModelAsync(int cloudflareWorkerId, CancellationToken ct)
    {
        if (cloudflareWorkerId <= 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (!await CloudflareWorkerTableExistsAsync(conn, ct))
            return null;

        var row = await conn.QuerySingleOrDefaultAsync<CloudflareWorkerEditDbRow>(new CommandDefinition(@"
SELECT
  CloudflareWorkerId,
  [Name],
  WorkerKey,
  BaseUrl,
  RoutePath,
  AuthHeaderName,
  AuthToken,
  TimeoutSeconds,
  IsEnabled,
  DisplayOrder,
  Notes
FROM dbo.CloudflareWorker
WHERE CloudflareWorkerId = @CloudflareWorkerId;",
            new { CloudflareWorkerId = cloudflareWorkerId },
            cancellationToken: ct));

        if (row is null)
            return null;

        return new CloudflareWorkerEditModel
        {
            CloudflareWorkerId = row.CloudflareWorkerId,
            Name = row.Name,
            WorkerKey = row.WorkerKey,
            BaseUrl = row.BaseUrl,
            RoutePath = row.RoutePath,
            AuthHeaderName = row.AuthHeaderName,
            AuthTokenMasked = MaskSecret(row.AuthToken),
            TimeoutSeconds = row.TimeoutSeconds,
            IsEnabled = row.IsEnabled,
            DisplayOrder = row.DisplayOrder,
            Notes = row.Notes
        };
    }

    public async Task<CloudflareWorkerRuntimeModel?> GetByKeyAsync(string workerKey, CancellationToken ct)
    {
        var normalizedWorkerKey = NormalizeRequired(workerKey, 200);
        if (normalizedWorkerKey is null)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (!await CloudflareWorkerTableExistsAsync(conn, ct))
            return null;

        return await conn.QuerySingleOrDefaultAsync<CloudflareWorkerRuntimeModel>(new CommandDefinition(@"
SELECT TOP 1
  CloudflareWorkerId,
  [Name],
  WorkerKey,
  BaseUrl,
  RoutePath,
  AuthHeaderName,
  AuthToken,
  TimeoutSeconds,
  IsEnabled,
  DisplayOrder,
  Notes,
  CreatedUtc,
  UpdatedUtc
FROM dbo.CloudflareWorker
WHERE WorkerKey = @WorkerKey;",
            new { WorkerKey = normalizedWorkerKey },
            cancellationToken: ct));
    }

    public string? BuildRequestUrl(CloudflareWorkerRuntimeModel worker)
    {
        var baseUrl = NormalizeOptional(worker.BaseUrl, 1000);
        var routePath = NormalizeOptional(worker.RoutePath, 500);
        if (baseUrl is null || routePath is null)
            return null;

        if (!Uri.TryCreate(AppendTrailingSlash(baseUrl), UriKind.Absolute, out var baseUri))
            return null;

        if (!Uri.TryCreate(baseUri, routePath.TrimStart('/'), out var resolved))
            return null;

        return resolved.ToString();
    }

    public bool IsWorkerEnabled(CloudflareWorkerRuntimeModel? worker)
    {
        if (worker is null || !worker.IsEnabled)
            return false;

        return BuildRequestUrl(worker) is not null;
    }

    public async Task<(bool Success, string Message, int? CloudflareWorkerId)> CreateAsync(CloudflareWorkerEditModel model, CancellationToken ct)
    {
        var validationMessage = Validate(model, isCreate: true);
        if (validationMessage is not null)
            return (false, validationMessage, null);

        var normalized = Normalize(model, existingAuthToken: null);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (!await CloudflareWorkerTableExistsAsync(conn, ct))
            return (false, SchemaUnavailableMessage, null);

        var existingId = await conn.ExecuteScalarAsync<int?>(new CommandDefinition(@"
SELECT TOP 1 CloudflareWorkerId
FROM dbo.CloudflareWorker
WHERE WorkerKey = @WorkerKey;",
            new { normalized.WorkerKey },
            cancellationToken: ct));
        if (existingId.HasValue)
            return (false, $"Worker key '{normalized.WorkerKey}' already exists.", null);

        var id = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.CloudflareWorker(
  [Name],
  WorkerKey,
  BaseUrl,
  RoutePath,
  AuthHeaderName,
  AuthToken,
  TimeoutSeconds,
  IsEnabled,
  DisplayOrder,
  Notes,
  CreatedUtc,
  UpdatedUtc)
OUTPUT INSERTED.CloudflareWorkerId
VALUES(
  @Name,
  @WorkerKey,
  @BaseUrl,
  @RoutePath,
  @AuthHeaderName,
  @AuthToken,
  @TimeoutSeconds,
  @IsEnabled,
  @DisplayOrder,
  @Notes,
  SYSUTCDATETIME(),
  SYSUTCDATETIME());",
            normalized,
            cancellationToken: ct));

        return (true, "Cloudflare worker created.", id);
    }

    public async Task<(bool Success, string Message)> UpdateAsync(int cloudflareWorkerId, CloudflareWorkerEditModel model, CancellationToken ct)
    {
        if (cloudflareWorkerId <= 0)
            return (false, "Cloudflare worker not found.");

        model.CloudflareWorkerId = cloudflareWorkerId;
        var validationMessage = Validate(model, isCreate: false);
        if (validationMessage is not null)
            return (false, validationMessage);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        if (!await CloudflareWorkerTableExistsAsync(conn, ct))
            return (false, SchemaUnavailableMessage);

        var existing = await conn.QuerySingleOrDefaultAsync<CloudflareWorkerEditDbRow>(new CommandDefinition(@"
SELECT
  CloudflareWorkerId,
  [Name],
  WorkerKey,
  BaseUrl,
  RoutePath,
  AuthHeaderName,
  AuthToken,
  TimeoutSeconds,
  IsEnabled,
  DisplayOrder,
  Notes
FROM dbo.CloudflareWorker
WHERE CloudflareWorkerId = @CloudflareWorkerId;",
            new { CloudflareWorkerId = cloudflareWorkerId },
            cancellationToken: ct));
        if (existing is null)
            return (false, "Cloudflare worker not found.");

        var normalized = Normalize(model, existing.AuthToken);
        var conflictId = await conn.ExecuteScalarAsync<int?>(new CommandDefinition(@"
SELECT TOP 1 CloudflareWorkerId
FROM dbo.CloudflareWorker
WHERE WorkerKey = @WorkerKey
  AND CloudflareWorkerId <> @CloudflareWorkerId;",
            new
            {
                normalized.WorkerKey,
                CloudflareWorkerId = cloudflareWorkerId
            },
            cancellationToken: ct));
        if (conflictId.HasValue)
            return (false, $"Worker key '{normalized.WorkerKey}' already exists.");

        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CloudflareWorker
SET
  [Name] = @Name,
  WorkerKey = @WorkerKey,
  BaseUrl = @BaseUrl,
  RoutePath = @RoutePath,
  AuthHeaderName = @AuthHeaderName,
  AuthToken = @AuthToken,
  TimeoutSeconds = @TimeoutSeconds,
  IsEnabled = @IsEnabled,
  DisplayOrder = @DisplayOrder,
  Notes = @Notes,
  UpdatedUtc = SYSUTCDATETIME()
WHERE CloudflareWorkerId = @CloudflareWorkerId;",
            new
            {
                CloudflareWorkerId = cloudflareWorkerId,
                normalized.Name,
                normalized.WorkerKey,
                normalized.BaseUrl,
                normalized.RoutePath,
                normalized.AuthHeaderName,
                normalized.AuthToken,
                normalized.TimeoutSeconds,
                normalized.IsEnabled,
                normalized.DisplayOrder,
                normalized.Notes
            },
            cancellationToken: ct));

        return updated > 0
            ? (true, "Cloudflare worker updated.")
            : (false, "Cloudflare worker not found.");
    }

    private static string? Validate(CloudflareWorkerEditModel model, bool isCreate)
    {
        if (NormalizeRequired(model.Name, 200) is null)
            return "Name is required.";
        if (NormalizeRequired(model.WorkerKey, 200) is null)
            return "Worker key is required.";
        if (NormalizeRequired(model.RoutePath, 500) is null)
            return "Route path is required.";
        if (NormalizeRequired(model.RoutePath, 500) is { } routePath && !routePath.StartsWith("/", StringComparison.Ordinal))
            return "Route path must start with '/'.";
        if (NormalizeOptional(model.BaseUrl, 1000) is { } baseUrl
            && !Uri.TryCreate(AppendTrailingSlash(baseUrl), UriKind.Absolute, out _))
            return "Base URL must be a valid absolute URL.";
        if (model.TimeoutSeconds < 1 || model.TimeoutSeconds > 300)
            return "Timeout seconds must be between 1 and 300.";
        if (model.DisplayOrder < 0)
            return "Display order must be zero or greater.";
        if (!isCreate && model.CloudflareWorkerId <= 0)
            return "Cloudflare worker not found.";
        return null;
    }

    private static NormalizedCloudflareWorkerModel Normalize(CloudflareWorkerEditModel model, string? existingAuthToken)
    {
        var authToken = NormalizeOptional(model.AuthToken, 1000);
        if (authToken is null)
            authToken = NormalizeOptional(existingAuthToken, 1000);

        return new NormalizedCloudflareWorkerModel(
            NormalizeRequired(model.Name, 200)!,
            NormalizeRequired(model.WorkerKey, 200)!,
            NormalizeOptional(model.BaseUrl, 1000) ?? string.Empty,
            NormalizeRequired(model.RoutePath, 500)!,
            NormalizeOptional(model.AuthHeaderName, 200),
            authToken,
            Math.Clamp(model.TimeoutSeconds, 1, 300),
            model.IsEnabled,
            Math.Max(0, model.DisplayOrder),
            NormalizeOptional(model.Notes, 2000));
    }

    private static string? NormalizeSearch(string? value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var normalized = NormalizeOptional(value, maxLength);
        return string.IsNullOrWhiteSpace(normalized) ? null : normalized;
    }

    private static string? NormalizeOptional(string? value, int maxLength)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return null;
        return normalized.Length <= maxLength ? normalized : normalized[..maxLength];
    }

    private static async Task<bool> CloudflareWorkerTableExistsAsync(SqlConnection conn, CancellationToken ct)
        => await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT CASE WHEN OBJECT_ID(N'dbo.CloudflareWorker', N'U') IS NOT NULL THEN 1 ELSE 0 END;",
            cancellationToken: ct)) == 1;

    private static string AppendTrailingSlash(string value)
        => value.EndsWith("/", StringComparison.Ordinal) ? value : value + "/";

    private static string? MaskSecret(string? value)
    {
        var normalized = NormalizeOptional(value, 1000);
        if (normalized is null)
            return null;
        if (normalized.Length <= 4)
            return new string('*', normalized.Length);
        return new string('*', Math.Max(0, normalized.Length - 4)) + normalized[^4..];
    }

    private sealed record CloudflareWorkerEditDbRow(
        int CloudflareWorkerId,
        string Name,
        string WorkerKey,
        string BaseUrl,
        string RoutePath,
        string? AuthHeaderName,
        string? AuthToken,
        int TimeoutSeconds,
        bool IsEnabled,
        int DisplayOrder,
        string? Notes);

    private sealed record NormalizedCloudflareWorkerModel(
        string Name,
        string WorkerKey,
        string BaseUrl,
        string RoutePath,
        string? AuthHeaderName,
        string? AuthToken,
        int TimeoutSeconds,
        bool IsEnabled,
        int DisplayOrder,
        string? Notes);
}
