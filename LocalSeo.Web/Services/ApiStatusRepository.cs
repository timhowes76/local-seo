using Dapper;
using LocalSeo.Web.Data;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public sealed record ApiStatusCheckDefinitionRecord(
    int Id,
    string Key,
    string DisplayName,
    string Category,
    bool IsEnabled,
    int IntervalSeconds,
    int TimeoutSeconds,
    int? DegradedThresholdMs,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record ApiStatusCheckLatestRow(
    int DefinitionId,
    string Key,
    string DisplayName,
    string Category,
    bool IsEnabled,
    int IntervalSeconds,
    int TimeoutSeconds,
    int? DegradedThresholdMs,
    DateTime? CheckedUtc,
    ApiHealthStatus? Status,
    int? LatencyMs,
    string? Message,
    int? HttpStatusCode,
    string? ErrorType);

public sealed record ApiStatusDefinitionUpdate(
    int Id,
    bool IsEnabled,
    int IntervalSeconds,
    int TimeoutSeconds,
    int? DegradedThresholdMs);

public interface IApiStatusRepository
{
    Task EnsureDefinitionsAsync(IReadOnlyList<ApiStatusCheckDefinitionSeed> definitions, CancellationToken ct);
    Task<IReadOnlyList<ApiStatusCheckDefinitionRecord>> GetDefinitionsAsync(bool includeDisabled, CancellationToken ct);
    Task<IReadOnlyList<ApiStatusCheckLatestRow>> GetLatestRowsAsync(bool includeDisabled, CancellationToken ct);
    Task InsertResultAsync(int definitionId, DateTime checkedUtc, ApiCheckRunResult result, CancellationToken ct);
    Task UpdateDefinitionsAsync(IReadOnlyList<ApiStatusDefinitionUpdate> updates, CancellationToken ct);
}

public sealed class ApiStatusRepository(ISqlConnectionFactory connectionFactory) : IApiStatusRepository
{
    public async Task EnsureDefinitionsAsync(IReadOnlyList<ApiStatusCheckDefinitionSeed> definitions, CancellationToken ct)
    {
        if (definitions.Count == 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        foreach (var definition in definitions)
        {
            var key = NormalizeRequired(definition.Key, 100);
            var displayName = NormalizeRequired(definition.DisplayName, 200);
            var category = NormalizeRequired(definition.Category, 100);
            if (key is null || displayName is null || category is null)
                continue;

            await conn.ExecuteAsync(new CommandDefinition(
                """
IF NOT EXISTS (SELECT 1 FROM dbo.ApiStatusCheckDefinition WHERE [Key] = @Key)
BEGIN
  INSERT INTO dbo.ApiStatusCheckDefinition(
    [Key],
    DisplayName,
    Category,
    IsEnabled,
    IntervalSeconds,
    TimeoutSeconds,
    DegradedThresholdMs,
    CreatedUtc,
    UpdatedUtc
  )
  VALUES(
    @Key,
    @DisplayName,
    @Category,
    1,
    @IntervalSeconds,
    @TimeoutSeconds,
    @DegradedThresholdMs,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
  );
END;
""",
                new
                {
                    Key = key,
                    DisplayName = displayName,
                    Category = category,
                    IntervalSeconds = Math.Clamp(definition.IntervalSeconds, 5, 86400),
                    TimeoutSeconds = Math.Clamp(definition.TimeoutSeconds, 1, 120),
                    DegradedThresholdMs = NormalizeNullableInt(definition.DegradedThresholdMs, 1, 300000)
                },
                cancellationToken: ct));
        }
    }

    public async Task<IReadOnlyList<ApiStatusCheckDefinitionRecord>> GetDefinitionsAsync(bool includeDisabled, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<ApiStatusCheckDefinitionRecord>(new CommandDefinition(
            """
SELECT
  Id,
  [Key],
  DisplayName,
  Category,
  IsEnabled,
  IntervalSeconds,
  TimeoutSeconds,
  DegradedThresholdMs,
  CreatedUtc,
  UpdatedUtc
FROM dbo.ApiStatusCheckDefinition
WHERE (@IncludeDisabled = 1 OR IsEnabled = 1)
ORDER BY Category ASC, DisplayName ASC, [Key] ASC;
""",
            new { IncludeDisabled = includeDisabled ? 1 : 0 },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<ApiStatusCheckLatestRow>> GetLatestRowsAsync(bool includeDisabled, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<ApiStatusCheckLatestRow>(new CommandDefinition(
            """
SELECT
  d.Id AS DefinitionId,
  d.[Key],
  d.DisplayName,
  d.Category,
  d.IsEnabled,
  d.IntervalSeconds,
  d.TimeoutSeconds,
  d.DegradedThresholdMs,
  latest.CheckedUtc,
  latest.[Status],
  latest.LatencyMs,
  latest.[Message],
  latest.HttpStatusCode,
  latest.ErrorType
FROM dbo.ApiStatusCheckDefinition d
OUTER APPLY (
  SELECT TOP 1
    r.CheckedUtc,
    r.[Status],
    r.LatencyMs,
    r.[Message],
    r.HttpStatusCode,
    r.ErrorType
  FROM dbo.ApiStatusCheckResult r
  WHERE r.DefinitionId = d.Id
  ORDER BY r.CheckedUtc DESC, r.Id DESC
) latest
WHERE (@IncludeDisabled = 1 OR d.IsEnabled = 1)
ORDER BY d.Category ASC, d.DisplayName ASC, d.[Key] ASC;
""",
            new { IncludeDisabled = includeDisabled ? 1 : 0 },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task InsertResultAsync(int definitionId, DateTime checkedUtc, ApiCheckRunResult result, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            """
INSERT INTO dbo.ApiStatusCheckResult(
  DefinitionId,
  CheckedUtc,
  [Status],
  LatencyMs,
  [Message],
  DetailsJson,
  HttpStatusCode,
  ErrorType,
  ErrorMessage
)
VALUES(
  @DefinitionId,
  @CheckedUtc,
  @Status,
  @LatencyMs,
  @Message,
  @DetailsJson,
  @HttpStatusCode,
  @ErrorType,
  @ErrorMessage
);
""",
            new
            {
                DefinitionId = definitionId,
                CheckedUtc = checkedUtc,
                Status = (byte)result.Status,
                LatencyMs = result.LatencyMs,
                Message = Truncate(result.Message, 500),
                DetailsJson = Truncate(result.DetailsJson, 16000),
                HttpStatusCode = result.HttpStatusCode,
                ErrorType = Truncate(result.ErrorType, 200),
                ErrorMessage = Truncate(result.ErrorMessage, 1000)
            },
            cancellationToken: ct));
    }

    public async Task UpdateDefinitionsAsync(IReadOnlyList<ApiStatusDefinitionUpdate> updates, CancellationToken ct)
    {
        if (updates.Count == 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        foreach (var update in updates)
        {
            if (update.Id <= 0)
                continue;

            await conn.ExecuteAsync(new CommandDefinition(
                """
UPDATE dbo.ApiStatusCheckDefinition
SET
  IsEnabled = @IsEnabled,
  IntervalSeconds = @IntervalSeconds,
  TimeoutSeconds = @TimeoutSeconds,
  DegradedThresholdMs = @DegradedThresholdMs,
  UpdatedUtc = SYSUTCDATETIME()
WHERE Id = @Id;
""",
                new
                {
                    update.Id,
                    IsEnabled = update.IsEnabled,
                    IntervalSeconds = Math.Clamp(update.IntervalSeconds, 5, 86400),
                    TimeoutSeconds = Math.Clamp(update.TimeoutSeconds, 1, 120),
                    DegradedThresholdMs = NormalizeNullableInt(update.DegradedThresholdMs, 1, 300000)
                },
                cancellationToken: ct));
        }
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;

        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static int? NormalizeNullableInt(int? value, int min, int max)
    {
        if (!value.HasValue)
            return null;
        if (value.Value < min)
            return null;

        return Math.Clamp(value.Value, min, max);
    }
}

