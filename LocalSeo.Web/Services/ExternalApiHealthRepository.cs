using Dapper;
using LocalSeo.Web.Data;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IExternalApiHealthRepository
{
    Task UpsertAsync(ApiStatusResult result, CancellationToken ct);
    Task<IReadOnlyList<ExternalApiHealthRow>> GetLatestAsync(CancellationToken ct);
}

public sealed class ExternalApiHealthRepository(ISqlConnectionFactory connectionFactory) : IExternalApiHealthRepository
{
    public async Task UpsertAsync(ApiStatusResult result, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            """
MERGE dbo.ExternalApiHealth WITH (HOLDLOCK) AS target
USING (SELECT @Name AS [Name]) AS src
ON target.[Name] = src.[Name]
WHEN MATCHED THEN
  UPDATE SET
    IsUp = @IsUp,
    IsDegraded = @IsDegraded,
    CheckedAtUtc = @CheckedAtUtc,
    LatencyMs = @LatencyMs,
    EndpointCalled = @EndpointCalled,
    HttpStatusCode = @HttpStatusCode,
    [Error] = @Error
WHEN NOT MATCHED THEN
  INSERT([Name], IsUp, IsDegraded, CheckedAtUtc, LatencyMs, EndpointCalled, HttpStatusCode, [Error])
  VALUES(@Name, @IsUp, @IsDegraded, @CheckedAtUtc, @LatencyMs, @EndpointCalled, @HttpStatusCode, @Error);
""",
            new
            {
                Name = Truncate(result.Name, 64) ?? "Unknown",
                IsUp = result.IsUp,
                IsDegraded = result.IsDegraded,
                CheckedAtUtc = result.CheckedAtUtc,
                LatencyMs = Math.Max(0, result.LatencyMs),
                EndpointCalled = Truncate(result.EndpointCalled, 256) ?? "N/A",
                HttpStatusCode = result.HttpStatusCode,
                Error = Truncate(result.Error, 512)
            },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<ExternalApiHealthRow>> GetLatestAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<ExternalApiHealthRow>(new CommandDefinition(
            """
SELECT
  [Name],
  IsUp,
  IsDegraded,
  CheckedAtUtc,
  LatencyMs,
  EndpointCalled,
  HttpStatusCode,
  [Error]
FROM dbo.ExternalApiHealth
ORDER BY [Name] ASC;
""",
            cancellationToken: ct));

        return rows.ToList();
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
