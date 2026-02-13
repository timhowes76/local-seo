using Dapper;
using LocalSeo.Web.Data;

namespace LocalSeo.Web.Services;

public sealed class RunDataPurgeResult
{
    public int PlaceSnapshotDeleted { get; init; }
    public int SearchRunDeleted { get; init; }
    public int PlaceDeleted { get; init; }
}

public interface IAdminMaintenanceService
{
    Task<RunDataPurgeResult> ClearRunDataAsync(CancellationToken ct);
}

public sealed class AdminMaintenanceService(
    ISqlConnectionFactory connectionFactory,
    ILogger<AdminMaintenanceService> logger) : IAdminMaintenanceService
{
    public async Task<RunDataPurgeResult> ClearRunDataAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var result = await conn.QuerySingleAsync<RunDataPurgeResult>(new CommandDefinition(@"
DELETE FROM dbo.PlaceSnapshot;
DECLARE @snapshotCount int = @@ROWCOUNT;

DELETE FROM dbo.SearchRun;
DECLARE @runCount int = @@ROWCOUNT;

DELETE FROM dbo.Place;
DECLARE @placeCount int = @@ROWCOUNT;

SELECT
    @snapshotCount AS PlaceSnapshotDeleted,
    @runCount AS SearchRunDeleted,
    @placeCount AS PlaceDeleted;", transaction: tx, cancellationToken: ct));

        await tx.CommitAsync(ct);

        logger.LogWarning(
            "Admin cleared run data. Deleted {SnapshotCount} snapshots, {RunCount} runs, {PlaceCount} places.",
            result.PlaceSnapshotDeleted,
            result.SearchRunDeleted,
            result.PlaceDeleted);

        return result;
    }
}
