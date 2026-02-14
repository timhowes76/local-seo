using Dapper;
using LocalSeo.Web.Data;

namespace LocalSeo.Web.Services;

public sealed class RunDataPurgeResult
{
    public int DataForSeoTaskDeleted { get; init; }
    public int PlaceReviewDeleted { get; init; }
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
DECLARE @reviewCount int = 0;
IF OBJECT_ID('dbo.PlaceReview','U') IS NOT NULL
BEGIN
  DELETE FROM dbo.PlaceReview;
  SET @reviewCount = @@ROWCOUNT;
END
DECLARE @taskCount int = 0;
IF OBJECT_ID('dbo.DataForSeoReviewTask','U') IS NOT NULL
BEGIN
  DELETE FROM dbo.DataForSeoReviewTask;
  SET @taskCount = @@ROWCOUNT;
END

DELETE FROM dbo.PlaceSnapshot;
DECLARE @snapshotCount int = @@ROWCOUNT;

DELETE FROM dbo.SearchRun;
DECLARE @runCount int = @@ROWCOUNT;

DELETE FROM dbo.Place;
DECLARE @placeCount int = @@ROWCOUNT;

SELECT
    @taskCount AS DataForSeoTaskDeleted,
    @reviewCount AS PlaceReviewDeleted,
    @snapshotCount AS PlaceSnapshotDeleted,
    @runCount AS SearchRunDeleted,
    @placeCount AS PlaceDeleted;", transaction: tx, cancellationToken: ct));

        await tx.CommitAsync(ct);

        logger.LogWarning(
            "Admin cleared run data. Deleted {TaskCount} DataForSEO tasks, {ReviewCount} reviews, {SnapshotCount} snapshots, {RunCount} runs, {PlaceCount} places.",
            result.DataForSeoTaskDeleted,
            result.PlaceReviewDeleted,
            result.PlaceSnapshotDeleted,
            result.SearchRunDeleted,
            result.PlaceDeleted);

        return result;
    }
}
