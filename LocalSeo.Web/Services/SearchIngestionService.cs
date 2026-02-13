using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface ISearchIngestionService
{
    Task<long> RunAsync(SearchFormModel model, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct);
    Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct);
}

public sealed class SearchIngestionService(
    ISqlConnectionFactory connectionFactory,
    IGooglePlacesClient google,
    IReviewsProviderResolver reviewsProviderResolver,
    ILogger<SearchIngestionService> logger) : ISearchIngestionService
{
    public async Task<long> RunAsync(SearchFormModel model, CancellationToken ct)
    {
        var places = await google.SearchAsync(model.SeedKeyword, model.LocationName, model.CenterLat, model.CenterLng, model.RadiusMeters, model.ResultLimit, ct);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var runId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SearchRun(SeedKeyword,LocationName,CenterLat,CenterLng,RadiusMeters,ResultLimit)
OUTPUT INSERTED.SearchRunId
VALUES(@SeedKeyword,@LocationName,@CenterLat,@CenterLng,@RadiusMeters,@ResultLimit)", model, tx, cancellationToken: ct));

        var provider = reviewsProviderResolver.Resolve(out var providerName);
        if (providerName.Equals("SerpApi", StringComparison.OrdinalIgnoreCase) && model.FetchReviews)
            logger.LogWarning("Reviews provider selected as SerpApi, but implementation is pending.");

        for (var i = 0; i < places.Count; i++)
        {
            var p = places[i];
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.Place AS target
USING (SELECT @PlaceId AS PlaceId) AS source
ON target.PlaceId = source.PlaceId
WHEN MATCHED THEN UPDATE SET
  DisplayName=@DisplayName,
  PrimaryType=@PrimaryType,
  TypesCsv=@TypesCsv,
  FormattedAddress=@FormattedAddress,
  Lat=@Lat,
  Lng=@Lng,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT(PlaceId,DisplayName,PrimaryType,TypesCsv,FormattedAddress,Lat,Lng)
VALUES(@PlaceId,@DisplayName,@PrimaryType,@TypesCsv,@FormattedAddress,@Lat,@Lng);",
                new
                {
                    PlaceId = p.Id,
                    p.DisplayName,
                    p.PrimaryType,
                    p.TypesCsv,
                    p.FormattedAddress,
                    p.Lat,
                    p.Lng
                }, tx, cancellationToken: ct));

            await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceSnapshot(SearchRunId,PlaceId,RankPosition,Rating,UserRatingCount)
VALUES(@SearchRunId,@PlaceId,@RankPosition,@Rating,@UserRatingCount)",
                new { SearchRunId = runId, PlaceId = p.Id, RankPosition = i + 1, p.Rating, p.UserRatingCount }, tx, cancellationToken: ct));

            if (model.FetchReviews)
                await provider.FetchAndStoreReviewsAsync(p.Id, ct);
        }

        await tx.CommitAsync(ct);
        return runId;
    }

    public async Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT TOP (@Take) SearchRunId, SeedKeyword, LocationName, CenterLat, CenterLng, RadiusMeters, ResultLimit, RanAtUtc
FROM dbo.SearchRun ORDER BY SearchRunId DESC", new { Take = take }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<PlaceSnapshotRow>(new CommandDefinition(@"
SELECT s.PlaceSnapshotId, s.SearchRunId, s.PlaceId, s.RankPosition, s.Rating, s.UserRatingCount, s.CapturedAtUtc,
       p.DisplayName, p.FormattedAddress
FROM dbo.PlaceSnapshot s
JOIN dbo.Place p ON p.PlaceId=s.PlaceId
WHERE s.SearchRunId=@RunId
ORDER BY s.RankPosition", new { RunId = runId }, cancellationToken: ct));
        return rows.ToList();
    }
}
