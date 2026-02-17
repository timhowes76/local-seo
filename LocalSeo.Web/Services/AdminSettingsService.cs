using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IAdminSettingsService
{
    Task<AdminSettingsModel> GetAsync(CancellationToken ct);
    Task SaveAsync(AdminSettingsModel model, CancellationToken ct);
}

public sealed class AdminSettingsService(ISqlConnectionFactory connectionFactory) : IAdminSettingsService
{
    public async Task<AdminSettingsModel> GetAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<AdminSettingsModel>(new CommandDefinition(@"
SELECT TOP 1
  EnhancedGoogleDataRefreshHours,
  GoogleReviewsRefreshHours,
  GoogleUpdatesRefreshHours,
  GoogleQuestionsAndAnswersRefreshHours,
  SearchVolumeRefreshCooldownDays,
  MapPackClickSharePercent,
  MapPackCtrPosition1Percent,
  MapPackCtrPosition2Percent,
  MapPackCtrPosition3Percent,
  MapPackCtrPosition4Percent,
  MapPackCtrPosition5Percent,
  MapPackCtrPosition6Percent,
  MapPackCtrPosition7Percent,
  MapPackCtrPosition8Percent,
  MapPackCtrPosition9Percent,
  MapPackCtrPosition10Percent
FROM dbo.AppSettings
WHERE AppSettingsId = 1;", cancellationToken: ct));

        return row ?? new AdminSettingsModel();
    }

    public async Task SaveAsync(AdminSettingsModel model, CancellationToken ct)
    {
        var normalized = new
        {
            EnhancedGoogleDataRefreshHours = Math.Clamp(model.EnhancedGoogleDataRefreshHours, 1, 24 * 365),
            GoogleReviewsRefreshHours = Math.Clamp(model.GoogleReviewsRefreshHours, 1, 24 * 365),
            GoogleUpdatesRefreshHours = Math.Clamp(model.GoogleUpdatesRefreshHours, 1, 24 * 365),
            GoogleQuestionsAndAnswersRefreshHours = Math.Clamp(model.GoogleQuestionsAndAnswersRefreshHours, 1, 24 * 365),
            SearchVolumeRefreshCooldownDays = Math.Clamp(model.SearchVolumeRefreshCooldownDays, 1, 3650),
            MapPackClickSharePercent = Math.Clamp(model.MapPackClickSharePercent, 0, 100),
            MapPackCtrPosition1Percent = Math.Clamp(model.MapPackCtrPosition1Percent, 0, 100),
            MapPackCtrPosition2Percent = Math.Clamp(model.MapPackCtrPosition2Percent, 0, 100),
            MapPackCtrPosition3Percent = Math.Clamp(model.MapPackCtrPosition3Percent, 0, 100),
            MapPackCtrPosition4Percent = Math.Clamp(model.MapPackCtrPosition4Percent, 0, 100),
            MapPackCtrPosition5Percent = Math.Clamp(model.MapPackCtrPosition5Percent, 0, 100),
            MapPackCtrPosition6Percent = Math.Clamp(model.MapPackCtrPosition6Percent, 0, 100),
            MapPackCtrPosition7Percent = Math.Clamp(model.MapPackCtrPosition7Percent, 0, 100),
            MapPackCtrPosition8Percent = Math.Clamp(model.MapPackCtrPosition8Percent, 0, 100),
            MapPackCtrPosition9Percent = Math.Clamp(model.MapPackCtrPosition9Percent, 0, 100),
            MapPackCtrPosition10Percent = Math.Clamp(model.MapPackCtrPosition10Percent, 0, 100)
        };

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.AppSettings AS target
USING (SELECT CAST(1 AS int) AS AppSettingsId) AS source
ON target.AppSettingsId = source.AppSettingsId
WHEN MATCHED THEN UPDATE SET
  EnhancedGoogleDataRefreshHours = @EnhancedGoogleDataRefreshHours,
  GoogleReviewsRefreshHours = @GoogleReviewsRefreshHours,
  GoogleUpdatesRefreshHours = @GoogleUpdatesRefreshHours,
  GoogleQuestionsAndAnswersRefreshHours = @GoogleQuestionsAndAnswersRefreshHours,
  SearchVolumeRefreshCooldownDays = @SearchVolumeRefreshCooldownDays,
  MapPackClickSharePercent = @MapPackClickSharePercent,
  MapPackCtrPosition1Percent = @MapPackCtrPosition1Percent,
  MapPackCtrPosition2Percent = @MapPackCtrPosition2Percent,
  MapPackCtrPosition3Percent = @MapPackCtrPosition3Percent,
  MapPackCtrPosition4Percent = @MapPackCtrPosition4Percent,
  MapPackCtrPosition5Percent = @MapPackCtrPosition5Percent,
  MapPackCtrPosition6Percent = @MapPackCtrPosition6Percent,
  MapPackCtrPosition7Percent = @MapPackCtrPosition7Percent,
  MapPackCtrPosition8Percent = @MapPackCtrPosition8Percent,
  MapPackCtrPosition9Percent = @MapPackCtrPosition9Percent,
  MapPackCtrPosition10Percent = @MapPackCtrPosition10Percent,
  UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(AppSettingsId, EnhancedGoogleDataRefreshHours, GoogleReviewsRefreshHours, GoogleUpdatesRefreshHours, GoogleQuestionsAndAnswersRefreshHours, SearchVolumeRefreshCooldownDays, MapPackClickSharePercent, MapPackCtrPosition1Percent, MapPackCtrPosition2Percent, MapPackCtrPosition3Percent, MapPackCtrPosition4Percent, MapPackCtrPosition5Percent, MapPackCtrPosition6Percent, MapPackCtrPosition7Percent, MapPackCtrPosition8Percent, MapPackCtrPosition9Percent, MapPackCtrPosition10Percent, UpdatedAtUtc)
  VALUES(1, @EnhancedGoogleDataRefreshHours, @GoogleReviewsRefreshHours, @GoogleUpdatesRefreshHours, @GoogleQuestionsAndAnswersRefreshHours, @SearchVolumeRefreshCooldownDays, @MapPackClickSharePercent, @MapPackCtrPosition1Percent, @MapPackCtrPosition2Percent, @MapPackCtrPosition3Percent, @MapPackCtrPosition4Percent, @MapPackCtrPosition5Percent, @MapPackCtrPosition6Percent, @MapPackCtrPosition7Percent, @MapPackCtrPosition8Percent, @MapPackCtrPosition9Percent, @MapPackCtrPosition10Percent, SYSUTCDATETIME());",
            normalized, cancellationToken: ct));
    }
}
