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
  GoogleQuestionsAndAnswersRefreshHours
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
            GoogleQuestionsAndAnswersRefreshHours = Math.Clamp(model.GoogleQuestionsAndAnswersRefreshHours, 1, 24 * 365)
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
  UpdatedAtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT(AppSettingsId, EnhancedGoogleDataRefreshHours, GoogleReviewsRefreshHours, GoogleUpdatesRefreshHours, GoogleQuestionsAndAnswersRefreshHours, UpdatedAtUtc)
  VALUES(1, @EnhancedGoogleDataRefreshHours, @GoogleReviewsRefreshHours, @GoogleUpdatesRefreshHours, @GoogleQuestionsAndAnswersRefreshHours, SYSUTCDATETIME());",
            normalized, cancellationToken: ct));
    }
}
