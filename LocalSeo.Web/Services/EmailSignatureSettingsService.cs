using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IEmailSignatureSettingsService
{
    Task<AdminEmailSignatureSettingsModel> GetAsync(CancellationToken ct);
    Task SaveAsync(string htmlSignature, CancellationToken ct);
}

public sealed class EmailSignatureSettingsService(
    ISqlConnectionFactory connectionFactory,
    IOptions<SendGridOptions> sendGridOptions) : IEmailSignatureSettingsService
{
    public async Task<AdminEmailSignatureSettingsModel> GetAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<EmailSettingsRecord>(new CommandDefinition(@"
SELECT TOP 1
  EmailSettingsId,
  FromEmail,
  FromName,
  GlobalSignatureHtml,
  WrapperViewPath,
  UpdatedUtc
FROM dbo.EmailSettings
ORDER BY EmailSettingsId ASC;", cancellationToken: ct));

        if (row is null)
        {
            return new AdminEmailSignatureSettingsModel
            {
                GlobalSignatureHtml = "<p>Kind regards,<br/>Local SEO Team</p>",
                WrapperViewPath = "_EmailWrapper.cshtml"
            };
        }

        return new AdminEmailSignatureSettingsModel
        {
            GlobalSignatureHtml = row.GlobalSignatureHtml ?? string.Empty,
            WrapperViewPath = NormalizeRequired(row.WrapperViewPath, 260) ?? "_EmailWrapper.cshtml"
        };
    }

    public async Task SaveAsync(string htmlSignature, CancellationToken ct)
    {
        var signature = NormalizeNullable(htmlSignature, int.MaxValue) ?? string.Empty;
        var nowUtc = DateTime.UtcNow;
        var fallbackFromEmail = NormalizeRequired(sendGridOptions.Value.FromEmail, 320) ?? "noreply@example.local";
        var fallbackFromName = NormalizeRequired(sendGridOptions.Value.FromName, 200) ?? "Local SEO";

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
DECLARE @EmailSettingsId int;
SELECT TOP 1 @EmailSettingsId = EmailSettingsId
FROM dbo.EmailSettings
ORDER BY EmailSettingsId ASC;

IF @EmailSettingsId IS NULL
BEGIN
  INSERT INTO dbo.EmailSettings(
    FromEmail,
    FromName,
    GlobalSignatureHtml,
    WrapperViewPath,
    UpdatedUtc)
  VALUES(
    @FromEmail,
    @FromName,
    @GlobalSignatureHtml,
    N'_EmailWrapper.cshtml',
    @UpdatedUtc);
END
ELSE
BEGIN
  UPDATE dbo.EmailSettings
  SET
    GlobalSignatureHtml = @GlobalSignatureHtml,
    UpdatedUtc = @UpdatedUtc
  WHERE EmailSettingsId = @EmailSettingsId;
END;",
            new
            {
                FromEmail = fallbackFromEmail,
                FromName = fallbackFromName,
                GlobalSignatureHtml = signature,
                UpdatedUtc = nowUtc
            },
            cancellationToken: ct));
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? NormalizeNullable(string? value, int maxLength)
    {
        if (value is null)
            return null;
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
