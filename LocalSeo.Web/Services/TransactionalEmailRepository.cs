using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services.TransactionalEmails;

public interface ITransactionalEmailRepository
{
    Task<DiskEmailTemplateRecord?> GetEnabledTemplateByKeyAsync(string templateKey, CancellationToken ct);
    Task<EmailSettingsRecord?> GetEmailSettingsAsync(CancellationToken ct);
}

public sealed class TransactionalEmailRepository(ISqlConnectionFactory connectionFactory) : ITransactionalEmailRepository
{
    public async Task<DiskEmailTemplateRecord?> GetEnabledTemplateByKeyAsync(string templateKey, CancellationToken ct)
    {
        var key = NormalizeRequired(templateKey, 100);
        if (key is null)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<DiskEmailTemplateRecord>(new CommandDefinition(@"
SELECT TOP 1
  EmailTemplateId,
  [Key],
  SubjectTemplate,
  ViewPath,
  IsEnabled,
  CreatedUtc,
  UpdatedUtc
FROM dbo.EmailTemplate
WHERE [Key] = @Key
  AND IsEnabled = 1;", new { Key = key }, cancellationToken: ct));
    }

    public async Task<EmailSettingsRecord?> GetEmailSettingsAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<EmailSettingsRecord>(new CommandDefinition(@"
SELECT TOP 1
  EmailSettingsId,
  FromEmail,
  FromName,
  GlobalSignatureHtml,
  WrapperViewPath,
  UpdatedUtc
FROM dbo.EmailSettings
ORDER BY EmailSettingsId ASC;", cancellationToken: ct));
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
