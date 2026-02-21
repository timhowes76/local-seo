using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IEmailTemplateRepository
{
    Task<IReadOnlyList<EmailTemplateRecord>> ListAsync(CancellationToken ct);
    Task<EmailTemplateRecord?> GetByIdAsync(int id, CancellationToken ct);
    Task<EmailTemplateRecord?> GetByKeyAsync(string key, CancellationToken ct);
    Task<int> CreateAsync(EmailTemplateEditModel model, DateTime nowUtc, CancellationToken ct);
    Task<bool> UpdateAsync(EmailTemplateEditModel model, DateTime nowUtc, CancellationToken ct);
}

public sealed class EmailTemplateRepository(ISqlConnectionFactory connectionFactory) : IEmailTemplateRepository
{
    public async Task<IReadOnlyList<EmailTemplateRecord>> ListAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<EmailTemplateRecord>(new CommandDefinition(@"
SELECT
  Id,
  [Key],
  [Name],
  FromName,
  FromEmail,
  SubjectTemplate,
  BodyHtmlTemplate,
  IsSensitive,
  IsEnabled,
  CreatedUtc,
  UpdatedUtc
FROM dbo.EmailTemplate
ORDER BY [Key] ASC;", cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<EmailTemplateRecord?> GetByIdAsync(int id, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<EmailTemplateRecord>(new CommandDefinition(@"
SELECT
  Id,
  [Key],
  [Name],
  FromName,
  FromEmail,
  SubjectTemplate,
  BodyHtmlTemplate,
  IsSensitive,
  IsEnabled,
  CreatedUtc,
  UpdatedUtc
FROM dbo.EmailTemplate
WHERE Id=@Id;", new { Id = id }, cancellationToken: ct));
    }

    public async Task<EmailTemplateRecord?> GetByKeyAsync(string key, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<EmailTemplateRecord>(new CommandDefinition(@"
SELECT
  Id,
  [Key],
  [Name],
  FromName,
  FromEmail,
  SubjectTemplate,
  BodyHtmlTemplate,
  IsSensitive,
  IsEnabled,
  CreatedUtc,
  UpdatedUtc
FROM dbo.EmailTemplate
WHERE LOWER([Key]) = LOWER(@Key);", new { Key = key }, cancellationToken: ct));
    }

    public async Task<int> CreateAsync(EmailTemplateEditModel model, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.EmailTemplate(
  [Key],
  [Name],
  FromName,
  FromEmail,
  SubjectTemplate,
  BodyHtmlTemplate,
  IsSensitive,
  IsEnabled,
  CreatedUtc,
  UpdatedUtc)
OUTPUT INSERTED.Id
VALUES(
  @Key,
  @Name,
  @FromName,
  @FromEmail,
  @SubjectTemplate,
  @BodyHtmlTemplate,
  @IsSensitive,
  @IsEnabled,
  @NowUtc,
  @NowUtc);",
            new
            {
                model.Key,
                model.Name,
                model.FromName,
                model.FromEmail,
                model.SubjectTemplate,
                model.BodyHtmlTemplate,
                model.IsSensitive,
                model.IsEnabled,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    public async Task<bool> UpdateAsync(EmailTemplateEditModel model, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var updated = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailTemplate
SET
  [Key] = @Key,
  [Name] = @Name,
  FromName = @FromName,
  FromEmail = @FromEmail,
  SubjectTemplate = @SubjectTemplate,
  BodyHtmlTemplate = @BodyHtmlTemplate,
  IsSensitive = @IsSensitive,
  IsEnabled = @IsEnabled,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;",
            new
            {
                model.Id,
                model.Key,
                model.Name,
                model.FromName,
                model.FromEmail,
                model.SubjectTemplate,
                model.BodyHtmlTemplate,
                model.IsSensitive,
                model.IsEnabled,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
        return updated > 0;
    }
}

public interface IEmailTemplateService
{
    Task<IReadOnlyList<EmailTemplateRecord>> ListAsync(CancellationToken ct);
    Task<EmailTemplateRecord?> GetByIdAsync(int id, CancellationToken ct);
    Task<EmailTemplateRecord?> GetByKeyAsync(string key, CancellationToken ct);
    Task<(bool Success, string Message, int? Id)> CreateAsync(EmailTemplateEditModel model, CancellationToken ct);
    Task<(bool Success, string Message)> UpdateAsync(EmailTemplateEditModel model, CancellationToken ct);
    IReadOnlyList<string> GetAvailableTokens(string templateKey);
}

public sealed class EmailTemplateService(
    IEmailTemplateRepository repository,
    TimeProvider timeProvider,
    ILogger<EmailTemplateService> logger) : IEmailTemplateService
{
    public Task<IReadOnlyList<EmailTemplateRecord>> ListAsync(CancellationToken ct) => repository.ListAsync(ct);

    public Task<EmailTemplateRecord?> GetByIdAsync(int id, CancellationToken ct) => repository.GetByIdAsync(id, ct);

    public Task<EmailTemplateRecord?> GetByKeyAsync(string key, CancellationToken ct) => repository.GetByKeyAsync((key ?? string.Empty).Trim(), ct);

    public async Task<(bool Success, string Message, int? Id)> CreateAsync(EmailTemplateEditModel model, CancellationToken ct)
    {
        var normalized = Normalize(model);
        if (normalized is null)
            return (false, "Template key, name, from email, subject template, and body template are required.", null);

        try
        {
            var id = await repository.CreateAsync(normalized, timeProvider.GetUtcNow().UtcDateTime, ct);
            return (true, "Template created.", id);
        }
        catch (SqlException ex) when (IsUniqueViolation(ex))
        {
            return (false, "Template key already exists.", null);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create email template. Key={TemplateKey}", normalized.Key);
            return (false, "Unable to create template right now.", null);
        }
    }

    public async Task<(bool Success, string Message)> UpdateAsync(EmailTemplateEditModel model, CancellationToken ct)
    {
        var normalized = Normalize(model);
        if (normalized is null || !normalized.Id.HasValue || normalized.Id <= 0)
            return (false, "Invalid template details.");

        try
        {
            var updated = await repository.UpdateAsync(normalized, timeProvider.GetUtcNow().UtcDateTime, ct);
            return updated
                ? (true, "Template saved.")
                : (false, "Template was not found.");
        }
        catch (SqlException ex) when (IsUniqueViolation(ex))
        {
            return (false, "Template key already exists.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update email template. TemplateId={TemplateId}", normalized.Id);
            return (false, "Unable to save template right now.");
        }
    }

    public IReadOnlyList<string> GetAvailableTokens(string templateKey)
    {
        var key = (templateKey ?? string.Empty).Trim();
        if (key.Equals("TwoFactorCode", StringComparison.OrdinalIgnoreCase))
            return ["Code", "ExpiryMinutes"];
        if (key.Equals("PasswordReset", StringComparison.OrdinalIgnoreCase))
            return ["Code", "ResetUrl", "ExpiryMinutes"];
        if (key.Equals("NewUserInvite", StringComparison.OrdinalIgnoreCase))
            return ["RecipientName", "InviteUrl", "ExpiresAtUtc"];
        if (key.Equals("InviteOtp", StringComparison.OrdinalIgnoreCase))
            return ["Code", "ExpiresAtUtc"];
        if (key.Equals("ChangePasswordOtp", StringComparison.OrdinalIgnoreCase))
            return ["Code", "ExpiresAtUtc"];

        return [];
    }

    private static bool IsUniqueViolation(SqlException ex) => ex.Number is 2601 or 2627;

    private static EmailTemplateEditModel? Normalize(EmailTemplateEditModel model)
    {
        var key = NormalizeRequired(model.Key, 100);
        var name = NormalizeRequired(model.Name, 200);
        var fromEmail = NormalizeRequired(model.FromEmail, 320);
        var subject = NormalizeRequired(model.SubjectTemplate, 4000);
        var body = NormalizeRequired(model.BodyHtmlTemplate, int.MaxValue);
        if (key is null || name is null || fromEmail is null || subject is null || body is null)
            return null;

        return new EmailTemplateEditModel
        {
            Id = model.Id,
            Key = key,
            Name = name,
            FromName = NormalizeOptional(model.FromName, 200),
            FromEmail = fromEmail,
            SubjectTemplate = subject,
            BodyHtmlTemplate = body,
            IsSensitive = model.IsSensitive,
            IsEnabled = model.IsEnabled
        };
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? NormalizeOptional(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
