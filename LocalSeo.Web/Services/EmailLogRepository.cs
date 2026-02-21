using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IEmailLogRepository
{
    Task<long> CreateQueuedAsync(EmailLogCreateRequest request, CancellationToken ct);
    Task MarkSentAsync(long id, string? providerMessageId, DateTime nowUtc, CancellationToken ct);
    Task MarkFailedAsync(long id, string error, DateTime nowUtc, CancellationToken ct);
    Task<PagedResult<EmailLogListRow>> SearchAsync(EmailLogQuery query, CancellationToken ct);
    Task<EmailLogDetailsRow?> GetDetailsAsync(long id, CancellationToken ct);
    Task<IReadOnlyList<EmailProviderEventRow>> ListEventsAsync(long emailLogId, CancellationToken ct);
    Task<long?> FindByProviderMessageIdAsync(string providerMessageId, CancellationToken ct);
    Task UpdateLastProviderEventAsync(long emailLogId, string eventType, DateTime eventUtc, CancellationToken ct);
}

public interface IEmailProviderEventRepository
{
    Task<bool> InsertIfNotExistsAsync(EmailProviderEventCreateRequest request, CancellationToken ct);
}

public sealed class EmailLogRepository(ISqlConnectionFactory connectionFactory) : IEmailLogRepository, IEmailProviderEventRepository
{
    public async Task<long> CreateQueuedAsync(EmailLogCreateRequest request, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.EmailLog(
  CreatedUtc,
  TemplateKey,
  ToEmail,
  ToEmailHash,
  FromName,
  FromEmail,
  SubjectRendered,
  BodyHtmlRendered,
  IsSensitive,
  RedactionApplied,
  Status,
  Error,
  CorrelationId)
OUTPUT INSERTED.Id
VALUES(
  @CreatedUtc,
  @TemplateKey,
  @ToEmail,
  @ToEmailHash,
  @FromName,
  @FromEmail,
  @SubjectRendered,
  @BodyHtmlRendered,
  @IsSensitive,
  @RedactionApplied,
  @Status,
  @Error,
  @CorrelationId);", request, cancellationToken: ct));
    }

    public async Task MarkSentAsync(long id, string? providerMessageId, DateTime nowUtc, CancellationToken ct)
    {
        var messageId = NormalizeNullable(providerMessageId, 200);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailLog
SET
  Status = N'Sent',
  Error = NULL,
  SendGridMessageId = COALESCE(@SendGridMessageId, SendGridMessageId)
WHERE Id = @Id;", new { Id = id, SendGridMessageId = messageId, NowUtc = nowUtc }, cancellationToken: ct));
    }

    public async Task MarkFailedAsync(long id, string error, DateTime nowUtc, CancellationToken ct)
    {
        var normalizedError = NormalizeNullable(error, 4000);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailLog
SET
  Status = N'Failed',
  Error = @Error
WHERE Id = @Id;", new { Id = id, Error = normalizedError, NowUtc = nowUtc }, cancellationToken: ct));
    }

    public async Task<PagedResult<EmailLogListRow>> SearchAsync(EmailLogQuery query, CancellationToken ct)
    {
        var pageSize = NormalizePageSize(query.PageSize);
        var pageNumber = Math.Max(1, query.PageNumber);

        var whereParts = new List<string>();
        var args = new DynamicParameters();

        if (query.DateFromUtc.HasValue)
        {
            whereParts.Add("el.CreatedUtc >= @DateFromUtc");
            args.Add("DateFromUtc", query.DateFromUtc.Value);
        }

        if (query.DateToUtc.HasValue)
        {
            whereParts.Add("el.CreatedUtc <= @DateToUtc");
            args.Add("DateToUtc", query.DateToUtc.Value);
        }

        var templateKey = NormalizeNullable(query.TemplateKey, 100);
        if (!string.IsNullOrWhiteSpace(templateKey))
        {
            whereParts.Add("el.TemplateKey = @TemplateKey");
            args.Add("TemplateKey", templateKey);
        }

        var status = NormalizeNullable(query.Status, 20);
        if (!string.IsNullOrWhiteSpace(status))
        {
            whereParts.Add("el.Status = @Status");
            args.Add("Status", status);
        }

        var eventType = NormalizeNullable(query.ProviderEventType, 50);
        if (!string.IsNullOrWhiteSpace(eventType))
        {
            whereParts.Add("el.LastProviderEvent = @ProviderEventType");
            args.Add("ProviderEventType", eventType);
        }

        AddContainsFilter(query.RecipientContains, 320, "ToEmail", whereParts, args);
        AddContainsFilter(query.CorrelationId, 64, "CorrelationId", whereParts, args);
        AddContainsFilter(query.MessageId, 200, "SendGridMessageId", whereParts, args);

        var whereSql = whereParts.Count == 0 ? string.Empty : $"WHERE {string.Join(" AND ", whereParts)}";

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition($@"
SELECT COUNT(1)
FROM dbo.EmailLog el
{whereSql};", args, cancellationToken: ct));

        var totalPages = totalCount <= 0 ? 1 : (int)Math.Ceiling(totalCount / (double)pageSize);
        if (pageNumber > totalPages)
            pageNumber = totalPages;
        var offset = (pageNumber - 1) * pageSize;
        args.Add("Offset", offset);
        args.Add("PageSize", pageSize);

        var rows = (await conn.QueryAsync<EmailLogListRow>(new CommandDefinition($@"
SELECT
  el.Id,
  el.CreatedUtc,
  el.TemplateKey,
  el.ToEmail,
  el.Status,
  el.LastProviderEvent,
  el.LastProviderEventUtc,
  el.CorrelationId,
  el.SendGridMessageId
FROM dbo.EmailLog el
{whereSql}
ORDER BY el.CreatedUtc DESC, el.Id DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;", args, cancellationToken: ct))).ToList();

        return new PagedResult<EmailLogListRow>(rows, totalCount);
    }

    public async Task<EmailLogDetailsRow?> GetDetailsAsync(long id, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<EmailLogDetailsRow>(new CommandDefinition(@"
SELECT
  el.Id,
  el.CreatedUtc,
  el.TemplateKey,
  el.ToEmail,
  el.FromName,
  el.FromEmail,
  el.SubjectRendered,
  el.BodyHtmlRendered,
  el.IsSensitive,
  el.RedactionApplied,
  el.Status,
  el.Error,
  el.CorrelationId,
  el.SendGridMessageId,
  el.LastProviderEvent,
  el.LastProviderEventUtc
FROM dbo.EmailLog el
WHERE el.Id = @Id;", new { Id = id }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<EmailProviderEventRow>> ListEventsAsync(long emailLogId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<EmailProviderEventRow>(new CommandDefinition(@"
SELECT
  Id,
  EmailLogId,
  Provider,
  EventType,
  EventUtc,
  ProviderMessageId,
  PayloadJson,
  CreatedUtc
FROM dbo.EmailProviderEvent
WHERE EmailLogId = @EmailLogId
ORDER BY EventUtc DESC, Id DESC;", new { EmailLogId = emailLogId }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<long?> FindByProviderMessageIdAsync(string providerMessageId, CancellationToken ct)
    {
        var normalized = NormalizeNullable(providerMessageId, 200);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<long?>(new CommandDefinition(@"
SELECT TOP 1 el.Id
FROM dbo.EmailLog el
WHERE el.SendGridMessageId = @ProviderMessageId
   OR (el.SendGridMessageId IS NOT NULL AND CHARINDEX(el.SendGridMessageId, @ProviderMessageId) > 0)
   OR (el.SendGridMessageId IS NOT NULL AND CHARINDEX(@ProviderMessageId, el.SendGridMessageId) > 0)
ORDER BY
  CASE
    WHEN el.SendGridMessageId = @ProviderMessageId THEN 0
    WHEN el.SendGridMessageId IS NOT NULL AND CHARINDEX(el.SendGridMessageId, @ProviderMessageId) > 0 THEN 1
    WHEN el.SendGridMessageId IS NOT NULL AND CHARINDEX(@ProviderMessageId, el.SendGridMessageId) > 0 THEN 2
    ELSE 3
  END,
  el.CreatedUtc DESC,
  el.Id DESC;", new { ProviderMessageId = normalized }, cancellationToken: ct));
    }

    public async Task UpdateLastProviderEventAsync(long emailLogId, string eventType, DateTime eventUtc, CancellationToken ct)
    {
        var normalizedType = NormalizeNullable(eventType, 50) ?? "unknown";
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.EmailLog
SET
  LastProviderEvent = @EventType,
  LastProviderEventUtc = @EventUtc
WHERE Id = @EmailLogId
  AND (
    LastProviderEventUtc IS NULL
    OR LastProviderEventUtc <= @EventUtc
  );",
            new { EmailLogId = emailLogId, EventType = normalizedType, EventUtc = eventUtc }, cancellationToken: ct));
    }

    public async Task<bool> InsertIfNotExistsAsync(EmailProviderEventCreateRequest request, CancellationToken ct)
    {
        try
        {
            await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
            var inserted = await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.EmailProviderEvent(
  EmailLogId,
  Provider,
  EventType,
  EventUtc,
  ProviderMessageId,
  PayloadJson,
  CreatedUtc)
VALUES(
  @EmailLogId,
  @Provider,
  @EventType,
  @EventUtc,
  @ProviderMessageId,
  @PayloadJson,
  @CreatedUtc);", request, cancellationToken: ct));
            return inserted > 0;
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            return false;
        }
    }

    private static int NormalizePageSize(int value)
    {
        return value switch
        {
            25 => 25,
            50 => 50,
            100 => 100,
            500 => 500,
            1000 => 1000,
            _ => 100
        };
    }

    private static string? NormalizeNullable(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static void AddContainsFilter(string raw, int maxLength, string column, List<string> whereParts, DynamicParameters args)
    {
        var normalized = NormalizeNullable(raw, maxLength);
        if (string.IsNullOrWhiteSpace(normalized))
            return;

        var parameterName = $"{column}Like";
        whereParts.Add($"el.{column} LIKE @{parameterName} ESCAPE '\\'");
        args.Add(parameterName, $"%{EscapeLike(normalized)}%");
    }

    private static string EscapeLike(string input)
    {
        return input
            .Replace(@"\", @"\\", StringComparison.Ordinal)
            .Replace("%", @"\%", StringComparison.Ordinal)
            .Replace("_", @"\_", StringComparison.Ordinal)
            .Replace("[", @"\[", StringComparison.Ordinal);
    }
}

public interface IEmailLogQueryService
{
    Task<PagedResult<EmailLogListRow>> SearchAsync(EmailLogQuery query, CancellationToken ct);
    Task<EmailLogDetailsViewModel?> GetDetailsAsync(long emailLogId, CancellationToken ct);
}

public sealed class EmailLogQueryService(
    IEmailLogRepository repository) : IEmailLogQueryService
{
    public Task<PagedResult<EmailLogListRow>> SearchAsync(EmailLogQuery query, CancellationToken ct) => repository.SearchAsync(query, ct);

    public async Task<EmailLogDetailsViewModel?> GetDetailsAsync(long emailLogId, CancellationToken ct)
    {
        var log = await repository.GetDetailsAsync(emailLogId, ct);
        if (log is null)
            return null;

        var events = await repository.ListEventsAsync(emailLogId, ct);
        return new EmailLogDetailsViewModel
        {
            Log = log,
            Events = events
        };
    }
}
