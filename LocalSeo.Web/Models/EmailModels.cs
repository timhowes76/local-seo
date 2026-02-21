namespace LocalSeo.Web.Models;

public sealed record EmailTemplateRecord(
    int Id,
    string Key,
    string Name,
    string? FromName,
    string FromEmail,
    string SubjectTemplate,
    string BodyHtmlTemplate,
    bool IsSensitive,
    bool IsEnabled,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed class EmailTemplateEditModel
{
    public int? Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? FromName { get; set; }
    public string FromEmail { get; set; } = string.Empty;
    public string SubjectTemplate { get; set; } = string.Empty;
    public string BodyHtmlTemplate { get; set; } = string.Empty;
    public bool IsSensitive { get; set; }
    public bool IsEnabled { get; set; } = true;
}

public sealed class EmailTemplateListViewModel
{
    public IReadOnlyList<EmailTemplateRecord> Rows { get; init; } = [];
}

public sealed class EmailTemplateEditViewModel
{
    public string Mode { get; init; } = "edit";
    public string? Message { get; init; }
    public EmailTemplateEditModel Template { get; init; } = new();
    public IReadOnlyList<string> AvailableTokens { get; init; } = [];
}

public sealed class EmailLogQuery
{
    public DateTime? DateFromUtc { get; init; }
    public DateTime? DateToUtc { get; init; }
    public string TemplateKey { get; init; } = string.Empty;
    public string Status { get; init; } = string.Empty;
    public string ProviderEventType { get; init; } = string.Empty;
    public string RecipientContains { get; init; } = string.Empty;
    public string CorrelationId { get; init; } = string.Empty;
    public string MessageId { get; init; } = string.Empty;
    public int PageSize { get; init; } = 100;
    public int PageNumber { get; init; } = 1;
}

public sealed class EmailLogListRow
{
    public long Id { get; init; }
    public DateTime CreatedUtc { get; init; }
    public string TemplateKey { get; init; } = string.Empty;
    public string ToEmail { get; init; } = string.Empty;
    public string Status { get; init; } = string.Empty;
    public string? LastProviderEvent { get; init; }
    public DateTime? LastProviderEventUtc { get; init; }
    public string? CorrelationId { get; init; }
    public string? SendGridMessageId { get; init; }
}

public sealed class EmailLogDetailsRow
{
    public long Id { get; init; }
    public DateTime CreatedUtc { get; init; }
    public string TemplateKey { get; init; } = string.Empty;
    public string ToEmail { get; init; } = string.Empty;
    public string? FromName { get; init; }
    public string FromEmail { get; init; } = string.Empty;
    public string SubjectRendered { get; init; } = string.Empty;
    public string BodyHtmlRendered { get; init; } = string.Empty;
    public bool IsSensitive { get; init; }
    public bool RedactionApplied { get; init; }
    public string Status { get; init; } = string.Empty;
    public string? Error { get; init; }
    public string? CorrelationId { get; init; }
    public string? SendGridMessageId { get; init; }
    public string? LastProviderEvent { get; init; }
    public DateTime? LastProviderEventUtc { get; init; }
}

public sealed class EmailProviderEventRow
{
    public long Id { get; init; }
    public long? EmailLogId { get; init; }
    public string Provider { get; init; } = "SendGrid";
    public string EventType { get; init; } = string.Empty;
    public DateTime EventUtc { get; init; }
    public string ProviderMessageId { get; init; } = string.Empty;
    public string? PayloadJson { get; init; }
    public DateTime CreatedUtc { get; init; }
}

public sealed class EmailLogListViewModel
{
    public IReadOnlyList<EmailLogListRow> Rows { get; init; } = [];
    public EmailLogQuery Query { get; init; } = new();
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
    public IReadOnlyList<string> TemplateKeys { get; init; } = [];
}

public sealed class EmailLogDetailsViewModel
{
    public required EmailLogDetailsRow Log { get; init; }
    public IReadOnlyList<EmailProviderEventRow> Events { get; init; } = [];
}

public sealed record EmailLogCreateRequest(
    DateTime CreatedUtc,
    string TemplateKey,
    string ToEmail,
    byte[] ToEmailHash,
    string? FromName,
    string FromEmail,
    string SubjectRendered,
    string BodyHtmlRendered,
    bool IsSensitive,
    bool RedactionApplied,
    string Status,
    string? Error,
    string? CorrelationId);

public sealed record EmailProviderEventCreateRequest(
    long? EmailLogId,
    string Provider,
    string EventType,
    DateTime EventUtc,
    string ProviderMessageId,
    string? PayloadJson,
    DateTime CreatedUtc);

public sealed record EmailSendRequest(
    string TemplateKey,
    string ToEmail,
    IReadOnlyDictionary<string, string> Tokens,
    string? CorrelationId);

public sealed record EmailSendResult(
    bool Success,
    string? ProviderMessageId,
    long EmailLogId,
    string? ErrorMessage);
