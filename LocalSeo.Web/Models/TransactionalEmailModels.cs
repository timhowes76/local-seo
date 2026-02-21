using Microsoft.AspNetCore.Html;

namespace LocalSeo.Web.Models;

public sealed record RenderedEmail(string Subject, string Html);

public sealed record DiskEmailTemplateRecord(
    int EmailTemplateId,
    string Key,
    string SubjectTemplate,
    string ViewPath,
    bool IsEnabled,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record EmailSettingsRecord(
    int EmailSettingsId,
    string FromEmail,
    string FromName,
    string GlobalSignatureHtml,
    string WrapperViewPath,
    DateTime UpdatedUtc);

public sealed class EmailWrapperRenderModel
{
    public string Subject { get; init; } = string.Empty;
    public IHtmlContent BodyHtml { get; init; } = new HtmlString(string.Empty);
    public IHtmlContent SignatureHtml { get; init; } = new HtmlString(string.Empty);
    public string? BrandName { get; init; }
    public string? FromName { get; init; }
}

public sealed class PasswordResetEmailModel
{
    public string RecipientName { get; init; } = string.Empty;
    public string ResetUrl { get; init; } = string.Empty;
}
