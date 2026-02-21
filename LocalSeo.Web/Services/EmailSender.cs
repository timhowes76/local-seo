using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IEmailSenderService
{
    Task<EmailSendResult> SendAsync(string templateKey, string toEmail, IReadOnlyDictionary<string, string> tokens, string? correlationId, CancellationToken ct);
}

public interface ISendGridEmailService
{
    Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct);
    Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct);
    Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct);
    Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct);
    Task SendChangePasswordOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct);
}

public sealed class EmailSenderService(
    IHttpClientFactory factory,
    IOptions<SendGridOptions> options,
    IEmailTemplateService templateService,
    IEmailTemplateRenderer templateRenderer,
    IEmailRedactionService redactionService,
    IEmailLogRepository emailLogRepository,
    TimeProvider timeProvider,
    ILogger<EmailSenderService> logger) : IEmailSenderService
{
    private static readonly Regex HtmlTagRegex = new("<[^>]*>", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public async Task<EmailSendResult> SendAsync(string templateKey, string toEmail, IReadOnlyDictionary<string, string> tokens, string? correlationId, CancellationToken ct)
    {
        var normalizedTemplateKey = NormalizeRequired(templateKey, 100);
        var normalizedToEmail = NormalizeRequired(toEmail, 320);
        if (normalizedTemplateKey is null || normalizedToEmail is null)
            return new EmailSendResult(false, null, 0, "Template key and recipient email are required.");

        var template = await templateService.GetByKeyAsync(normalizedTemplateKey, ct);
        if (template is null || !template.IsEnabled)
            return new EmailSendResult(false, null, 0, "Email template is missing or disabled.");

        var effectiveSensitive = template.IsSensitive || IsAlwaysSensitiveTemplate(normalizedTemplateKey);
        var normalizedTokens = NormalizeTokens(tokens);
        var subjectRender = templateRenderer.Render(template.SubjectTemplate, normalizedTokens);
        var bodyRender = templateRenderer.Render(template.BodyHtmlTemplate, normalizedTokens);
        if (subjectRender.UnknownTokens.Count > 0 || bodyRender.UnknownTokens.Count > 0)
        {
            var unknown = subjectRender.UnknownTokens
                .Concat(bodyRender.UnknownTokens)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
            logger.LogWarning("Email template rendered with unknown tokens. TemplateKey={TemplateKey} UnknownTokens={UnknownTokens}", normalizedTemplateKey, string.Join(",", unknown));
        }

        var redaction = redactionService.RedactForStorage(
            normalizedTemplateKey,
            effectiveSensitive,
            subjectRender.RenderedText,
            bodyRender.RenderedText,
            normalizedTokens);

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var emailLogId = await emailLogRepository.CreateQueuedAsync(new EmailLogCreateRequest(
            CreatedUtc: nowUtc,
            TemplateKey: normalizedTemplateKey,
            ToEmail: normalizedToEmail,
            ToEmailHash: ComputeEmailHash(normalizedToEmail),
            FromName: NormalizeNullable(template.FromName, 200),
            FromEmail: NormalizeRequired(template.FromEmail, 320) ?? "noreply@example.local",
            SubjectRendered: redaction.SubjectRedacted,
            BodyHtmlRendered: redaction.BodyHtmlRedacted,
            IsSensitive: effectiveSensitive,
            RedactionApplied: redaction.RedactionApplied,
            Status: "Queued",
            Error: null,
            CorrelationId: NormalizeNullable(correlationId, 64)), ct);

        var sendResult = await SendViaProviderAsync(emailLogId, template, normalizedToEmail, subjectRender.RenderedText, bodyRender.RenderedText, ct);
        if (sendResult.Success)
        {
            await emailLogRepository.MarkSentAsync(emailLogId, sendResult.ProviderMessageId, timeProvider.GetUtcNow().UtcDateTime, ct);
            return sendResult with { EmailLogId = emailLogId };
        }

        await emailLogRepository.MarkFailedAsync(emailLogId, sendResult.ErrorMessage ?? "Email send failed.", timeProvider.GetUtcNow().UtcDateTime, ct);
        return sendResult with { EmailLogId = emailLogId };
    }

    private async Task<EmailSendResult> SendViaProviderAsync(long emailLogId, EmailTemplateRecord template, string toEmail, string renderedSubject, string renderedBodyHtml, CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.ApiKey))
            return new EmailSendResult(false, null, 0, "SendGrid API key is not configured.");

        var fromEmail = NormalizeRequired(template.FromEmail, 320) ?? NormalizeRequired(cfg.FromEmail, 320);
        if (string.IsNullOrWhiteSpace(fromEmail))
            return new EmailSendResult(false, null, 0, "Template from-email is not configured.");

        var fromName = NormalizeNullable(template.FromName, 200) ?? NormalizeNullable(cfg.FromName, 200);
        var plainTextBody = BuildPlainText(renderedBodyHtml);

        var client = factory.CreateClient();
        using var req = new HttpRequestMessage(HttpMethod.Post, "https://api.sendgrid.com/v3/mail/send");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", cfg.ApiKey);
        req.Content = JsonContent.Create(new
        {
            from = new { email = fromEmail, name = fromName },
            personalizations = new[]
            {
                new
                {
                    to = new[] { new { email = toEmail } },
                    custom_args = new
                    {
                        email_log_id = emailLogId.ToString()
                    }
                }
            },
            subject = renderedSubject,
            content = new[]
            {
                new { type = "text/plain", value = plainTextBody },
                new { type = "text/html", value = renderedBodyHtml }
            }
        });

        using var response = await client.SendAsync(req, ct);
        var providerMessageId = TryReadProviderMessageId(response);
        if (response.IsSuccessStatusCode)
            return new EmailSendResult(true, providerMessageId, 0, null);

        var body = await response.Content.ReadAsStringAsync(ct);
        var safeError = Truncate($"SendGrid HTTP {(int)response.StatusCode}: {body}", 4000) ?? "SendGrid send failed.";
        logger.LogError("SendGrid send failed. StatusCode={StatusCode} TemplateKey={TemplateKey} Recipient={Recipient}", (int)response.StatusCode, template.Key, toEmail);
        return new EmailSendResult(false, providerMessageId, 0, safeError);
    }

    private byte[] ComputeEmailHash(string normalizedToEmail)
    {
        var salt = options.Value.EmailHashSalt ?? string.Empty;
        var payload = $"{normalizedToEmail.ToLowerInvariant()}{salt}";
        return SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(payload));
    }

    private static Dictionary<string, string> NormalizeTokens(IReadOnlyDictionary<string, string> tokens)
    {
        var normalized = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in tokens)
        {
            var key = NormalizeRequired(pair.Key, 100);
            if (key is null)
                continue;
            normalized[key] = pair.Value ?? string.Empty;
        }
        return normalized;
    }

    private static string BuildPlainText(string html)
    {
        var stripped = HtmlTagRegex.Replace(html ?? string.Empty, " ");
        var decoded = System.Net.WebUtility.HtmlDecode(stripped);
        return string.Join(' ', decoded.Split(['\r', '\n', '\t', ' '], StringSplitOptions.RemoveEmptyEntries));
    }

    private static string? TryReadProviderMessageId(HttpResponseMessage response)
    {
        if (TryReadHeader(response, "X-Message-Id", out var value))
            return NormalizeProviderMessageId(value);
        if (TryReadHeader(response, "x-message-id", out value))
            return NormalizeProviderMessageId(value);
        if (TryReadHeader(response, "X-Message-ID", out value))
            return NormalizeProviderMessageId(value);
        if (TryReadHeader(response, "sg_message_id", out value))
            return NormalizeProviderMessageId(value);
        return null;
    }

    private static string? NormalizeProviderMessageId(string? value)
    {
        var normalized = NormalizeNullable(value, 200);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        normalized = normalized.Trim().Trim('<', '>', '"', '\'');
        var firstSegment = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault();
        return NormalizeNullable(firstSegment ?? normalized, 200);
    }

    private static bool TryReadHeader(HttpResponseMessage response, string headerName, out string value)
    {
        value = string.Empty;
        if (!response.Headers.TryGetValues(headerName, out var values))
            return false;
        value = values.FirstOrDefault() ?? string.Empty;
        return value.Length > 0;
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
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Length <= maxLength ? value : value[..maxLength];
    }

    private static bool IsAlwaysSensitiveTemplate(string templateKey)
    {
        return templateKey.Equals("TwoFactorCode", StringComparison.OrdinalIgnoreCase)
               || templateKey.Equals("PasswordReset", StringComparison.OrdinalIgnoreCase)
               || templateKey.Equals("NewUserInvite", StringComparison.OrdinalIgnoreCase)
               || templateKey.Equals("InviteOtp", StringComparison.OrdinalIgnoreCase)
               || templateKey.Equals("ChangePasswordOtp", StringComparison.OrdinalIgnoreCase);
    }
}

public sealed class SendGridEmailService(
    IEmailSenderService sender,
    IEmailTokenFactory tokenFactory,
    ISecuritySettingsProvider securitySettingsProvider) : ISendGridEmailService
{
    public async Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct)
    {
        var settings = await securitySettingsProvider.GetAsync(ct);
        var result = await sender.SendAsync(
            "TwoFactorCode",
            email,
            tokenFactory.BuildTwoFactorCodeTokens(code, settings.EmailCodeExpiryMinutes),
            correlationId: null,
            ct);
        EnsureSuccess(result);
    }

    public async Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct)
    {
        var settings = await securitySettingsProvider.GetAsync(ct);
        var result = await sender.SendAsync(
            "PasswordReset",
            email,
            tokenFactory.BuildPasswordResetTokens(code, resetUrl, settings.EmailCodeExpiryMinutes),
            correlationId: null,
            ct);
        EnsureSuccess(result);
    }

    public async Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct)
    {
        var result = await sender.SendAsync(
            "NewUserInvite",
            email,
            tokenFactory.BuildNewUserInviteTokens(recipientName, inviteUrl, expiresAtUtc),
            correlationId: null,
            ct);
        EnsureSuccess(result);
    }

    public async Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
    {
        var result = await sender.SendAsync(
            "InviteOtp",
            email,
            tokenFactory.BuildInviteOtpTokens(code, expiresAtUtc),
            correlationId: null,
            ct);
        EnsureSuccess(result);
    }

    public async Task SendChangePasswordOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
    {
        var result = await sender.SendAsync(
            "ChangePasswordOtp",
            email,
            tokenFactory.BuildChangePasswordOtpTokens(code, expiresAtUtc),
            correlationId: null,
            ct);
        EnsureSuccess(result);
    }

    private static void EnsureSuccess(EmailSendResult result)
    {
        if (result.Success)
            return;
        throw new InvalidOperationException(result.ErrorMessage ?? "Email send failed.");
    }
}
