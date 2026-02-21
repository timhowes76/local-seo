using System.Net.Http.Headers;
using System.Net.Http.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.TransactionalEmails;

public interface IEmailSender
{
    Task SendAsync(string toEmail, string toName, string templateKey, object model, CancellationToken ct);
}

public sealed class SendGridTransactionalEmailSender(
    IHttpClientFactory httpClientFactory,
    IOptions<SendGridOptions> sendGridOptions,
    IEmailTemplateRenderer renderer,
    ITransactionalEmailRepository repository,
    ILogger<SendGridTransactionalEmailSender> logger) : IEmailSender
{
    public async Task SendAsync(string toEmail, string toName, string templateKey, object model, CancellationToken ct)
    {
        var recipientEmail = NormalizeRequired(toEmail, 320) ?? throw new InvalidOperationException("Recipient email is required.");
        var recipientName = NormalizeOptional(toName, 200);
        var normalizedTemplateKey = NormalizeRequired(templateKey, 100) ?? throw new InvalidOperationException("Template key is required.");

        var settings = await repository.GetEmailSettingsAsync(ct)
            ?? throw new InvalidOperationException("Email settings row is missing.");

        var rendered = await renderer.RenderAsync(normalizedTemplateKey, model, ct);
        var apiKey = NormalizeRequired(sendGridOptions.Value.ApiKey, 5000)
            ?? throw new InvalidOperationException("SendGrid API key is not configured.");

        var fromEmail = NormalizeRequired(settings.FromEmail, 320) ?? throw new InvalidOperationException("Email settings FromEmail is required.");
        var fromName = NormalizeRequired(settings.FromName, 200) ?? "Local SEO";

        var client = httpClientFactory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://api.sendgrid.com/v3/mail/send");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        request.Content = JsonContent.Create(new
        {
            from = new { email = fromEmail, name = fromName },
            personalizations = new[]
            {
                new
                {
                    to = new[]
                    {
                        new { email = recipientEmail, name = recipientName }
                    }
                }
            },
            subject = rendered.Subject,
            content = new[]
            {
                new
                {
                    type = "text/html",
                    value = rendered.Html
                }
            }
        });

        try
        {
            using var response = await client.SendAsync(request, ct);
            if (response.IsSuccessStatusCode)
                return;

            var responseBody = await response.Content.ReadAsStringAsync(ct);
            logger.LogError(
                "Transactional email send failed. StatusCode={StatusCode} TemplateKey={TemplateKey} To={ToEmail} ResponseBody={ResponseBody}",
                (int)response.StatusCode,
                normalizedTemplateKey,
                recipientEmail,
                Truncate(responseBody, 2000));
            throw new InvalidOperationException($"SendGrid send failed with HTTP {(int)response.StatusCode}.");
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            logger.LogError(ex, "Transactional email send failed unexpectedly. TemplateKey={TemplateKey} To={ToEmail}", normalizedTemplateKey, recipientEmail);
            throw;
        }
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

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Length <= maxLength ? value : value[..maxLength];
    }
}
