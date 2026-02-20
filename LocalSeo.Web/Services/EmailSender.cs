using System.Net.Http.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ISendGridEmailService
{
    Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct);
    Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct);
}

public sealed class SendGridEmailService(
    IHttpClientFactory factory,
    IOptions<SendGridOptions> options,
    ILogger<SendGridEmailService> logger) : ISendGridEmailService
{
    public Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct)
    {
        return SendAsync(
            email,
            "Your Local SEO login code",
            $"Your 2FA login code is {code}. It expires in 10 minutes.",
            ct);
    }

    public Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct)
    {
        return SendAsync(
            email,
            "Your Local SEO password reset code",
            $"Use code {code} to reset your password. Reset link: {resetUrl}. The code expires in 10 minutes.",
            ct);
    }

    private async Task SendAsync(string email, string subject, string plainTextBody, CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.ApiKey))
        {
            logger.LogWarning("SendGrid API key not configured; skipping outbound email for {Email}", email);
            return;
        }

        var client = factory.CreateClient();
        using var req = new HttpRequestMessage(HttpMethod.Post, "https://api.sendgrid.com/v3/mail/send");
        req.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", cfg.ApiKey);
        req.Content = JsonContent.Create(new
        {
            from = new { email = cfg.FromEmail, name = cfg.FromName },
            personalizations = new[] { new { to = new[] { new { email } } } },
            subject,
            content = new[] { new { type = "text/plain", value = plainTextBody } }
        });

        var resp = await client.SendAsync(req, ct);
        resp.EnsureSuccessStatusCode();
    }
}
