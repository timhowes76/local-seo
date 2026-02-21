using System.Net.Http.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ISendGridEmailService
{
    Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct);
    Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct);
    Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct);
    Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct);
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

    public Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct)
    {
        var friendlyName = string.IsNullOrWhiteSpace(recipientName) ? "there" : recipientName.Trim();
        return SendAsync(
            email,
            "You have been invited to Local SEO",
            $"Hi {friendlyName}, you have been invited to Local SEO. Open this invite link to start onboarding: {inviteUrl}. This link expires at {expiresAtUtc:u}.",
            ct);
    }

    public Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
    {
        return SendAsync(
            email,
            "Your Local SEO invite verification code",
            $"Your invite verification code is {code}. It expires at {expiresAtUtc:u}.",
            ct);
    }

    private async Task SendAsync(string email, string subject, string plainTextBody, CancellationToken ct)
    {
        var cfg = options.Value;
        if (string.IsNullOrWhiteSpace(cfg.ApiKey))
        {
            throw new InvalidOperationException("SendGrid API key is not configured.");
        }

        if (string.IsNullOrWhiteSpace(cfg.FromEmail))
        {
            throw new InvalidOperationException("SendGrid FromEmail is not configured.");
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

        using var resp = await client.SendAsync(req, ct);
        if (resp.IsSuccessStatusCode)
            return;

        var responseBody = await resp.Content.ReadAsStringAsync(ct);
        logger.LogError(
            "SendGrid email send failed. StatusCode={StatusCode} Recipient={Recipient} Body={Body}",
            (int)resp.StatusCode,
            email,
            responseBody);
        throw new HttpRequestException(
            $"SendGrid email send failed with HTTP {(int)resp.StatusCode}.",
            null,
            resp.StatusCode);
    }
}
