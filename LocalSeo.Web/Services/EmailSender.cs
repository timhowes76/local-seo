using System.Net.Http.Json;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IEmailSender
{
    Task SendLoginCodeAsync(string email, string code, CancellationToken ct);
}

public sealed class SendGridEmailSender(IHttpClientFactory factory, IOptions<SendGridOptions> options, ILogger<SendGridEmailSender> logger) : IEmailSender
{
    public async Task SendLoginCodeAsync(string email, string code, CancellationToken ct)
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
            subject = "Your Local SEO login code",
            content = new[] { new { type = "text/plain", value = $"Your login code is {code}. It expires in 10 minutes." } }
        });

        var resp = await client.SendAsync(req, ct);
        resp.EnsureSuccessStatusCode();
    }
}
