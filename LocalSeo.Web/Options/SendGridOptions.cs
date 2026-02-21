namespace LocalSeo.Web.Options;

public sealed class SendGridOptions
{
    public string ApiKey { get; set; } = string.Empty;
    public string FromEmail { get; set; } = string.Empty;
    public string FromName { get; set; } = "Local SEO";
    public string EventWebhookPublicKeyPem { get; set; } = string.Empty;
    public string EmailHashSalt { get; set; } = "change-this-email-hash-salt";
}
