namespace LocalSeo.Web.Options;

public sealed class GoogleOptions
{
    public string ApiKey { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string RedirectBaseUrl { get; set; } = string.Empty;
    public string BusinessProfileOAuthRefreshToken { get; set; } = string.Empty;
}
