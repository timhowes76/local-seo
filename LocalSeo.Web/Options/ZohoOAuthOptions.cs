namespace LocalSeo.Web.Options;

public sealed class ZohoOAuthOptions
{
    public string AccountsBaseUrl { get; set; } = string.Empty;
    public string CrmApiBaseUrl { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string RedirectUri { get; set; } = string.Empty;
    public string Scopes { get; set; } = "ZohoCRM.modules.leads.ALL,ZohoCRM.settings.modules.READ";
}
