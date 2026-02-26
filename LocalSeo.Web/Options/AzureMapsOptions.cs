namespace LocalSeo.Web.Options;

public sealed class AzureMapsOptions
{
    public string? ClientId { get; set; }
    public string PrimaryKey { get; set; } = string.Empty;
    public string SecondaryKey { get; set; } = string.Empty;
}
