namespace LocalSeo.Web.Models;

public sealed class PasswordInputModel
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public string? Autocomplete { get; set; }
    public string? InputMode { get; set; }
    public string? Pattern { get; set; }
    public int? MaxLength { get; set; }
    public string? Placeholder { get; set; }
    public bool Required { get; set; } = true;
    public string CssClass { get; set; } = string.Empty;
}
