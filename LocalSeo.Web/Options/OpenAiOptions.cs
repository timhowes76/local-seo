namespace LocalSeo.Web.Options;

public sealed class OpenAiOptions
{
    public string ApiBaseUrl { get; set; } = "https://api.openai.com/v1/responses";
    public string ApiKey { get; set; } = string.Empty;
    public string DefaultModel { get; set; } = "gpt-4.1-mini";
    public int TimeoutSeconds { get; set; } = 20;
}
