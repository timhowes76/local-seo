using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class OpenAiConfiguredApiStatusCheck(
    IAdminSettingsService adminSettingsService,
    IOptions<OpenAiOptions> openAiOptions) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "openai.config",
        DisplayName: "OpenAI Configuration",
        Category: "AI",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: null);

    public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var settings = await adminSettingsService.GetAsync(ct);
        var apiKey = !string.IsNullOrWhiteSpace(settings.OpenAiApiKey)
            ? settings.OpenAiApiKey
            : openAiOptions.Value.ApiKey;
        var baseUrl = (openAiOptions.Value.ApiBaseUrl ?? string.Empty).Trim();
        var hasProtectedKey = !string.IsNullOrWhiteSpace(settings.OpenAiApiKeyProtected);
        var hasAnyKey = !string.IsNullOrWhiteSpace(apiKey);

        if (string.IsNullOrWhiteSpace(baseUrl) || !hasAnyKey)
        {
            if (hasProtectedKey && !hasAnyKey)
            {
                return new ApiCheckRunResult(
                    ApiHealthStatus.Down,
                    (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                    "Stored key is unreadable",
                    null,
                    null,
                    "SecretUnavailable",
                    "An encrypted OpenAI key exists but could not be read on this host.");
            }

            return new ApiCheckRunResult(
                ApiHealthStatus.Unknown,
                (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                "Not configured",
                null,
                null,
                null,
                null);
        }

        return new ApiCheckRunResult(
            ApiHealthStatus.Up,
            (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
            "Configured",
            null,
            null,
            null,
            null);
    }
}
