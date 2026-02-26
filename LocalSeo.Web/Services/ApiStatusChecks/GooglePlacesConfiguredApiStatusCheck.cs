using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services.ApiStatusChecks;

public sealed class GooglePlacesConfiguredApiStatusCheck(
    IOptions<GoogleOptions> googleOptions) : IApiStatusCheck, IApiStatusCheckDefinitionProvider
{
    public string Key => Definition.Key;

    public ApiStatusCheckDefinitionSeed Definition { get; } = new(
        Key: "google.places.config",
        DisplayName: "Google Places Configuration",
        Category: "Google",
        IntervalSeconds: 300,
        TimeoutSeconds: 10,
        DegradedThresholdMs: null);

    public Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
    {
        var startedUtc = DateTime.UtcNow;
        var apiKey = (googleOptions.Value.ApiKey ?? string.Empty).Trim();
        var configured = apiKey.Length > 0;

        return Task.FromResult(new ApiCheckRunResult(
            configured ? ApiHealthStatus.Up : ApiHealthStatus.Unknown,
            (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
            configured ? "Configured" : "Not configured",
            null,
            null,
            null,
            null));
    }
}

