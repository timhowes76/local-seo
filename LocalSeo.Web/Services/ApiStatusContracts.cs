namespace LocalSeo.Web.Services;

public enum ApiHealthStatus : byte
{
    Unknown = 0,
    Up = 1,
    Degraded = 2,
    Down = 3
}

public interface IApiStatusCheck
{
    string Key { get; }
    Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct);
}

public sealed record ApiCheckRunResult(
    ApiHealthStatus Status,
    int? LatencyMs,
    string? Message,
    string? DetailsJson,
    int? HttpStatusCode,
    string? ErrorType,
    string? ErrorMessage);

public sealed record ApiStatusCheckDefinitionSeed(
    string Key,
    string DisplayName,
    string Category,
    int IntervalSeconds = 300,
    int TimeoutSeconds = 10,
    int? DegradedThresholdMs = null);

public interface IApiStatusCheckDefinitionProvider
{
    ApiStatusCheckDefinitionSeed Definition { get; }
}

