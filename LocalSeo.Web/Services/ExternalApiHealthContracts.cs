namespace LocalSeo.Web.Services;

public interface IExternalApiStatusChecker
{
    string Name { get; }
    Task<ApiStatusResult> CheckAsync(CancellationToken ct);
}

public sealed record ApiStatusResult(
    string Name,
    bool IsUp,
    bool IsDegraded,
    DateTime CheckedAtUtc,
    int LatencyMs,
    string EndpointCalled,
    int? HttpStatusCode,
    string? Error);

public sealed record ExternalApiHealthRow(
    string Name,
    bool IsUp,
    bool IsDegraded,
    DateTime CheckedAtUtc,
    int LatencyMs,
    string EndpointCalled,
    int? HttpStatusCode,
    string? Error);
