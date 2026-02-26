using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IExternalApiHealthService
{
    Task RunChecksAsync(CancellationToken ct);
    Task<IReadOnlyList<ExternalApiHealthWidgetModel>> GetDashboardWidgetsAsync(CancellationToken ct);
}

public sealed class ExternalApiHealthService(
    IEnumerable<IExternalApiStatusChecker> checkers,
    IExternalApiHealthRepository repository,
    ILogger<ExternalApiHealthService> logger) : IExternalApiHealthService
{
    private static readonly SemaphoreSlim RunGate = new(1, 1);

    public async Task RunChecksAsync(CancellationToken ct)
    {
        if (!await RunGate.WaitAsync(0, ct))
        {
            logger.LogInformation("External API health check is already running; skipping duplicate run.");
            return;
        }

        try
        {
            foreach (var checker in checkers)
            {
                ApiStatusResult result;
                try
                {
                    result = await checker.CheckAsync(ct);
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    result = new ApiStatusResult(
                        checker.Name,
                        IsUp: false,
                        IsDegraded: false,
                        CheckedAtUtc: DateTime.UtcNow,
                        LatencyMs: 0,
                        EndpointCalled: "N/A",
                        HttpStatusCode: null,
                        Error: "Health check execution failed.");
                }

                await repository.UpsertAsync(result, ct);

                if (result.IsUp)
                {
                    logger.LogInformation("External API check succeeded for {Name}. HttpStatusCode={HttpStatusCode}.", result.Name, result.HttpStatusCode);
                }
                else
                {
                    logger.LogWarning(
                        "External API check failed for {Name}. HttpStatusCode={HttpStatusCode}. Error={Error}",
                        result.Name,
                        result.HttpStatusCode,
                        result.Error);
                }
            }
        }
        finally
        {
            RunGate.Release();
        }
    }

    public async Task<IReadOnlyList<ExternalApiHealthWidgetModel>> GetDashboardWidgetsAsync(CancellationToken ct)
    {
        var rows = await repository.GetLatestAsync(ct);
        var byName = rows.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);

        var widgets = new List<ExternalApiHealthWidgetModel>();
        foreach (var checker in checkers)
        {
            if (byName.TryGetValue(checker.Name, out var row))
            {
                widgets.Add(new ExternalApiHealthWidgetModel(
                    Name: row.Name,
                    HasData: true,
                    IsUp: row.IsUp,
                    IsDegraded: row.IsDegraded,
                    CheckedAtUtc: row.CheckedAtUtc,
                    LatencyMs: row.LatencyMs,
                    EndpointCalled: row.EndpointCalled,
                    HttpStatusCode: row.HttpStatusCode,
                    LastError: row.Error));
                continue;
            }

            widgets.Add(new ExternalApiHealthWidgetModel(
                Name: checker.Name,
                HasData: false,
                IsUp: false,
                IsDegraded: false,
                CheckedAtUtc: null,
                LatencyMs: null,
                EndpointCalled: "N/A",
                HttpStatusCode: null,
                LastError: null));
        }

        return widgets;
    }
}
