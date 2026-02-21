using System.Collections.Concurrent;

namespace LocalSeo.Web.Services;

public interface ISearchRunExecutor
{
    void EnsureRunning(long runId);
    bool IsRunning(long runId);
}

public sealed class SearchRunExecutor(
    IServiceScopeFactory scopeFactory,
    ILogger<SearchRunExecutor> logger) : ISearchRunExecutor
{
    private readonly ConcurrentDictionary<long, Task> runningRuns = new();

    public void EnsureRunning(long runId)
    {
        if (runId <= 0)
            return;

        runningRuns.GetOrAdd(runId, StartRun);
    }

    public bool IsRunning(long runId) => runningRuns.ContainsKey(runId);

    private Task StartRun(long runId)
    {
        return Task.Run(async () =>
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var ingestion = scope.ServiceProvider.GetRequiredService<ISearchIngestionService>();
                var snapshot = await ingestion.GetRunProgressAsync(runId, CancellationToken.None);
                if (snapshot is null)
                    return;
                if (IsTerminalStatus(snapshot.Status))
                    return;

                await ingestion.ExecuteQueuedRunAsync(runId, CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Background search run failed. RunId={RunId}", runId);
            }
            finally
            {
                runningRuns.TryRemove(runId, out _);
            }
        });
    }

    private static bool IsTerminalStatus(string? status)
    {
        if (string.IsNullOrWhiteSpace(status))
            return false;
        return status.Equals("Completed", StringComparison.OrdinalIgnoreCase)
               || status.Equals("Failed", StringComparison.OrdinalIgnoreCase);
    }
}
