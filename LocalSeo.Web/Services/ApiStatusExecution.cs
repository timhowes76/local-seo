namespace LocalSeo.Web.Services;

public static class ApiStatusScheduling
{
    public static bool IsStale(DateTime? checkedUtc, int intervalSeconds, DateTime nowUtc)
    {
        if (!checkedUtc.HasValue)
            return true;

        var normalizedInterval = Math.Clamp(intervalSeconds, 5, 86400);
        return checkedUtc.Value <= nowUtc.AddSeconds(-normalizedInterval);
    }
}

public static class ApiStatusResultMapping
{
    public static ApiHealthStatus ApplyLatencyThreshold(ApiHealthStatus status, int? latencyMs, int? degradedThresholdMs)
    {
        if (status != ApiHealthStatus.Up || !latencyMs.HasValue || !degradedThresholdMs.HasValue || degradedThresholdMs.Value <= 0)
            return status;

        return latencyMs.Value > degradedThresholdMs.Value
            ? ApiHealthStatus.Degraded
            : status;
    }
}

public interface IApiStatusCheckRunner
{
    Task<ApiCheckRunResult> ExecuteAsync(
        IApiStatusCheck check,
        int timeoutSeconds,
        int? degradedThresholdMs,
        CancellationToken ct);
}

public sealed class ApiStatusCheckRunner(ILogger<ApiStatusCheckRunner> logger) : IApiStatusCheckRunner
{
    public async Task<ApiCheckRunResult> ExecuteAsync(
        IApiStatusCheck check,
        int timeoutSeconds,
        int? degradedThresholdMs,
        CancellationToken ct)
    {
        var normalizedTimeoutSeconds = Math.Clamp(timeoutSeconds, 1, 120);
        var startedUtc = DateTime.UtcNow;
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(normalizedTimeoutSeconds));

        try
        {
            var result = await check.ExecuteAsync(timeoutCts.Token);
            var measuredLatency = result.LatencyMs ?? (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds);
            var mappedStatus = ApiStatusResultMapping.ApplyLatencyThreshold(result.Status, measuredLatency, degradedThresholdMs);

            return result with
            {
                Status = mappedStatus,
                LatencyMs = measuredLatency,
                Message = Truncate(result.Message, 500),
                DetailsJson = Truncate(result.DetailsJson, 16000),
                ErrorType = Truncate(result.ErrorType, 200),
                ErrorMessage = Truncate(result.ErrorMessage, 1000)
            };
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new ApiCheckRunResult(
                ApiHealthStatus.Down,
                (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                "Timed out",
                null,
                null,
                "Timeout",
                $"Check timed out after {normalizedTimeoutSeconds} seconds.");
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "API status check {CheckKey} failed.", check.Key);
            return new ApiCheckRunResult(
                ApiHealthStatus.Down,
                (int)Math.Max(0, (DateTime.UtcNow - startedUtc).TotalMilliseconds),
                "Request failed",
                null,
                null,
                Truncate(ex.GetType().Name, 200),
                Truncate(ex.Message, 1000));
        }
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}

