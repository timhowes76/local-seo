using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace LocalSeo.Web.Tests;

public class ApiStatusExecutionTests
{
    [Fact]
    public async Task ExecuteAsync_ReturnsDownTimeout_WhenCheckExceedsTimeout()
    {
        var runner = new ApiStatusCheckRunner(NullLogger<ApiStatusCheckRunner>.Instance);
        var check = new DelayedCheck("timeout.test", TimeSpan.FromSeconds(3));

        var result = await runner.ExecuteAsync(check, timeoutSeconds: 1, degradedThresholdMs: null, CancellationToken.None);

        Assert.Equal(ApiHealthStatus.Down, result.Status);
        Assert.Equal("Timeout", result.ErrorType);
        Assert.NotNull(result.ErrorMessage);
    }

    [Fact]
    public void IsStale_ReturnsExpectedValue_ForBoundaryCases()
    {
        var nowUtc = new DateTime(2026, 2, 25, 12, 0, 0, DateTimeKind.Utc);

        Assert.True(ApiStatusScheduling.IsStale(null, 300, nowUtc));
        Assert.False(ApiStatusScheduling.IsStale(nowUtc.AddSeconds(-299), 300, nowUtc));
        Assert.True(ApiStatusScheduling.IsStale(nowUtc.AddSeconds(-300), 300, nowUtc));
    }

    [Fact]
    public void ApplyLatencyThreshold_MapsUpToDegraded_WhenLatencyExceedsThreshold()
    {
        Assert.Equal(
            ApiHealthStatus.Degraded,
            ApiStatusResultMapping.ApplyLatencyThreshold(ApiHealthStatus.Up, latencyMs: 250, degradedThresholdMs: 200));
        Assert.Equal(
            ApiHealthStatus.Up,
            ApiStatusResultMapping.ApplyLatencyThreshold(ApiHealthStatus.Up, latencyMs: 150, degradedThresholdMs: 200));
        Assert.Equal(
            ApiHealthStatus.Down,
            ApiStatusResultMapping.ApplyLatencyThreshold(ApiHealthStatus.Down, latencyMs: 999, degradedThresholdMs: 200));
        Assert.Equal(
            ApiHealthStatus.Up,
            ApiStatusResultMapping.ApplyLatencyThreshold(ApiHealthStatus.Up, latencyMs: null, degradedThresholdMs: 200));
    }

    private sealed class DelayedCheck(string key, TimeSpan delay) : IApiStatusCheck
    {
        public string Key { get; } = key;

        public async Task<ApiCheckRunResult> ExecuteAsync(CancellationToken ct)
        {
            await Task.Delay(delay, ct);
            return new ApiCheckRunResult(ApiHealthStatus.Up, null, "OK", null, 200, null, null);
        }
    }
}

