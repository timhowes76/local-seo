using System.Collections.Concurrent;

namespace LocalSeo.Web.Services;

public sealed record ApiStatusCacheSnapshot(
    DateTime RetrievedUtc,
    IReadOnlyList<ApiStatusCheckLatestRow> Rows);

public interface IApiStatusLatestCache
{
    ApiStatusCacheSnapshot? GetSnapshot();
    void SetSnapshot(ApiStatusCacheSnapshot snapshot);
}

public sealed class ApiStatusLatestCache : IApiStatusLatestCache
{
    private readonly object gate = new();
    private ApiStatusCacheSnapshot? snapshot;

    public ApiStatusCacheSnapshot? GetSnapshot()
    {
        lock (gate)
        {
            return snapshot;
        }
    }

    public void SetSnapshot(ApiStatusCacheSnapshot value)
    {
        lock (gate)
        {
            snapshot = value;
        }
    }
}

public sealed record ApiStatusRefreshDecision(bool Allowed, int RetryAfterSeconds);

public interface IApiStatusRefreshRateLimiter
{
    ApiStatusRefreshDecision TryAcquire(string key, DateTime nowUtc);
}

public sealed class ApiStatusRefreshRateLimiter : IApiStatusRefreshRateLimiter
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(30);
    private readonly ConcurrentDictionary<string, DateTime> lastRunByKey = new(StringComparer.OrdinalIgnoreCase);

    public ApiStatusRefreshDecision TryAcquire(string key, DateTime nowUtc)
    {
        var normalizedKey = string.IsNullOrWhiteSpace(key) ? "anonymous" : key.Trim();
        if (lastRunByKey.TryGetValue(normalizedKey, out var lastRunUtc))
        {
            var remaining = Window - (nowUtc - lastRunUtc);
            if (remaining > TimeSpan.Zero)
                return new ApiStatusRefreshDecision(false, Math.Max(1, (int)Math.Ceiling(remaining.TotalSeconds)));
        }

        lastRunByKey[normalizedKey] = nowUtc;
        return new ApiStatusRefreshDecision(true, 0);
    }
}

