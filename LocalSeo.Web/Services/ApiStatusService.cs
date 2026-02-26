using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record ApiStatusDashboardSnapshot(
    DateTime RetrievedUtc,
    IReadOnlyList<ApiStatusWidgetModel> Widgets);

public interface IApiStatusService
{
    Task SeedDefinitionsAsync(CancellationToken ct);
    Task WarmLatestCacheAsync(CancellationToken ct);
    Task<ApiStatusDashboardSnapshot> GetDashboardSnapshotAsync(CancellationToken ct);
    Task<ApiStatusDetailsViewModel> GetDetailsAsync(string? category, string? search, CancellationToken ct);
    Task<int> RefreshStaleChecksAsync(bool forceAll, CancellationToken ct);
    Task<ApiStatusDashboardSnapshot> RefreshAllChecksAsync(CancellationToken ct);
    Task<IReadOnlyList<AdminApiStatusDefinitionRowModel>> GetAdminDefinitionsAsync(CancellationToken ct);
    Task UpdateAdminDefinitionsAsync(IReadOnlyList<AdminApiStatusDefinitionRowModel> rows, CancellationToken ct);
}

public sealed class ApiStatusService(
    IEnumerable<IApiStatusCheck> checks,
    IApiStatusRepository repository,
    IApiStatusLatestCache latestCache,
    IApiStatusCheckRunner checkRunner,
    TimeProvider timeProvider,
    ILogger<ApiStatusService> logger) : IApiStatusService
{
    private const int MaxParallelRuns = 4;
    private readonly IReadOnlyDictionary<string, IApiStatusCheck> checksByKey = checks
        .GroupBy(x => x.Key.Trim(), StringComparer.OrdinalIgnoreCase)
        .ToDictionary(x => x.Key, x => x.Last(), StringComparer.OrdinalIgnoreCase);
    private readonly IReadOnlyList<ApiStatusCheckDefinitionSeed> definitions = checks
        .OfType<IApiStatusCheckDefinitionProvider>()
        .Select(x => x.Definition)
        .GroupBy(x => x.Key.Trim(), StringComparer.OrdinalIgnoreCase)
        .Select(x => x.Last())
        .ToList();

    public async Task SeedDefinitionsAsync(CancellationToken ct)
    {
        if (definitions.Count == 0)
        {
            logger.LogWarning("No API status check definitions were discovered for seeding.");
            return;
        }

        await repository.EnsureDefinitionsAsync(definitions, ct);
    }

    public async Task WarmLatestCacheAsync(CancellationToken ct)
        => await RefreshLatestCacheAsync(ct);

    public async Task<ApiStatusDashboardSnapshot> GetDashboardSnapshotAsync(CancellationToken ct)
    {
        var snapshot = latestCache.GetSnapshot();
        if (snapshot is null || snapshot.Rows.Count == 0)
            snapshot = await RefreshLatestCacheAsync(ct);

        var widgets = snapshot.Rows
            .Where(x => x.IsEnabled)
            .Select(ToWidgetModel)
            .ToList();

        return new ApiStatusDashboardSnapshot(snapshot.RetrievedUtc, widgets);
    }

    public async Task<ApiStatusDetailsViewModel> GetDetailsAsync(string? category, string? search, CancellationToken ct)
    {
        var snapshot = latestCache.GetSnapshot() ?? await RefreshLatestCacheAsync(ct);
        var normalizedCategory = NormalizeOptional(category);
        var normalizedSearch = NormalizeOptional(search);

        var rows = snapshot.Rows.Select(ToWidgetModel).ToList();
        var categories = rows
            .Select(x => x.Category)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (!string.IsNullOrWhiteSpace(normalizedCategory))
        {
            rows = rows
                .Where(x => string.Equals(x.Category, normalizedCategory, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        if (!string.IsNullOrWhiteSpace(normalizedSearch))
        {
            rows = rows
                .Where(x =>
                    ContainsIgnoreCase(x.DisplayName, normalizedSearch)
                    || ContainsIgnoreCase(x.Key, normalizedSearch)
                    || ContainsIgnoreCase(x.Message, normalizedSearch))
                .ToList();
        }

        return new ApiStatusDetailsViewModel
        {
            Rows = rows,
            CategoryOptions = categories,
            SelectedCategory = normalizedCategory,
            Search = normalizedSearch
        };
    }

    public async Task<int> RefreshStaleChecksAsync(bool forceAll, CancellationToken ct)
    {
        await SeedDefinitionsAsync(ct);
        var definitions = await repository.GetDefinitionsAsync(includeDisabled: false, ct);
        if (definitions.Count == 0)
            return 0;

        var latest = await repository.GetLatestRowsAsync(includeDisabled: true, ct);
        var latestByDefinitionId = latest.ToDictionary(x => x.DefinitionId);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

        var toRun = definitions
            .Where(x =>
                forceAll
                || !latestByDefinitionId.TryGetValue(x.Id, out var latestRow)
                || ApiStatusScheduling.IsStale(latestRow.CheckedUtc, x.IntervalSeconds, nowUtc))
            .ToList();

        if (toRun.Count == 0)
            return 0;

        await RunChecksAsync(toRun, ct);
        await RefreshLatestCacheAsync(ct);
        return toRun.Count;
    }

    public async Task<ApiStatusDashboardSnapshot> RefreshAllChecksAsync(CancellationToken ct)
    {
        await RefreshStaleChecksAsync(forceAll: true, ct);
        return await GetDashboardSnapshotAsync(ct);
    }

    public async Task<IReadOnlyList<AdminApiStatusDefinitionRowModel>> GetAdminDefinitionsAsync(CancellationToken ct)
    {
        await SeedDefinitionsAsync(ct);
        var definitions = await repository.GetDefinitionsAsync(includeDisabled: true, ct);
        return definitions
            .Select(x => new AdminApiStatusDefinitionRowModel
            {
                Id = x.Id,
                Key = x.Key,
                DisplayName = x.DisplayName,
                Category = x.Category,
                IsEnabled = x.IsEnabled,
                IntervalSeconds = x.IntervalSeconds,
                TimeoutSeconds = x.TimeoutSeconds,
                DegradedThresholdMs = x.DegradedThresholdMs
            })
            .ToList();
    }

    public async Task UpdateAdminDefinitionsAsync(IReadOnlyList<AdminApiStatusDefinitionRowModel> rows, CancellationToken ct)
    {
        if (rows.Count == 0)
            return;

        var updates = rows
            .Where(x => x.Id > 0)
            .Select(x => new ApiStatusDefinitionUpdate(
                x.Id,
                x.IsEnabled,
                x.IntervalSeconds,
                x.TimeoutSeconds,
                x.DegradedThresholdMs))
            .ToList();

        await repository.UpdateDefinitionsAsync(updates, ct);
        await RefreshLatestCacheAsync(ct);
    }

    private async Task RunChecksAsync(IReadOnlyList<ApiStatusCheckDefinitionRecord> definitionsToRun, CancellationToken ct)
    {
        var semaphore = new SemaphoreSlim(MaxParallelRuns, MaxParallelRuns);
        var tasks = definitionsToRun.Select(async definition =>
        {
            await semaphore.WaitAsync(ct);
            try
            {
                await RunSingleCheckAsync(definition, ct);
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private async Task RunSingleCheckAsync(ApiStatusCheckDefinitionRecord definition, CancellationToken ct)
    {
        var checkedUtc = timeProvider.GetUtcNow().UtcDateTime;
        if (!checksByKey.TryGetValue(definition.Key, out var check))
        {
            await repository.InsertResultAsync(
                definition.Id,
                checkedUtc,
                new ApiCheckRunResult(
                    ApiHealthStatus.Unknown,
                    null,
                    "Check implementation missing",
                    null,
                    null,
                    "MissingCheck",
                    $"No check implementation is registered for key '{definition.Key}'."),
                ct);
            return;
        }

        var result = await checkRunner.ExecuteAsync(check, definition.TimeoutSeconds, definition.DegradedThresholdMs, ct);
        await repository.InsertResultAsync(definition.Id, checkedUtc, result, ct);
    }

    private async Task<ApiStatusCacheSnapshot> RefreshLatestCacheAsync(CancellationToken ct)
    {
        var rows = await repository.GetLatestRowsAsync(includeDisabled: true, ct);
        var snapshot = new ApiStatusCacheSnapshot(timeProvider.GetUtcNow().UtcDateTime, rows);
        latestCache.SetSnapshot(snapshot);
        return snapshot;
    }

    private static ApiStatusWidgetModel ToWidgetModel(ApiStatusCheckLatestRow row)
    {
        return new ApiStatusWidgetModel(
            row.DefinitionId,
            row.Key,
            row.DisplayName,
            row.Category,
            row.IsEnabled,
            row.Status ?? ApiHealthStatus.Unknown,
            row.CheckedUtc,
            row.LatencyMs,
            row.Message);
    }

    private static string? NormalizeOptional(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static bool ContainsIgnoreCase(string? value, string search)
    {
        if (string.IsNullOrWhiteSpace(value) || string.IsNullOrWhiteSpace(search))
            return false;
        return value.Contains(search, StringComparison.OrdinalIgnoreCase);
    }
}

