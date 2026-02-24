using System.Collections.Concurrent;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IKeyphraseBulkAddJobService
{
    string Start(AddBulkKeyphrasesRequest request, string ownerKey);
    bool TryGetStatus(string jobId, string ownerKey, out AddBulkKeyphraseJobStatusResponse status);
}

public sealed class KeyphraseBulkAddJobService(
    IServiceScopeFactory scopeFactory,
    TimeProvider timeProvider,
    ILogger<KeyphraseBulkAddJobService> logger) : IKeyphraseBulkAddJobService
{
    private readonly ConcurrentDictionary<string, JobState> jobs = new(StringComparer.Ordinal);

    public string Start(AddBulkKeyphrasesRequest request, string ownerKey)
    {
        var normalizedOwnerKey = NormalizeOwnerKey(ownerKey);
        var jobId = Guid.NewGuid().ToString("N");
        var snapshotRequest = new AddBulkKeyphrasesRequest
        {
            CategoryId = (request.CategoryId ?? string.Empty).Trim(),
            CountyId = request.CountyId,
            TownId = request.TownId,
            Items = (request.Items ?? [])
                .Select(x => new AddBulkKeyphraseItem
                {
                    Keyword = (x.Keyword ?? string.Empty).Trim(),
                    KeywordType = x.KeywordType
                })
                .ToList()
        };

        var state = new JobState(jobId, normalizedOwnerKey, snapshotRequest, timeProvider.GetUtcNow());
        jobs[jobId] = state;
        _ = RunAsync(state);
        PruneCompleted();
        return jobId;
    }

    public bool TryGetStatus(string jobId, string ownerKey, out AddBulkKeyphraseJobStatusResponse status)
    {
        status = new AddBulkKeyphraseJobStatusResponse();
        if (string.IsNullOrWhiteSpace(jobId))
            return false;
        if (!jobs.TryGetValue(jobId, out var state))
            return false;
        if (!string.Equals(state.OwnerKey, NormalizeOwnerKey(ownerKey), StringComparison.Ordinal))
            return false;

        lock (state.Sync)
        {
            status = new AddBulkKeyphraseJobStatusResponse
            {
                JobId = state.JobId,
                TotalCount = state.TotalCount,
                CompletedCount = state.CompletedCount,
                AddedCount = state.AddedCount,
                SkippedCount = state.SkippedCount,
                ErrorCount = state.ErrorCount,
                IsCompleted = state.IsCompleted,
                IsFailed = state.IsFailed,
                Message = state.Message,
                Results = state.Results
                    .Select(x => new AddBulkKeyphraseItemResult
                    {
                        Keyword = x.Keyword,
                        KeywordType = x.KeywordType,
                        Status = x.Status,
                        Message = x.Message
                    })
                    .ToList()
            };
        }

        return true;
    }

    private async Task RunAsync(JobState state)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var keyphraseService = scope.ServiceProvider.GetRequiredService<ICategoryLocationKeywordService>();

            var existing = await keyphraseService.GetKeyphrasesAsync(
                state.Request.TownId,
                state.Request.CategoryId,
                CancellationToken.None);
            var seen = existing.Rows
                .Select(x => KeyphraseSuggestionRules.Normalize(x.Keyword))
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .ToHashSet(StringComparer.Ordinal);

            foreach (var item in state.Request.Items)
            {
                var keyword = (item.Keyword ?? string.Empty).Trim();
                if (keyword.Length == 0)
                {
                    AddResult(state, keyword, item.KeywordType, "error", "Keyword is required.");
                    continue;
                }

                if (keyword.Length > 255)
                    keyword = keyword[..255].Trim();
                if (keyword.Length == 0)
                {
                    AddResult(state, keyword, item.KeywordType, "error", "Keyword is invalid.");
                    continue;
                }

                if (item.KeywordType is not (CategoryLocationKeywordTypes.Modifier or CategoryLocationKeywordTypes.Adjacent))
                {
                    AddResult(state, keyword, item.KeywordType, "error", "Only Modifier or Adjacent keyphrase types are allowed.");
                    continue;
                }

                var normalizedKeyword = KeyphraseSuggestionRules.Normalize(keyword);
                if (seen.Contains(normalizedKeyword))
                {
                    AddResult(state, keyword, item.KeywordType, "skipped", "Duplicate keyphrase skipped.");
                    continue;
                }

                try
                {
                    var summary = await keyphraseService.AddKeywordAndRefreshAsync(
                        state.Request.TownId,
                        state.Request.CategoryId,
                        new CategoryLocationKeywordCreateModel
                        {
                            Keyword = keyword,
                            KeywordType = item.KeywordType
                        },
                        CancellationToken.None);

                    seen.Add(normalizedKeyword);
                    AddResult(
                        state,
                        keyword,
                        item.KeywordType,
                        "added",
                        $"Added. Refreshed: {summary.RefreshedCount}, Errors: {summary.ErrorCount}.");
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
                {
                    seen.Add(normalizedKeyword);
                    AddResult(state, keyword, item.KeywordType, "skipped", "Duplicate keyphrase skipped.");
                }
                catch
                {
                    AddResult(state, keyword, item.KeywordType, "error", "Failed to add keyphrase.");
                }
            }

            lock (state.Sync)
            {
                state.IsCompleted = true;
                state.Message = "Bulk add completed.";
                state.UpdatedAtUtc = timeProvider.GetUtcNow();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Bulk add keyphrase job failed. JobId={JobId}", state.JobId);
            lock (state.Sync)
            {
                state.IsCompleted = true;
                state.IsFailed = true;
                state.Message = "Bulk add failed.";
                state.UpdatedAtUtc = timeProvider.GetUtcNow();
            }
        }
    }

    private void AddResult(JobState state, string keyword, int keywordType, string status, string message)
    {
        lock (state.Sync)
        {
            state.Results.Add(new AddBulkKeyphraseItemResult
            {
                Keyword = keyword,
                KeywordType = keywordType,
                Status = status,
                Message = message
            });
            state.CompletedCount++;
            if (string.Equals(status, "added", StringComparison.Ordinal))
                state.AddedCount++;
            else if (string.Equals(status, "skipped", StringComparison.Ordinal))
                state.SkippedCount++;
            else
                state.ErrorCount++;
            state.UpdatedAtUtc = timeProvider.GetUtcNow();
        }
    }

    private void PruneCompleted()
    {
        var cutoff = timeProvider.GetUtcNow().AddHours(-2);
        foreach (var kvp in jobs)
        {
            var shouldRemove = false;
            lock (kvp.Value.Sync)
            {
                shouldRemove = kvp.Value.IsCompleted && kvp.Value.UpdatedAtUtc <= cutoff;
            }
            if (shouldRemove)
                jobs.TryRemove(kvp.Key, out _);
        }
    }

    private static string NormalizeOwnerKey(string? ownerKey)
    {
        var value = (ownerKey ?? string.Empty).Trim();
        if (value.Length == 0)
            return "unknown";
        return value;
    }

    private sealed class JobState
    {
        public JobState(string jobId, string ownerKey, AddBulkKeyphrasesRequest request, DateTimeOffset now)
        {
            JobId = jobId;
            OwnerKey = ownerKey;
            Request = request;
            TotalCount = request.Items.Count;
            UpdatedAtUtc = now;
        }

        public object Sync { get; } = new();
        public string JobId { get; }
        public string OwnerKey { get; }
        public AddBulkKeyphrasesRequest Request { get; }
        public int TotalCount { get; }
        public int CompletedCount { get; set; }
        public int AddedCount { get; set; }
        public int SkippedCount { get; set; }
        public int ErrorCount { get; set; }
        public bool IsCompleted { get; set; }
        public bool IsFailed { get; set; }
        public string Message { get; set; } = "Queued";
        public DateTimeOffset UpdatedAtUtc { get; set; }
        public List<AddBulkKeyphraseItemResult> Results { get; } = [];
    }
}
