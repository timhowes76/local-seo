using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record DataForSeoPopulateResult(bool Success, string Message, int ReviewsUpserted);
public sealed record DataForSeoPostbackResult(bool Success, string Message);

public interface IDataForSeoTaskTracker
{
    Task<IReadOnlyList<DataForSeoTaskRow>> GetLatestTasksAsync(int take, string? taskType, CancellationToken ct);
    Task<int> RefreshTaskStatusesAsync(CancellationToken ct);
    Task<int> DeleteErrorTasksAsync(string? taskType, CancellationToken ct);
    Task<DataForSeoPopulateResult> PopulateTaskAsync(long dataForSeoReviewTaskId, CancellationToken ct);
    Task<DataForSeoPostbackResult> HandlePostbackAsync(string? taskIdFromQuery, string? tagFromQuery, string payloadJson, CancellationToken ct);
}
