using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface IHomePageFetchWorkerClient
{
    Task<HomePageFetchWorkerResult> FetchAsync(string workerKey, string websiteUrl, CancellationToken ct);
}

public sealed class HomePageFetchWorkerClient(
    HttpClient httpClient,
    ICloudflareWorkerService cloudflareWorkerService,
    ILogger<HomePageFetchWorkerClient> logger) : IHomePageFetchWorkerClient
{
    public async Task<HomePageFetchWorkerResult> FetchAsync(string workerKey, string websiteUrl, CancellationToken ct)
    {
        var worker = await cloudflareWorkerService.GetByKeyAsync(workerKey, ct);
        if (worker is null)
        {
            return HomePageFetchWorkerResult.Disabled(
                workerKey,
                websiteUrl,
                "Worker configuration was not found.",
                "WorkerNotConfigured");
        }

        if (!worker.IsEnabled)
        {
            return HomePageFetchWorkerResult.Disabled(
                worker.WorkerKey,
                websiteUrl,
                $"Worker '{worker.Name}' is disabled.",
                "WorkerDisabled");
        }

        var requestUrl = cloudflareWorkerService.BuildRequestUrl(worker);
        if (string.IsNullOrWhiteSpace(requestUrl))
        {
            return HomePageFetchWorkerResult.Disabled(
                worker.WorkerKey,
                websiteUrl,
                $"Worker '{worker.Name}' does not have a valid request URL.",
                "WorkerNotConfigured");
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl)
        {
            Content = JsonContent.Create(new { url = websiteUrl })
        };

        if (!string.IsNullOrWhiteSpace(worker.AuthHeaderName) && !string.IsNullOrWhiteSpace(worker.AuthToken))
            request.Headers.TryAddWithoutValidation(worker.AuthHeaderName, worker.AuthToken);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(Math.Clamp(worker.TimeoutSeconds, 1, 300)));

        try
        {
            using var response = await httpClient.SendAsync(request, timeoutCts.Token);
            var responseBody = await response.Content.ReadAsStringAsync(timeoutCts.Token);
            if (!response.IsSuccessStatusCode)
            {
                return new HomePageFetchWorkerResult(
                    worker.WorkerKey,
                    false,
                    websiteUrl,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "WorkerHttpError",
                    BuildHttpErrorMessage(response.StatusCode, responseBody),
                    true);
            }

            WorkerResponsePayload? payload;
            try
            {
                payload = JsonSerializer.Deserialize<WorkerResponsePayload>(responseBody, JsonOptions);
            }
            catch (JsonException ex)
            {
                logger.LogWarning(ex, "Cloudflare worker {WorkerKey} returned invalid JSON.", worker.WorkerKey);
                return new HomePageFetchWorkerResult(
                    worker.WorkerKey,
                    false,
                    websiteUrl,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "WorkerInvalidResponse",
                    "Worker returned invalid JSON.",
                    false);
            }

            if (payload is null)
            {
                return new HomePageFetchWorkerResult(
                    worker.WorkerKey,
                    false,
                    websiteUrl,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "WorkerInvalidResponse",
                    "Worker returned an empty response.",
                    false);
            }

            return new HomePageFetchWorkerResult(
                string.IsNullOrWhiteSpace(payload.WorkerName) ? worker.WorkerKey : payload.WorkerName,
                payload.Success,
                string.IsNullOrWhiteSpace(payload.RequestedUrl) ? websiteUrl : payload.RequestedUrl,
                payload.FinalUrl,
                payload.StatusCode,
                payload.FetchedUtc,
                payload.Html,
                "text/html",
                payload.Success ? null : "WorkerFetchFailed",
                payload.ErrorMessage,
                true);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new HomePageFetchWorkerResult(
                worker.WorkerKey,
                false,
                websiteUrl,
                null,
                null,
                null,
                null,
                null,
                "WorkerTimeout",
                "Worker request timed out.",
                true);
        }
        catch (HttpRequestException ex)
        {
            logger.LogWarning(ex, "Cloudflare worker {WorkerKey} request failed.", worker.WorkerKey);
            return new HomePageFetchWorkerResult(
                worker.WorkerKey,
                false,
                websiteUrl,
                null,
                null,
                null,
                null,
                null,
                "WorkerRequestFailed",
                ex.Message,
                true);
        }
    }

    private static string BuildHttpErrorMessage(HttpStatusCode statusCode, string? body)
    {
        if ((int)statusCode == 522)
        {
            return "Configured worker endpoint returned HTTP 522. Cloudflare could not reach the origin for the worker URL. Verify the Base URL and route point to a deployed worker endpoint.";
        }

        var summary = $"Worker request failed with HTTP {(int)statusCode}.";
        var snippet = (body ?? string.Empty).Trim();
        if (snippet.Length == 0)
            return summary;
        return snippet.Length <= 300 ? $"{summary} {snippet}" : $"{summary} {snippet[..300]}";
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed class WorkerResponsePayload
    {
        public bool Success { get; set; }
        public string? WorkerName { get; set; }
        public string? RequestedUrl { get; set; }
        public string? FinalUrl { get; set; }
        public int? StatusCode { get; set; }
        public DateTime? FetchedUtc { get; set; }
        public string? Html { get; set; }
        public string? ErrorMessage { get; set; }
    }
}

public sealed record HomePageFetchWorkerResult(
    string WorkerName,
    bool Success,
    string RequestedUrl,
    string? FinalUrl,
    int? StatusCode,
    DateTime? FetchedUtc,
    string? Html,
    string? ContentType,
    string? ErrorCode,
    string? ErrorMessage,
    bool WorkerWasConfigured)
{
    public static HomePageFetchWorkerResult Disabled(string workerKey, string requestedUrl, string message, string errorCode)
        => new(
            workerKey,
            false,
            requestedUrl,
            null,
            null,
            null,
            null,
            null,
            errorCode,
            message,
            false);
}
