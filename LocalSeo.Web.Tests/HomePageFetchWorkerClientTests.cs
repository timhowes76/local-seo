using System.Net;
using System.Net.Http;
using System.Text;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;

public sealed class HomePageFetchWorkerClientTests
{
    [Fact]
    public async Task FetchAsync_WhenWorkerEndpointReturns522_ReturnsConfiguredFailureWithActionableMessage()
    {
        var worker = new CloudflareWorkerRuntimeModel(
            1,
            "Sales Local SEO - Home Page Fetch",
            "SalesLocalSeoHomePageFetch",
            "https://api.example.com",
            "/sales-local-seo-homepage-fetch",
            null,
            null,
            30,
            true,
            10,
            null,
            DateTime.UtcNow,
            DateTime.UtcNow);
        var httpClient = new HttpClient(new StubHttpMessageHandler(new HttpResponseMessage((HttpStatusCode)522)
        {
            Content = new StringContent(string.Empty, Encoding.UTF8, "text/plain")
        }));
        var client = new HomePageFetchWorkerClient(
            httpClient,
            new StubCloudflareWorkerService(worker),
            NullLogger<HomePageFetchWorkerClient>.Instance);

        var result = await client.FetchAsync(worker.WorkerKey, "https://example.com", CancellationToken.None);

        Assert.False(result.Success);
        Assert.True(result.WorkerWasConfigured);
        Assert.Equal("WorkerHttpError", result.ErrorCode);
        Assert.Equal(
            "Configured worker endpoint returned HTTP 522. Cloudflare could not reach the origin for the worker URL. Verify the Base URL and route point to a deployed worker endpoint.",
            result.ErrorMessage);
    }

    private sealed class StubCloudflareWorkerService(CloudflareWorkerRuntimeModel worker) : ICloudflareWorkerService
    {
        public Task<bool> IsAvailableAsync(CancellationToken ct) => Task.FromResult(true);

        public Task<IReadOnlyList<CloudflareWorkerListRowModel>> GetListAsync(string? search, CancellationToken ct)
            => Task.FromResult<IReadOnlyList<CloudflareWorkerListRowModel>>([]);

        public Task<CloudflareWorkerEditModel?> GetEditModelAsync(int cloudflareWorkerId, CancellationToken ct)
            => Task.FromResult<CloudflareWorkerEditModel?>(null);

        public Task<CloudflareWorkerRuntimeModel?> GetByKeyAsync(string workerKey, CancellationToken ct)
            => Task.FromResult<CloudflareWorkerRuntimeModel?>(worker);

        public string? BuildRequestUrl(CloudflareWorkerRuntimeModel worker)
            => "https://api.example.com/sales-local-seo-homepage-fetch";

        public bool IsWorkerEnabled(CloudflareWorkerRuntimeModel? worker)
            => true;

        public Task<(bool Success, string Message, int? CloudflareWorkerId)> CreateAsync(CloudflareWorkerEditModel model, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<(bool Success, string Message)> UpdateAsync(int cloudflareWorkerId, CloudflareWorkerEditModel model, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private sealed class StubHttpMessageHandler(HttpResponseMessage response) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(response);
    }
}
