using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IReviewsProvider
{
    Task FetchAndStoreReviewsAsync(string placeId, CancellationToken ct);
}

public interface IReviewsProviderResolver
{
    IReviewsProvider Resolve(out string providerName);
}

public sealed class ReviewsProviderResolver(IOptions<PlacesOptions> options, NullReviewsProvider none, NotImplementedReviewsProvider notImplemented) : IReviewsProviderResolver
{
    public IReviewsProvider Resolve(out string providerName)
    {
        providerName = options.Value.ReviewsProvider;
        return providerName.Equals("SerpApi", StringComparison.OrdinalIgnoreCase) ? notImplemented : none;
    }
}

public sealed class NullReviewsProvider : IReviewsProvider
{
    public Task FetchAndStoreReviewsAsync(string placeId, CancellationToken ct) => Task.CompletedTask;
}

public sealed class NotImplementedReviewsProvider(ILogger<NotImplementedReviewsProvider> logger) : IReviewsProvider
{
    public Task FetchAndStoreReviewsAsync(string placeId, CancellationToken ct)
    {
        logger.LogWarning("SerpApi provider requested for place {PlaceId}, but not implemented.", placeId);
        return Task.CompletedTask;
    }
}
