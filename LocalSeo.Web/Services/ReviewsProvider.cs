using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IReviewsProvider
{
    Task FetchAndStoreReviewsAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        bool fetchGoogleReviews,
        bool fetchMyBusinessInfo,
        bool fetchGoogleUpdates,
        bool fetchGoogleQuestionsAndAnswers,
        bool fetchGoogleSocialProfiles,
        CancellationToken ct);
}

public interface IReviewsProviderResolver
{
    IReviewsProvider Resolve(out string providerName);
}

public sealed class ReviewsProviderResolver(
    IOptions<PlacesOptions> options,
    NullReviewsProvider none,
    NotImplementedReviewsProvider notImplemented,
    DataForSeoReviewsProvider dataForSeo) : IReviewsProviderResolver
{
    public IReviewsProvider Resolve(out string providerName)
    {
        providerName = options.Value.ReviewsProvider?.Trim() ?? string.Empty;
        if (providerName.Equals("DataForSeo", StringComparison.OrdinalIgnoreCase)
            || providerName.Equals("DataForSEO", StringComparison.OrdinalIgnoreCase)
            || providerName.Equals("DateForSeo", StringComparison.OrdinalIgnoreCase)
            || providerName.Equals("DateForSEO", StringComparison.OrdinalIgnoreCase))
            return dataForSeo;
        if (providerName.Equals("SerpApi", StringComparison.OrdinalIgnoreCase))
            return notImplemented;
        if (providerName.Equals("None", StringComparison.OrdinalIgnoreCase))
            return none;
        return dataForSeo;
    }
}

public sealed class NullReviewsProvider : IReviewsProvider
{
    public Task FetchAndStoreReviewsAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        bool fetchGoogleReviews,
        bool fetchMyBusinessInfo,
        bool fetchGoogleUpdates,
        bool fetchGoogleQuestionsAndAnswers,
        bool fetchGoogleSocialProfiles,
        CancellationToken ct) => Task.CompletedTask;
}

public sealed class NotImplementedReviewsProvider(ILogger<NotImplementedReviewsProvider> logger) : IReviewsProvider
{
    public Task FetchAndStoreReviewsAsync(
        string placeId,
        int? reviewCount,
        string? locationName,
        decimal? centerLat,
        decimal? centerLng,
        int? radiusMeters,
        bool fetchGoogleReviews,
        bool fetchMyBusinessInfo,
        bool fetchGoogleUpdates,
        bool fetchGoogleQuestionsAndAnswers,
        bool fetchGoogleSocialProfiles,
        CancellationToken ct)
    {
        logger.LogWarning("SerpApi provider requested for place {PlaceId}, but not implemented.", placeId);
        return Task.CompletedTask;
    }
}
