using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace LocalSeo.Web.Services;

public interface ISearchIngestionService
{
    Task<long> RunAsync(SearchFormModel model, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct);
    Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct);
    Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct);
    Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct);
}

public sealed class SearchIngestionService(
    ISqlConnectionFactory connectionFactory,
    IGooglePlacesClient google,
    IOptions<PlacesOptions> placesOptions,
    IReviewsProviderResolver reviewsProviderResolver,
    ILogger<SearchIngestionService> logger) : ISearchIngestionService
{
    public async Task<long> RunAsync(SearchFormModel model, CancellationToken ct)
    {
        var center = await google.GeocodeAsync(model.LocationName, placesOptions.Value.GeocodeCountryCode, ct);
        if (center is null)
            throw new InvalidOperationException($"Could not determine coordinates for '{model.LocationName}'.");

        var (centerLat, centerLng) = center.Value;
        var places = await google.SearchAsync(model.SeedKeyword, model.LocationName, centerLat, centerLng, model.RadiusMeters, model.ResultLimit, ct);

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var runId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.SearchRun(SeedKeyword,LocationName,CenterLat,CenterLng,RadiusMeters,ResultLimit)
OUTPUT INSERTED.SearchRunId
VALUES(@SeedKeyword,@LocationName,@CenterLat,@CenterLng,@RadiusMeters,@ResultLimit)",
            new
            {
                model.SeedKeyword,
                model.LocationName,
                CenterLat = centerLat,
                CenterLng = centerLng,
                model.RadiusMeters,
                model.ResultLimit
            }, tx, cancellationToken: ct));

        var provider = reviewsProviderResolver.Resolve(out var providerName);
        if (providerName.Equals("SerpApi", StringComparison.OrdinalIgnoreCase) && model.FetchReviews)
            logger.LogWarning("Reviews provider selected as SerpApi, but implementation is pending.");

        for (var i = 0; i < places.Count; i++)
        {
            var p = places[i];
            await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.Place AS target
USING (SELECT @PlaceId AS PlaceId) AS source
ON target.PlaceId = source.PlaceId
WHEN MATCHED THEN UPDATE SET
  DisplayName=@DisplayName,
  PrimaryType=@PrimaryType,
  PrimaryCategory=@PrimaryCategory,
  TypesCsv=@TypesCsv,
  FormattedAddress=@FormattedAddress,
  Lat=@Lat,
  Lng=@Lng,
  NationalPhoneNumber=@NationalPhoneNumber,
  WebsiteUri=@WebsiteUri,
  Description=@Description,
  PhotoCount=@PhotoCount,
  IsServiceAreaBusiness=@IsServiceAreaBusiness,
  BusinessStatus=@BusinessStatus,
  RegularOpeningHoursJson=@RegularOpeningHoursJson,
  LastSeenUtc=SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT(
  PlaceId,DisplayName,PrimaryType,PrimaryCategory,TypesCsv,FormattedAddress,Lat,Lng,NationalPhoneNumber,WebsiteUri,Description,PhotoCount,IsServiceAreaBusiness,BusinessStatus,RegularOpeningHoursJson
)
VALUES(
  @PlaceId,@DisplayName,@PrimaryType,@PrimaryCategory,@TypesCsv,@FormattedAddress,@Lat,@Lng,@NationalPhoneNumber,@WebsiteUri,@Description,@PhotoCount,@IsServiceAreaBusiness,@BusinessStatus,@RegularOpeningHoursJson
);",
                new
                {
                    PlaceId = p.Id,
                    p.DisplayName,
                    p.PrimaryType,
                    p.PrimaryCategory,
                    p.TypesCsv,
                    p.FormattedAddress,
                    p.Lat,
                    p.Lng,
                    p.NationalPhoneNumber,
                    p.WebsiteUri,
                    p.Description,
                    p.PhotoCount,
                    p.IsServiceAreaBusiness,
                    p.BusinessStatus,
                    RegularOpeningHoursJson = p.RegularOpeningHours.Count == 0 ? null : JsonSerializer.Serialize(p.RegularOpeningHours)
                }, tx, cancellationToken: ct));

            await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceSnapshot(SearchRunId,PlaceId,RankPosition,Rating,UserRatingCount)
VALUES(@SearchRunId,@PlaceId,@RankPosition,@Rating,@UserRatingCount)",
                new { SearchRunId = runId, PlaceId = p.Id, RankPosition = i + 1, p.Rating, p.UserRatingCount }, tx, cancellationToken: ct));

            if (model.FetchReviews)
                await provider.FetchAndStoreReviewsAsync(p.Id, ct);
        }

        await tx.CommitAsync(ct);
        return runId;
    }

    public async Task<IReadOnlyList<SearchRun>> GetLatestRunsAsync(int take, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT TOP (@Take) SearchRunId, SeedKeyword, LocationName, CenterLat, CenterLng, RadiusMeters, ResultLimit, RanAtUtc
FROM dbo.SearchRun ORDER BY SearchRunId DESC", new { Take = take }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<SearchRun?> GetRunAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<SearchRun>(new CommandDefinition(@"
SELECT SearchRunId, SeedKeyword, LocationName, CenterLat, CenterLng, RadiusMeters, ResultLimit, RanAtUtc
FROM dbo.SearchRun
WHERE SearchRunId=@RunId", new { RunId = runId }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<PlaceSnapshotRow>> GetRunSnapshotsAsync(long runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<PlaceSnapshotRow>(new CommandDefinition(@"
SELECT s.PlaceSnapshotId, s.SearchRunId, s.PlaceId, s.RankPosition, s.Rating, s.UserRatingCount, s.CapturedAtUtc,
       p.DisplayName, p.PrimaryCategory, p.PhotoCount, p.NationalPhoneNumber, p.Lat, p.Lng, p.FormattedAddress, p.WebsiteUri
FROM dbo.PlaceSnapshot s
JOIN dbo.Place p ON p.PlaceId=s.PlaceId
WHERE s.SearchRunId=@RunId
ORDER BY s.RankPosition", new { RunId = runId }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<PlaceDetailsViewModel?> GetPlaceDetailsAsync(string placeId, long? runId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var place = await conn.QuerySingleOrDefaultAsync<PlaceDetailsRow>(new CommandDefinition(@"
SELECT
  PlaceId,
  DisplayName,
  FormattedAddress,
  PrimaryType,
  PrimaryCategory,
  TypesCsv,
  NationalPhoneNumber,
  WebsiteUri,
  Lat,
  Lng,
  Description,
  PhotoCount,
  IsServiceAreaBusiness,
  BusinessStatus,
  RegularOpeningHoursJson
FROM dbo.Place
WHERE PlaceId=@PlaceId", new { PlaceId = placeId }, cancellationToken: ct));

        if (place is null)
            return null;

        var liveDetails = await google.GetPlaceDetailsAsync(placeId, ct);

        PlaceSnapshotContextRow? active = null;
        if (runId.HasValue)
        {
            active = await conn.QuerySingleOrDefaultAsync<PlaceSnapshotContextRow>(new CommandDefinition(@"
SELECT TOP 1 SearchRunId, RankPosition, Rating, UserRatingCount, CapturedAtUtc
FROM dbo.PlaceSnapshot
WHERE PlaceId=@PlaceId AND SearchRunId=@RunId
ORDER BY CapturedAtUtc DESC", new { PlaceId = placeId, RunId = runId.Value }, cancellationToken: ct));
        }

        active ??= await conn.QuerySingleOrDefaultAsync<PlaceSnapshotContextRow>(new CommandDefinition(@"
SELECT TOP 1 SearchRunId, RankPosition, Rating, UserRatingCount, CapturedAtUtc
FROM dbo.PlaceSnapshot
WHERE PlaceId=@PlaceId
ORDER BY CapturedAtUtc DESC", new { PlaceId = placeId }, cancellationToken: ct));

        var history = (await conn.QueryAsync<PlaceHistoryRow>(new CommandDefinition(@"
SELECT TOP 20 SearchRunId, RankPosition, Rating, UserRatingCount, CapturedAtUtc
FROM dbo.PlaceSnapshot
WHERE PlaceId=@PlaceId
ORDER BY CapturedAtUtc DESC", new { PlaceId = placeId }, cancellationToken: ct))).ToList();

        var effectiveTypesCsv = ChooseBestTypesCsv(place.TypesCsv, liveDetails?.TypesCsv);
        var primaryType = SelectBestPrimaryType(
            PreferSpecificType(place.PrimaryType, liveDetails?.PrimaryType),
            effectiveTypesCsv);
        var primaryCategory = SelectPrimaryCategoryLabel(
            PreferSpecificCategory(place.PrimaryCategory, liveDetails?.PrimaryCategory),
            primaryType);
        var regularOpeningHours = ParseJsonStringArray(place.RegularOpeningHoursJson);
        if (regularOpeningHours.Count == 0 && liveDetails is { RegularOpeningHours.Count: > 0 })
            regularOpeningHours = liveDetails.RegularOpeningHours;

        var otherCategories = SplitTypes(effectiveTypesCsv)
            .Where(x => !IsGenericType(x))
            .Where(x => !string.Equals(x, primaryType, StringComparison.OrdinalIgnoreCase))
            .Select(HumanizeType)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new PlaceDetailsViewModel
        {
            PlaceId = place.PlaceId,
            DisplayName = PreferNonEmpty(place.DisplayName, liveDetails?.DisplayName),
            FormattedAddress = PreferNonEmpty(place.FormattedAddress, liveDetails?.FormattedAddress),
            PrimaryType = primaryType,
            PrimaryCategory = primaryCategory,
            NationalPhoneNumber = PreferNonEmpty(place.NationalPhoneNumber, liveDetails?.NationalPhoneNumber),
            WebsiteUri = PreferNonEmpty(place.WebsiteUri, liveDetails?.WebsiteUri),
            Lat = place.Lat ?? liveDetails?.Lat,
            Lng = place.Lng ?? liveDetails?.Lng,
            Description = PreferLongerText(place.Description, liveDetails?.Description),
            PhotoCount = place.PhotoCount ?? liveDetails?.PhotoCount,
            IsServiceAreaBusiness = place.IsServiceAreaBusiness ?? liveDetails?.IsServiceAreaBusiness,
            BusinessStatus = HumanizeType(PreferNonEmpty(place.BusinessStatus, liveDetails?.BusinessStatus)),
            RegularOpeningHours = regularOpeningHours,
            OtherCategories = otherCategories,
            ActiveRunId = active?.SearchRunId,
            ActiveRankPosition = active?.RankPosition,
            ActiveRating = active?.Rating,
            ActiveUserRatingCount = active?.UserRatingCount,
            ActiveCapturedAtUtc = active?.CapturedAtUtc,
            History = history
        };
    }

    private static IReadOnlyList<string> SplitTypes(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv))
            return [];
        return csv
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string? ChooseBestTypesCsv(string? currentTypesCsv, string? liveTypesCsv)
    {
        var currentSpecific = CountSpecificTypes(currentTypesCsv);
        var liveSpecific = CountSpecificTypes(liveTypesCsv);
        if (liveSpecific > currentSpecific)
            return liveTypesCsv;
        return currentTypesCsv;
    }

    private static int CountSpecificTypes(string? typesCsv)
        => SplitTypes(typesCsv).Count(x => !IsGenericType(x));

    private static string? SelectBestPrimaryType(string? primaryType, string? typesCsv)
    {
        var candidates = SplitTypes(typesCsv).Where(x => !IsGenericType(x)).ToList();
        var nonServiceCandidates = candidates.Where(x => !HasServiceToken(x)).ToList();

        if (nonServiceCandidates.Count > 0)
            return nonServiceCandidates[0];
        if (!string.IsNullOrWhiteSpace(primaryType) && !IsGenericType(primaryType))
            return primaryType;
        if (candidates.Count > 0)
            return candidates[0];
        return primaryType;
    }

    private static string SelectPrimaryCategoryLabel(string? primaryCategory, string? primaryType)
    {
        if (!string.IsNullOrWhiteSpace(primaryCategory) && !IsGenericCategoryLabel(primaryCategory))
            return primaryCategory;
        return HumanizeType(primaryType) ?? "(unknown)";
    }

    private static bool IsGenericType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return false;

        var normalized = NormalizeKey(type);
        if (GenericTypes.Contains(normalized))
            return true;
        return HasServiceToken(normalized) && normalized != "web_designer";
    }

    private static bool IsGenericCategoryLabel(string? label)
    {
        if (string.IsNullOrWhiteSpace(label))
            return false;
        var normalized = NormalizeLabel(label);
        if (GenericCategoryLabels.Contains(normalized))
            return true;
        return normalized.Contains("service");
    }

    private static string? PreferNonEmpty(string? first, string? second)
        => !string.IsNullOrWhiteSpace(first) ? first : second;

    private static string? PreferLongerText(string? first, string? second)
    {
        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        return second.Length > first.Length ? second : first;
    }

    private static string? PreferSpecificType(string? first, string? second)
    {
        var firstGeneric = IsGenericType(first);
        var secondGeneric = IsGenericType(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static string? PreferSpecificCategory(string? first, string? second)
    {
        var firstGeneric = IsGenericCategoryLabel(first);
        var secondGeneric = IsGenericCategoryLabel(second);

        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;
        if (firstGeneric && !secondGeneric)
            return second;
        return first;
    }

    private static string? HumanizeType(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return string.Join(' ',
            value.Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries)
                .Select(token => char.ToUpperInvariant(token[0]) + token[1..].ToLowerInvariant()));
    }

    private static bool HasServiceToken(string value)
    {
        var normalized = NormalizeKey(value);
        return normalized.Contains("_service")
            || normalized.Contains("_services")
            || normalized == "service"
            || normalized == "services";
    }

    private static string NormalizeKey(string value)
        => value.Trim().ToLowerInvariant();

    private static string NormalizeLabel(string value)
        => value.Trim().ToLowerInvariant();

    private static IReadOnlyList<string> ParseJsonStringArray(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];
        try
        {
            var parsed = JsonSerializer.Deserialize<List<string>>(json);
            if (parsed is null)
                return [];
            return parsed.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private sealed record PlaceDetailsRow(
        string PlaceId,
        string? DisplayName,
        string? FormattedAddress,
        string? PrimaryType,
        string? PrimaryCategory,
        string? TypesCsv,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        decimal? Lat,
        decimal? Lng,
        string? Description,
        int? PhotoCount,
        bool? IsServiceAreaBusiness,
        string? BusinessStatus,
        string? RegularOpeningHoursJson);

    private sealed record PlaceSnapshotContextRow(long SearchRunId, int RankPosition, decimal? Rating, int? UserRatingCount, DateTime CapturedAtUtc);

    private static readonly HashSet<string> GenericTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "point_of_interest",
        "establishment",
        "service",
        "services",
        "professional_services",
        "business_to_business_service",
        "local_services"
    };

    private static readonly HashSet<string> GenericCategoryLabels = new(StringComparer.OrdinalIgnoreCase)
    {
        "services",
        "service",
        "business",
        "point of interest",
        "establishment",
        "professional services",
        "local services"
    };
}
