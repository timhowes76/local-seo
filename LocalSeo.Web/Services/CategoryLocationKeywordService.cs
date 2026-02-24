using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ICategoryLocationKeywordService
{
    Task<LocationCategoryListViewModel> GetLocationCategoriesAsync(long locationId, CancellationToken ct);
    Task<CategoryKeyphrasesViewModel> GetKeyphrasesAsync(long locationId, string categoryId, CancellationToken ct);
    Task<IReadOnlyList<OtherLocationKeyphraseSourceSummary>> GetRecentCategoryLocationsWithKeyphrasesAsync(string categoryId, long excludeLocationId, int take, CancellationToken ct);
    Task<CategoryLocationKeywordRefreshSummary> AddKeywordAndRefreshAsync(long locationId, string categoryId, CategoryLocationKeywordCreateModel model, CancellationToken ct);
    Task<CategoryLocationKeywordRefreshSummary> RefreshKeywordAsync(long locationId, string categoryId, int keywordId, CancellationToken ct);
    Task<CategoryLocationKeywordRefreshSummary> RefreshEligibleKeywordsAsync(long locationId, string categoryId, CancellationToken ct);
    Task<bool> SetMainTermAsync(long locationId, string categoryId, int keywordId, CancellationToken ct);
    Task<bool> SetKeywordTypeAsync(long locationId, string categoryId, int keywordId, int keywordType, CancellationToken ct);
    Task<bool> DeleteKeywordAsync(long locationId, string categoryId, int keywordId, CancellationToken ct);
}

public sealed class CategoryLocationKeywordService(
    ISqlConnectionFactory connectionFactory,
    IAdminSettingsService adminSettingsService,
    IHttpClientFactory httpClientFactory,
    IOptions<DataForSeoOptions> dataForSeoOptions) : ICategoryLocationKeywordService
{
    public async Task<LocationCategoryListViewModel> GetLocationCategoriesAsync(long locationId, CancellationToken ct)
    {
        if (locationId <= 0)
            throw new InvalidOperationException("Location is required.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var location = await conn.QuerySingleOrDefaultAsync<TownLocationRow>(new CommandDefinition(@"
SELECT TOP 1
  t.TownId AS LocationId,
  t.Name AS LocationName,
  c.Name AS CountyName
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
WHERE t.TownId = @LocationId;", new { LocationId = locationId }, cancellationToken: ct));
        if (location is null)
            throw new InvalidOperationException("Location was not found.");

        var rows = (await conn.QueryAsync<LocationCategoryRow>(new CommandDefinition(@"
SELECT
  c.CategoryId,
  c.DisplayName AS CategoryDisplayName,
  COUNT(k.Id) AS KeywordCount,
  SUM(CASE WHEN k.KeywordType = 1 THEN 1 ELSE 0 END) AS MainTermCount,
  MAX(k.UpdatedUtc) AS LastKeywordUpdatedUtc
FROM dbo.GoogleBusinessProfileCategory c
LEFT JOIN dbo.CategoryLocationKeyword k
  ON k.CategoryId = c.CategoryId
 AND k.LocationId = @LocationId
WHERE c.Status = N'Active'
  AND (
    EXISTS (
      SELECT 1
      FROM dbo.SearchRun r
      WHERE r.TownId = @LocationId
        AND r.CategoryId = c.CategoryId
    )
    OR EXISTS (
      SELECT 1
      FROM dbo.CategoryLocationKeyword k2
      WHERE k2.LocationId = @LocationId
        AND k2.CategoryId = c.CategoryId
    )
  )
GROUP BY c.CategoryId, c.DisplayName
ORDER BY c.DisplayName, c.CategoryId;", new { LocationId = locationId }, cancellationToken: ct))).ToList();

        return new LocationCategoryListViewModel
        {
            LocationId = location.LocationId,
            LocationName = location.LocationName,
            CountyName = location.CountyName,
            Rows = rows
        };
    }

    public async Task<IReadOnlyList<OtherLocationKeyphraseSourceSummary>> GetRecentCategoryLocationsWithKeyphrasesAsync(
        string categoryId,
        long excludeLocationId,
        int take,
        CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Category is required.");

        var clampedTake = Math.Clamp(take, 1, 50);
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = (await conn.QueryAsync<SourceLocationSummaryRow>(new CommandDefinition(@"
SELECT TOP (@Take)
  t.TownId AS LocationId,
  t.Name AS LocationName,
  county.Name AS CountyName,
  COUNT(k.Id) AS KeywordCount,
  MAX(COALESCE(k.UpdatedUtc, k.CreatedUtc)) AS LastUpdatedUtc
FROM dbo.CategoryLocationKeyword k
JOIN dbo.GbTown t ON t.TownId = k.LocationId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE k.CategoryId = @CategoryId
  AND (@ExcludeLocationId <= 0 OR k.LocationId <> @ExcludeLocationId)
GROUP BY t.TownId, t.Name, county.Name
ORDER BY
  MAX(COALESCE(k.UpdatedUtc, k.CreatedUtc)) DESC,
  COUNT(k.Id) DESC,
  t.Name ASC;", new
        {
            Take = clampedTake,
            CategoryId = normalizedCategoryId,
            ExcludeLocationId = excludeLocationId
        }, cancellationToken: ct))).ToList();

        return rows.Select(x => new OtherLocationKeyphraseSourceSummary
        {
            LocationId = x.LocationId,
            LocationName = x.LocationName,
            CountyName = x.CountyName,
            KeywordCount = x.KeywordCount,
            LastUpdatedUtc = x.LastUpdatedUtc
        }).ToList();
    }

    public async Task<CategoryKeyphrasesViewModel> GetKeyphrasesAsync(long locationId, string categoryId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Location and category are required.");

        var cooldownDays = await GetCooldownDaysAsync(ct);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var location = await conn.QuerySingleOrDefaultAsync<TownLocationRow>(new CommandDefinition(@"
SELECT TOP 1
  t.TownId AS LocationId,
  t.Name AS LocationName,
  c.Name AS CountyName
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
WHERE t.TownId = @LocationId;", new { LocationId = locationId }, cancellationToken: ct));
        if (location is null)
            throw new InvalidOperationException("Location was not found.");

        var category = await conn.QuerySingleOrDefaultAsync<CategoryLookupRow>(new CommandDefinition(@"
SELECT TOP 1
  CategoryId,
  DisplayName
FROM dbo.GoogleBusinessProfileCategory
WHERE CategoryId = @CategoryId;", new { CategoryId = normalizedCategoryId }, cancellationToken: ct));
        if (category is null)
            throw new InvalidOperationException("Category was not found.");

        var rows = (await conn.QueryAsync<KeyphraseRow>(new CommandDefinition(@"
SELECT
  Id,
  Keyword,
  KeywordType,
  CanonicalKeywordId,
  AvgSearchVolume,
  Cpc,
  Competition,
  CompetitionIndex,
  LowTopOfPageBid,
  HighTopOfPageBid,
  NoData,
  NoDataReason,
  LastAttemptedUtc,
  LastSucceededUtc,
  LastStatusCode,
  LastStatusMessage
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
ORDER BY
  CASE KeywordType WHEN 1 THEN 0 WHEN 2 THEN 1 WHEN 3 THEN 2 WHEN 4 THEN 3 ELSE 9 END,
  Keyword,
  Id;", new
        {
            CategoryId = normalizedCategoryId,
            LocationId = locationId
        }, cancellationToken: ct))).ToList();

        var monthlyByKeywordId = new Dictionary<int, List<SearchVolumePoint>>();
        if (rows.Count > 0)
        {
            var keywordIds = rows.Select(x => x.Id).Distinct().ToArray();
            var monthlyRows = (await conn.QueryAsync<SearchVolumeMonthlyRow>(new CommandDefinition(@"
SELECT
  CategoryLocationKeywordId,
  [Year],
  [Month],
  SearchVolume
FROM dbo.CategoryLocationSearchVolume
WHERE CategoryLocationKeywordId IN @KeywordIds
ORDER BY [Year] DESC, [Month] DESC;", new { KeywordIds = keywordIds }, cancellationToken: ct))).ToList();
            foreach (var row in monthlyRows)
            {
                if (!monthlyByKeywordId.TryGetValue(row.CategoryLocationKeywordId, out var list))
                {
                    list = [];
                    monthlyByKeywordId[row.CategoryLocationKeywordId] = list;
                }
                list.Add(new SearchVolumePoint(row.Year, row.Month, row.SearchVolume));
            }
        }

        var cutoffUtc = DateTime.UtcNow.AddDays(-cooldownDays);
        var keywordById = rows.ToDictionary(x => x.Id, x => x.Keyword);
        var mappedRows = rows.Select(row =>
        {
            var monthly = monthlyByKeywordId.TryGetValue(row.Id, out var points)
                ? points.OrderByDescending(x => x.Year).ThenByDescending(x => x.Month).Take(12).OrderBy(x => x.Year).ThenBy(x => x.Month).ToList()
                : [];
            DateTime? dataAsOfUtc = null;
            if (monthly.Count > 0)
            {
                var latest = monthly.OrderByDescending(x => x.Year).ThenByDescending(x => x.Month).First();
                dataAsOfUtc = new DateTime(latest.Year, latest.Month, 1, 0, 0, 0, DateTimeKind.Utc);
            }

            return new CategoryLocationKeywordListItem
            {
                Id = row.Id,
                Keyword = row.Keyword,
                KeywordType = row.KeywordType,
                CanonicalKeywordId = row.CanonicalKeywordId,
                SynonymOfKeyword = row.KeywordType == CategoryLocationKeywordTypes.Synonym
                    && row.CanonicalKeywordId.HasValue
                    && keywordById.TryGetValue(row.CanonicalKeywordId.Value, out var canonicalKeyword)
                    ? canonicalKeyword
                    : null,
                AvgSearchVolume = row.AvgSearchVolume,
                Cpc = row.Cpc,
                Competition = row.Competition,
                CompetitionIndex = row.CompetitionIndex,
                LowTopOfPageBid = row.LowTopOfPageBid,
                HighTopOfPageBid = row.HighTopOfPageBid,
                NoData = row.NoData,
                NoDataReason = row.NoDataReason,
                LastAttemptedUtc = row.LastAttemptedUtc,
                LastSucceededUtc = row.LastSucceededUtc,
                LastStatusCode = row.LastStatusCode,
                LastStatusMessage = row.LastStatusMessage,
                DataAsOfUtc = dataAsOfUtc,
                IsRefreshEligible = !row.LastAttemptedUtc.HasValue || row.LastAttemptedUtc.Value <= cutoffUtc,
                Last12Months = monthly
            };
        }).ToList();

        mappedRows = mappedRows
            .OrderBy(row => row.KeywordType == CategoryLocationKeywordTypes.MainTerm ? 0 : 1)
            .ThenBy(row => HasDataForOrdering(row) ? 0 : 1)
            .ThenByDescending(GetLatestMonthlyVolume)
            .ThenBy(row => row.Keyword, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var weightedByMonth = new Dictionary<(int Year, int Month), decimal>();
        foreach (var row in mappedRows)
        {
            if (row.NoData || row.KeywordType == CategoryLocationKeywordTypes.Synonym || row.Last12Months.Count == 0)
                continue;

            var weight = row.KeywordType switch
            {
                CategoryLocationKeywordTypes.MainTerm => 1m,
                CategoryLocationKeywordTypes.Modifier => 0.7m,
                CategoryLocationKeywordTypes.Adjacent => 0.7m,
                _ => 0m
            };
            if (weight <= 0m)
                continue;

            foreach (var point in row.Last12Months)
            {
                var key = (point.Year, point.Month);
                weightedByMonth[key] = weightedByMonth.TryGetValue(key, out var existing)
                    ? existing + (point.SearchVolume * weight)
                    : point.SearchVolume * weight;
            }
        }
        var weightedPoints = weightedByMonth
            .Select(x => new WeightedSearchVolumePoint(
                x.Key.Year,
                x.Key.Month,
                decimal.Round(x.Value, 2, MidpointRounding.AwayFromZero)))
            .OrderByDescending(x => x.Year)
            .ThenByDescending(x => x.Month)
            .Take(12)
            .OrderBy(x => x.Year)
            .ThenBy(x => x.Month)
            .ToList();

        var expectedMainKeyword = BuildExpectedMainKeyword(category.DisplayName, location.LocationName);

        return new CategoryKeyphrasesViewModel
        {
            LocationId = location.LocationId,
            LocationName = location.LocationName,
            CountyName = location.CountyName,
            CategoryId = category.CategoryId,
            CategoryDisplayName = category.DisplayName,
            ExpectedMainKeyword = expectedMainKeyword,
            SearchVolumeRefreshCooldownDays = cooldownDays,
            Rows = mappedRows,
            WeightedTotalLast12Months = weightedPoints
        };
    }

    public async Task<CategoryLocationKeywordRefreshSummary> AddKeywordAndRefreshAsync(long locationId, string categoryId, CategoryLocationKeywordCreateModel model, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        var normalizedKeyword = NormalizeKeyword(model.Keyword);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Location and category are required.");
        if (string.IsNullOrWhiteSpace(normalizedKeyword))
            throw new InvalidOperationException("Keyword is required.");
        if (model.KeywordType is not (CategoryLocationKeywordTypes.MainTerm or CategoryLocationKeywordTypes.Modifier or CategoryLocationKeywordTypes.Adjacent))
            throw new InvalidOperationException("Keyword type is invalid.");

        int insertedId;
        await using (var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        await using (var tx = await conn.BeginTransactionAsync(ct))
        {
            var context = await GetKeywordValidationContextAsync(conn, tx, locationId, normalizedCategoryId, ct);
            if (!KeywordContainsLocation(normalizedKeyword, context.LocationName))
                throw new InvalidOperationException($"Please include the location \"{context.LocationName}\" in the keyphrase.");

            var expectedMainKeyword = BuildExpectedMainKeyword(context.CategoryDisplayName, context.LocationName);
            if (model.KeywordType == CategoryLocationKeywordTypes.MainTerm && !IsCanonicalMainKeyword(normalizedKeyword, expectedMainKeyword))
                throw new InvalidOperationException($"Main Term is only allowed when the keyphrase exactly matches \"{expectedMainKeyword}\".");

            var duplicateCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
  AND LOWER(LTRIM(RTRIM(Keyword))) = LOWER(@Keyword);", new
            {
                CategoryId = normalizedCategoryId,
                LocationId = locationId,
                Keyword = normalizedKeyword
            }, tx, cancellationToken: ct));
            if (duplicateCount > 0)
                throw new InvalidOperationException("That keyword already exists for this category and location.");

            var nowUtc = DateTime.UtcNow;
            insertedId = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
INSERT INTO dbo.CategoryLocationKeyword(
  CategoryId,
  LocationId,
  Keyword,
  KeywordType,
  CanonicalKeywordId,
  CreatedUtc,
  UpdatedUtc
)
OUTPUT INSERTED.Id
VALUES(
  @CategoryId,
  @LocationId,
  @Keyword,
  @KeywordType,
  NULL,
  @NowUtc,
  @NowUtc
);", new
            {
                CategoryId = normalizedCategoryId,
                LocationId = locationId,
                Keyword = normalizedKeyword,
                KeywordType = model.KeywordType,
                NowUtc = nowUtc
            }, tx, cancellationToken: ct));

            if (model.KeywordType == CategoryLocationKeywordTypes.MainTerm)
            {
                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @ModifierType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
  AND Id <> @MainId
  AND KeywordType = @MainTermType;

UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @MainTermType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE Id = @MainId;", new
                {
                    CategoryId = normalizedCategoryId,
                    LocationId = locationId,
                    MainId = insertedId,
                    MainTermType = CategoryLocationKeywordTypes.MainTerm,
                    ModifierType = CategoryLocationKeywordTypes.Modifier,
                    NowUtc = nowUtc
                }, tx, cancellationToken: ct));
            }

            await RecomputeKeywordTypesAsync(conn, tx, normalizedCategoryId, locationId, nowUtc, ct);
            await tx.CommitAsync(ct);
        }

        return await RefreshKeywordsCoreAsync(locationId, normalizedCategoryId, [insertedId], enforceEligibility: false, ct);
    }

    public Task<CategoryLocationKeywordRefreshSummary> RefreshKeywordAsync(long locationId, string categoryId, int keywordId, CancellationToken ct)
        => RefreshKeywordsCoreAsync(locationId, NormalizeCategoryId(categoryId), [keywordId], enforceEligibility: true, ct);

    public async Task<CategoryLocationKeywordRefreshSummary> RefreshEligibleKeywordsAsync(long locationId, string categoryId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Location and category are required.");

        var cooldownDays = await GetCooldownDaysAsync(ct);
        var cutoffUtc = DateTime.UtcNow.AddDays(-cooldownDays);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var eligibleIds = (await conn.QueryAsync<int>(new CommandDefinition(@"
SELECT Id
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
  AND (LastAttemptedUtc IS NULL OR LastAttemptedUtc <= @CutoffUtc);", new
        {
            CategoryId = normalizedCategoryId,
            LocationId = locationId,
            CutoffUtc = cutoffUtc
        }, cancellationToken: ct))).ToList();

        if (eligibleIds.Count == 0)
            return new CategoryLocationKeywordRefreshSummary(0, 0, 0, 0);

        return await RefreshKeywordsCoreAsync(locationId, normalizedCategoryId, eligibleIds, enforceEligibility: false, ct);
    }

    public async Task<bool> SetMainTermAsync(long locationId, string categoryId, int keywordId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId) || keywordId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var exists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.CategoryLocationKeyword
WHERE Id = @Id
  AND CategoryId = @CategoryId
  AND LocationId = @LocationId;", new
        {
            Id = keywordId,
            CategoryId = normalizedCategoryId,
            LocationId = locationId
        }, tx, cancellationToken: ct));
        if (exists == 0)
            return false;

        var context = await GetKeywordValidationContextAsync(conn, tx, locationId, normalizedCategoryId, ct);
        var keyword = await conn.ExecuteScalarAsync<string?>(new CommandDefinition(@"
SELECT TOP 1 Keyword
FROM dbo.CategoryLocationKeyword
WHERE Id = @Id
  AND CategoryId = @CategoryId
  AND LocationId = @LocationId;", new
        {
            Id = keywordId,
            CategoryId = normalizedCategoryId,
            LocationId = locationId
        }, tx, cancellationToken: ct));
        var expectedMainKeyword = BuildExpectedMainKeyword(context.CategoryDisplayName, context.LocationName);
        if (!IsCanonicalMainKeyword(keyword, expectedMainKeyword))
            throw new InvalidOperationException($"Only \"{expectedMainKeyword}\" can be set as Main Term for this category and location.");

        var nowUtc = DateTime.UtcNow;
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @ModifierType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
  AND Id <> @MainId
  AND KeywordType = @MainTermType;

UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @MainTermType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE Id = @MainId;", new
        {
            CategoryId = normalizedCategoryId,
            LocationId = locationId,
            MainId = keywordId,
            MainTermType = CategoryLocationKeywordTypes.MainTerm,
            ModifierType = CategoryLocationKeywordTypes.Modifier,
            NowUtc = nowUtc
        }, tx, cancellationToken: ct));

        await RecomputeKeywordTypesAsync(conn, tx, normalizedCategoryId, locationId, nowUtc, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<bool> SetKeywordTypeAsync(long locationId, string categoryId, int keywordId, int keywordType, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId) || keywordId <= 0)
            return false;
        if (keywordType is not (CategoryLocationKeywordTypes.Modifier or CategoryLocationKeywordTypes.Adjacent))
            throw new InvalidOperationException("Only Modifier or Adjacent can be manually set.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<KeyphraseTypeCheckRow>(new CommandDefinition(@"
SELECT TOP 1 Id, KeywordType
FROM dbo.CategoryLocationKeyword
WHERE Id = @Id
  AND CategoryId = @CategoryId
  AND LocationId = @LocationId;", new
        {
            Id = keywordId,
            CategoryId = normalizedCategoryId,
            LocationId = locationId
        }, tx, cancellationToken: ct));
        if (row is null)
            return false;
        if (row.KeywordType == CategoryLocationKeywordTypes.MainTerm)
            throw new InvalidOperationException("Main term cannot be changed directly. Set another keyword as Main Term first.");

        var nowUtc = DateTime.UtcNow;
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @KeywordType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;", new
        {
            Id = keywordId,
            KeywordType = keywordType,
            NowUtc = nowUtc
        }, tx, cancellationToken: ct));

        await RecomputeKeywordTypesAsync(conn, tx, normalizedCategoryId, locationId, nowUtc, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<bool> DeleteKeywordAsync(long locationId, string categoryId, int keywordId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId) || keywordId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var affected = await conn.ExecuteAsync(new CommandDefinition(@"
DELETE FROM dbo.CategoryLocationKeyword
WHERE Id = @Id
  AND CategoryId = @CategoryId
  AND LocationId = @LocationId;", new
        {
            Id = keywordId,
            CategoryId = normalizedCategoryId,
            LocationId = locationId
        }, tx, cancellationToken: ct));
        if (affected == 0)
            return false;

        var nowUtc = DateTime.UtcNow;
        await RecomputeKeywordTypesAsync(conn, tx, normalizedCategoryId, locationId, nowUtc, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    private async Task<CategoryLocationKeywordRefreshSummary> RefreshKeywordsCoreAsync(
        long locationId,
        string categoryId,
        IReadOnlyCollection<int> keywordIds,
        bool enforceEligibility,
        CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (locationId <= 0 || string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Location and category are required.");

        var normalizedIds = keywordIds.Where(x => x > 0).Distinct().ToArray();
        if (normalizedIds.Length == 0)
            return new CategoryLocationKeywordRefreshSummary(0, 0, 0, 0);

        var cooldownDays = await GetCooldownDaysAsync(ct);
        var cutoffUtc = DateTime.UtcNow.AddDays(-cooldownDays);

        List<KeywordRefreshRow> selectedRows;
        await using (var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        {
            selectedRows = (await conn.QueryAsync<KeywordRefreshRow>(new CommandDefinition(@"
SELECT
  Id,
  Keyword,
  KeywordType,
  Fingerprint,
  NoData,
  LastAttemptedUtc
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
  AND Id IN @Ids;", new
            {
                CategoryId = normalizedCategoryId,
                LocationId = locationId,
                Ids = normalizedIds
            }, cancellationToken: ct))).ToList();
        }

        if (selectedRows.Count == 0)
            return new CategoryLocationKeywordRefreshSummary(0, 0, 0, 0);

        var eligibleRows = selectedRows
            .Where(x => !x.LastAttemptedUtc.HasValue || x.LastAttemptedUtc.Value <= cutoffUtc)
            .ToList();
        if (enforceEligibility && eligibleRows.Count != selectedRows.Count)
            throw new InvalidOperationException($"One or more keywords are still in cooldown ({cooldownDays} days).");

        if (eligibleRows.Count == 0)
            return new CategoryLocationKeywordRefreshSummary(selectedRows.Count, 0, selectedRows.Count, 0);

        SearchVolumeBatchResult batch;
        try
        {
            batch = await FetchSearchVolumeBatchAsync(eligibleRows.Select(x => x.Keyword).ToList(), ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            batch = SearchVolumeBatchResult.FromFailure(0, ex.Message);
        }

        var nowUtc = DateTime.UtcNow;
        var refreshed = 0;
        var errors = 0;

        await using (var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct))
        await using (var tx = await conn.BeginTransactionAsync(ct))
        {
            foreach (var keywordRow in eligibleRows)
            {
                if (!batch.Success)
                {
                    await MarkKeywordApiErrorAsync(conn, tx, keywordRow.Id, nowUtc, batch.StatusCode, batch.StatusMessage, ct);
                    errors++;
                    continue;
                }

                var normalizedKeyword = NormalizeKeywordKey(keywordRow.Keyword);
                if (!batch.ResultsByKeyword.TryGetValue(normalizedKeyword, out var result))
                {
                    await MarkKeywordApiErrorAsync(conn, tx, keywordRow.Id, nowUtc, batch.StatusCode, "No search volume result was returned for this keyword.", ct, "Unknown");
                    errors++;
                    continue;
                }

                if (!result.Success)
                {
                    await MarkKeywordApiErrorAsync(conn, tx, keywordRow.Id, nowUtc, result.StatusCode ?? batch.StatusCode, result.StatusMessage ?? batch.StatusMessage, ct);
                    errors++;
                    continue;
                }

                if (!result.SearchVolume.HasValue)
                {
                    await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  AvgSearchVolume = NULL,
  Cpc = NULL,
  Competition = NULL,
  CompetitionIndex = NULL,
  LowTopOfPageBid = NULL,
  HighTopOfPageBid = NULL,
  Fingerprint = NULL,
  NoData = 1,
  NoDataReason = N'BelowThreshold',
  LastAttemptedUtc = @NowUtc,
  LastStatusCode = @StatusCode,
  LastStatusMessage = @StatusMessage,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;
DELETE FROM dbo.CategoryLocationSearchVolume
WHERE CategoryLocationKeywordId = @Id;", new
                    {
                        Id = keywordRow.Id,
                        NowUtc = nowUtc,
                        StatusCode = result.StatusCode ?? batch.StatusCode,
                        StatusMessage = Truncate(result.StatusMessage ?? batch.StatusMessage, 255)
                    }, tx, cancellationToken: ct));
                    refreshed++;
                    continue;
                }

                var orderedMonthly = result.MonthlySearches
                    .OrderBy(x => x.Year)
                    .ThenBy(x => x.Month)
                    .ToList();
                var searchVolumeOptions = dataForSeoOptions.Value;
                var fingerprint = BuildFingerprint(
                    ResolveFingerprintLocationKey(searchVolumeOptions, result),
                    ResolveFingerprintLanguageKey(searchVolumeOptions, result),
                    result.SearchPartners ?? searchVolumeOptions.SearchVolumeSearchPartners,
                    orderedMonthly);

                await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  AvgSearchVolume = @AvgSearchVolume,
  Cpc = @Cpc,
  Competition = @Competition,
  CompetitionIndex = @CompetitionIndex,
  LowTopOfPageBid = @LowTopOfPageBid,
  HighTopOfPageBid = @HighTopOfPageBid,
  Fingerprint = @Fingerprint,
  NoData = 0,
  NoDataReason = NULL,
  LastAttemptedUtc = @NowUtc,
  LastSucceededUtc = @NowUtc,
  LastStatusCode = @StatusCode,
  LastStatusMessage = @StatusMessage,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;
DELETE FROM dbo.CategoryLocationSearchVolume
WHERE CategoryLocationKeywordId = @Id;", new
                {
                    Id = keywordRow.Id,
                    AvgSearchVolume = result.SearchVolume,
                    result.Cpc,
                    result.Competition,
                    result.CompetitionIndex,
                    result.LowTopOfPageBid,
                    result.HighTopOfPageBid,
                    Fingerprint = fingerprint,
                    NowUtc = nowUtc,
                    StatusCode = result.StatusCode ?? batch.StatusCode,
                    StatusMessage = Truncate(result.StatusMessage ?? batch.StatusMessage, 255)
                }, tx, cancellationToken: ct));

                foreach (var month in orderedMonthly)
                {
                    await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.CategoryLocationSearchVolume(CategoryLocationKeywordId, [Year], [Month], SearchVolume)
VALUES(@CategoryLocationKeywordId, @Year, @Month, @SearchVolume);", new
                    {
                        CategoryLocationKeywordId = keywordRow.Id,
                        month.Year,
                        month.Month,
                        month.SearchVolume
                    }, tx, cancellationToken: ct));
                }

                refreshed++;
            }

            await RecomputeKeywordTypesAsync(conn, tx, normalizedCategoryId, locationId, nowUtc, ct);
            await tx.CommitAsync(ct);
        }

        var skipped = selectedRows.Count - eligibleRows.Count;
        return new CategoryLocationKeywordRefreshSummary(selectedRows.Count, refreshed, skipped, errors);
    }

    private async Task RecomputeKeywordTypesAsync(SqlConnection conn, IDbTransaction tx, string categoryId, long locationId, DateTime nowUtc, CancellationToken ct)
    {
        var rows = (await conn.QueryAsync<KeywordClassificationRow>(new CommandDefinition(@"
SELECT
  Id,
  KeywordType,
  CanonicalKeywordId,
  Fingerprint,
  NoData
FROM dbo.CategoryLocationKeyword
WHERE CategoryId = @CategoryId
  AND LocationId = @LocationId
ORDER BY Id;", new
        {
            CategoryId = categoryId,
            LocationId = locationId
        }, tx, cancellationToken: ct))).ToList();
        if (rows.Count == 0)
            return;

        var mainId = rows.FirstOrDefault(x => x.KeywordType == CategoryLocationKeywordTypes.MainTerm)?.Id;
        if (mainId.HasValue)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @MainTermType,
  CanonicalKeywordId = NULL,
  UpdatedUtc = @NowUtc
WHERE Id = @MainId
  AND (KeywordType <> @MainTermType OR CanonicalKeywordId IS NOT NULL);", new
            {
                MainId = mainId.Value,
                MainTermType = CategoryLocationKeywordTypes.MainTerm,
                NowUtc = nowUtc
            }, tx, cancellationToken: ct));
        }

        var fingerprintPlans = rows
            .Where(x => !x.NoData && !string.IsNullOrWhiteSpace(x.Fingerprint))
            .GroupBy(x => x.Fingerprint!, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var members = group.ToList();
                var representativeId = SelectFingerprintRepresentativeId(members, mainId);
                return new FingerprintPlan(group.Key, representativeId, members.Count);
            })
            .ToDictionary(x => x.Fingerprint, StringComparer.OrdinalIgnoreCase);

        foreach (var row in rows)
        {
            int desiredType;
            int? desiredCanonicalId;

            if (mainId.HasValue && row.Id == mainId.Value)
            {
                desiredType = CategoryLocationKeywordTypes.MainTerm;
                desiredCanonicalId = null;
            }
            else if (!row.NoData
                && !string.IsNullOrWhiteSpace(row.Fingerprint)
                && fingerprintPlans.TryGetValue(row.Fingerprint, out var plan)
                && plan.MemberCount > 1
                && row.Id != plan.RepresentativeId)
            {
                desiredType = CategoryLocationKeywordTypes.Synonym;
                desiredCanonicalId = plan.RepresentativeId;
            }
            else if (row.KeywordType == CategoryLocationKeywordTypes.Adjacent)
            {
                desiredType = CategoryLocationKeywordTypes.Adjacent;
                desiredCanonicalId = null;
            }
            else
            {
                desiredType = CategoryLocationKeywordTypes.Modifier;
                desiredCanonicalId = null;
            }

            if (row.KeywordType == desiredType && row.CanonicalKeywordId == desiredCanonicalId)
                continue;

            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  KeywordType = @KeywordType,
  CanonicalKeywordId = @CanonicalKeywordId,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;", new
            {
                Id = row.Id,
                KeywordType = desiredType,
                CanonicalKeywordId = desiredCanonicalId,
                NowUtc = nowUtc
            }, tx, cancellationToken: ct));
        }
    }

    private async Task MarkKeywordApiErrorAsync(
        SqlConnection conn,
        IDbTransaction tx,
        int keywordId,
        DateTime nowUtc,
        int? statusCode,
        string? statusMessage,
        CancellationToken ct,
        string reason = "ApiError")
    {
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.CategoryLocationKeyword
SET
  NoData = 1,
  NoDataReason = @NoDataReason,
  LastAttemptedUtc = @NowUtc,
  LastStatusCode = @StatusCode,
  LastStatusMessage = @StatusMessage,
  UpdatedUtc = @NowUtc
WHERE Id = @Id;", new
        {
            Id = keywordId,
            NoDataReason = reason,
            NowUtc = nowUtc,
            StatusCode = statusCode,
            StatusMessage = Truncate(statusMessage, 255)
        }, tx, cancellationToken: ct));
    }

    private async Task<SearchVolumeBatchResult> FetchSearchVolumeBatchAsync(IReadOnlyList<string> keywords, CancellationToken ct)
    {
        var cfg = dataForSeoOptions.Value;
        var login = (cfg.Login ?? string.Empty).Trim();
        var password = (cfg.Password ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(login) || string.IsNullOrWhiteSpace(password))
            throw new InvalidOperationException("DataForSEO credentials are not configured.");

        var normalizedKeywords = keywords
            .Select(NormalizeKeyword)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (normalizedKeywords.Count == 0)
            return SearchVolumeBatchResult.FromSuccess(20000, "No keywords provided.", new Dictionary<string, SearchVolumeKeywordResult>(StringComparer.OrdinalIgnoreCase));

        var taskPayload = new Dictionary<string, object?>
        {
            ["keywords"] = normalizedKeywords,
            ["search_partners"] = cfg.SearchVolumeSearchPartners
        };
        if (!string.IsNullOrWhiteSpace(cfg.SearchVolumeLocationName))
            taskPayload["location_name"] = cfg.SearchVolumeLocationName.Trim();
        else
            taskPayload["location_code"] = cfg.SearchVolumeLocationCode;

        if (!string.IsNullOrWhiteSpace(cfg.SearchVolumeLanguageName))
            taskPayload["language_name"] = cfg.SearchVolumeLanguageName.Trim();
        else
            taskPayload["language_code"] = cfg.SearchVolumeLanguageCode;

        var payload = JsonSerializer.Serialize(new[] { taskPayload });

        var baseUrl = (cfg.BaseUrl ?? string.Empty).TrimEnd('/');
        if (string.IsNullOrWhiteSpace(baseUrl))
            throw new InvalidOperationException("DataForSEO base URL is missing.");
        var path = (cfg.SearchVolumePath ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException("DataForSEO search volume path is missing.");
        var url = $"{baseUrl}{(path.StartsWith('/') ? path : "/" + path)}";

        var auth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{login}:{password}"));
        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", auth);

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, ct);
        var responseBody = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            return SearchVolumeBatchResult.FromFailure((int)response.StatusCode, $"HTTP {(int)response.StatusCode}: {Truncate(responseBody, 220)}");

        using var doc = JsonDocument.Parse(responseBody);
        var root = doc.RootElement;
        var rootStatusCode = TryGetInt(root, "status_code");
        var rootStatusMessage = TryGetString(root, "status_message") ?? "Unknown response status.";
        if (rootStatusCode is not (>= 20000 and < 30000))
            return SearchVolumeBatchResult.FromFailure(rootStatusCode, rootStatusMessage);

        if (!root.TryGetProperty("tasks", out var tasksNode) || tasksNode.ValueKind != JsonValueKind.Array || tasksNode.GetArrayLength() == 0)
            return SearchVolumeBatchResult.FromFailure(rootStatusCode, "DataForSEO response had no tasks.");

        var resultsByKeyword = new Dictionary<string, SearchVolumeKeywordResult>(StringComparer.OrdinalIgnoreCase);
        foreach (var taskNode in tasksNode.EnumerateArray())
        {
            var taskStatusCode = TryGetInt(taskNode, "status_code");
            var taskStatusMessage = TryGetString(taskNode, "status_message") ?? rootStatusMessage;
            if (taskStatusCode is not (>= 20000 and < 30000))
            {
                foreach (var keyword in normalizedKeywords)
                {
                    var key = NormalizeKeywordKey(keyword);
                    if (!resultsByKeyword.ContainsKey(key))
                        resultsByKeyword[key] = SearchVolumeKeywordResult.Error(taskStatusCode, taskStatusMessage);
                }
                continue;
            }

            if (!taskNode.TryGetProperty("result", out var resultNode) || resultNode.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var keywordNode in resultNode.EnumerateArray())
            {
                var keyword = NormalizeKeyword(TryGetString(keywordNode, "keyword"));
                if (string.IsNullOrWhiteSpace(keyword))
                    continue;

                var monthly = new List<SearchVolumePoint>();
                if (keywordNode.TryGetProperty("monthly_searches", out var monthlyNode) && monthlyNode.ValueKind == JsonValueKind.Array)
                {
                    foreach (var monthNode in monthlyNode.EnumerateArray())
                    {
                        var year = TryGetInt(monthNode, "year");
                        var month = TryGetInt(monthNode, "month");
                        var volume = TryGetInt(monthNode, "search_volume");
                        if (!year.HasValue || !month.HasValue || !volume.HasValue || month.Value is < 1 or > 12)
                            continue;
                        monthly.Add(new SearchVolumePoint(year.Value, month.Value, volume.Value));
                    }
                }

                var result = SearchVolumeKeywordResult.Ok(
                    TryGetInt(keywordNode, "search_volume"),
                    TryGetDecimal(keywordNode, "cpc"),
                    TryGetString(keywordNode, "competition"),
                    TryGetInt(keywordNode, "competition_index"),
                    TryGetDecimal(keywordNode, "low_top_of_page_bid"),
                    TryGetDecimal(keywordNode, "high_top_of_page_bid"),
                    monthly,
                    TryGetStringOrNumber(keywordNode, "location_code"),
                    TryGetStringOrNumber(keywordNode, "language_code"),
                    TryGetBool(keywordNode, "search_partners"),
                    taskStatusCode,
                    taskStatusMessage);
                resultsByKeyword[NormalizeKeywordKey(keyword)] = result;
            }
        }

        return SearchVolumeBatchResult.FromSuccess(rootStatusCode, rootStatusMessage, resultsByKeyword);
    }

    private static string BuildFingerprint(string locationCode, string languageCode, bool searchPartners, IReadOnlyList<SearchVolumePoint> monthlyPoints)
    {
        var builder = new StringBuilder();
        builder.Append(NormalizeFingerprintKey(locationCode));
        builder.Append('|');
        builder.Append(NormalizeFingerprintKey(languageCode));
        builder.Append('|');
        builder.Append(searchPartners ? "1" : "0");
        foreach (var point in monthlyPoints.OrderBy(x => x.Year).ThenBy(x => x.Month))
        {
            builder.Append('|');
            builder.Append(point.Year);
            builder.Append('-');
            builder.Append(point.Month.ToString("00"));
            builder.Append(':');
            builder.Append(point.SearchVolume);
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static string ResolveFingerprintLocationKey(DataForSeoOptions cfg, SearchVolumeKeywordResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.LocationCode))
            return result.LocationCode;
        if (!string.IsNullOrWhiteSpace(cfg.SearchVolumeLocationName))
            return cfg.SearchVolumeLocationName;
        return cfg.SearchVolumeLocationCode.ToString(CultureInfo.InvariantCulture);
    }

    private static string ResolveFingerprintLanguageKey(DataForSeoOptions cfg, SearchVolumeKeywordResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.LanguageCode))
            return result.LanguageCode;
        if (!string.IsNullOrWhiteSpace(cfg.SearchVolumeLanguageName))
            return cfg.SearchVolumeLanguageName;
        return cfg.SearchVolumeLanguageCode.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizeFingerprintKey(string? value) => (value ?? string.Empty).Trim().ToLowerInvariant();

    private async Task EnsureLocationAndCategoryExistAsync(SqlConnection conn, IDbTransaction tx, long locationId, string categoryId, CancellationToken ct)
    {
        var locationExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GbTown
WHERE TownId = @LocationId;", new { LocationId = locationId }, tx, cancellationToken: ct));
        if (locationExists == 0)
            throw new InvalidOperationException("Location was not found.");

        var categoryExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GoogleBusinessProfileCategory
WHERE CategoryId = @CategoryId;", new { CategoryId = categoryId }, tx, cancellationToken: ct));
        if (categoryExists == 0)
            throw new InvalidOperationException("Category was not found.");
    }

    private async Task<KeywordValidationContext> GetKeywordValidationContextAsync(SqlConnection conn, IDbTransaction tx, long locationId, string categoryId, CancellationToken ct)
    {
        var row = await conn.QuerySingleOrDefaultAsync<KeywordValidationContext>(new CommandDefinition(@"
SELECT TOP 1
  t.Name AS LocationName,
  c.DisplayName AS CategoryDisplayName
FROM dbo.GbTown t
CROSS JOIN dbo.GoogleBusinessProfileCategory c
WHERE t.TownId = @LocationId
  AND c.CategoryId = @CategoryId;", new
        {
            LocationId = locationId,
            CategoryId = categoryId
        }, tx, cancellationToken: ct));
        if (row is null)
            throw new InvalidOperationException("Location or category was not found.");
        return row;
    }

    private async Task<int> GetCooldownDaysAsync(CancellationToken ct)
    {
        var settings = await adminSettingsService.GetAsync(ct);
        return Math.Clamp(settings.SearchVolumeRefreshCooldownDays, 0, 3650);
    }

    private static string NormalizeCategoryId(string? categoryId) => (categoryId ?? string.Empty).Trim();

    private static string NormalizeKeyword(string? keyword) => (keyword ?? string.Empty).Trim();

    private static string NormalizeKeywordKey(string? keyword) => NormalizeKeyword(keyword).ToLowerInvariant();

    private static string BuildExpectedMainKeyword(string categoryDisplayName, string locationName)
    {
        var category = CompactSpaces(categoryDisplayName);
        var location = CompactSpaces(locationName);
        return $"{category} {location}".Trim();
    }

    private static bool IsCanonicalMainKeyword(string? keyword, string expectedMainKeyword)
        => string.Equals(NormalizeForKeywordComparison(keyword), NormalizeForKeywordComparison(expectedMainKeyword), StringComparison.Ordinal);

    private static bool KeywordContainsLocation(string? keyword, string locationName)
    {
        var normalizedKeyword = NormalizeForKeywordComparison(keyword);
        var normalizedLocation = NormalizeForKeywordComparison(locationName);
        if (string.IsNullOrWhiteSpace(normalizedKeyword) || string.IsNullOrWhiteSpace(normalizedLocation))
            return false;
        return $" {normalizedKeyword} ".Contains($" {normalizedLocation} ", StringComparison.Ordinal);
    }

    private static string NormalizeForKeywordComparison(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value)
            builder.Append(char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : ' ');
        return CompactSpaces(builder.ToString());
    }

    private static string CompactSpaces(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        return string.Join(' ', value.Split(' ', StringSplitOptions.RemoveEmptyEntries));
    }

    private static bool HasDataForOrdering(CategoryLocationKeywordListItem row)
        => !row.NoData && row.Last12Months.Count > 0;

    private static int GetLatestMonthlyVolume(CategoryLocationKeywordListItem row)
    {
        if (row.Last12Months.Count == 0)
            return 0;
        var latest = row.Last12Months
            .OrderByDescending(x => x.Year)
            .ThenByDescending(x => x.Month)
            .First();
        return latest.SearchVolume;
    }

    private static int SelectFingerprintRepresentativeId(IReadOnlyList<KeywordClassificationRow> members, int? mainId)
    {
        if (mainId.HasValue && members.Any(x => x.Id == mainId.Value))
            return mainId.Value;

        return members
            .OrderBy(x => x.KeywordType switch
            {
                CategoryLocationKeywordTypes.Modifier => 0,
                CategoryLocationKeywordTypes.Adjacent => 1,
                CategoryLocationKeywordTypes.MainTerm => 2,
                CategoryLocationKeywordTypes.Synonym => 3,
                _ => 9
            })
            .ThenBy(x => x.Id)
            .First()
            .Id;
    }

    private static int? TryGetInt(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node))
            return null;
        return node.ValueKind switch
        {
            JsonValueKind.Number when node.TryGetInt32(out var value) => value,
            JsonValueKind.String when int.TryParse(node.GetString(), out var value) => value,
            _ => null
        };
    }

    private static decimal? TryGetDecimal(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node))
            return null;
        return node.ValueKind switch
        {
            JsonValueKind.Number when node.TryGetDecimal(out var value) => value,
            JsonValueKind.String when decimal.TryParse(node.GetString(), out var value) => value,
            _ => null
        };
    }

    private static string? TryGetString(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node) || node.ValueKind != JsonValueKind.String)
            return null;
        return node.GetString();
    }

    private static string? TryGetStringOrNumber(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node))
            return null;
        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Number => node.GetRawText(),
            _ => null
        };
    }

    private static bool? TryGetBool(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node))
            return null;
        return node.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(node.GetString(), out var value) => value,
            _ => null
        };
    }

    private static string Truncate(string? value, int maxLength)
    {
        var text = (value ?? string.Empty).Trim();
        if (text.Length <= maxLength)
            return text;
        return text[..maxLength];
    }

    private sealed record TownLocationRow(long LocationId, string LocationName, string CountyName);
    private sealed record CategoryLookupRow(string CategoryId, string DisplayName);
    private sealed record KeyphraseRow(
        int Id,
        string Keyword,
        int KeywordType,
        int? CanonicalKeywordId,
        int? AvgSearchVolume,
        decimal? Cpc,
        string? Competition,
        int? CompetitionIndex,
        decimal? LowTopOfPageBid,
        decimal? HighTopOfPageBid,
        bool NoData,
        string? NoDataReason,
        DateTime? LastAttemptedUtc,
        DateTime? LastSucceededUtc,
        int? LastStatusCode,
        string? LastStatusMessage);
    private sealed record SearchVolumeMonthlyRow(int CategoryLocationKeywordId, int Year, int Month, int SearchVolume);
    private sealed record SourceLocationSummaryRow(long LocationId, string LocationName, string CountyName, int KeywordCount, DateTime? LastUpdatedUtc);
    private sealed record KeyphraseTypeCheckRow(int Id, int KeywordType);
    private sealed record KeywordRefreshRow(int Id, string Keyword, int KeywordType, string? Fingerprint, bool NoData, DateTime? LastAttemptedUtc);
    private sealed record KeywordClassificationRow(int Id, int KeywordType, int? CanonicalKeywordId, string? Fingerprint, bool NoData);
    private sealed record FingerprintPlan(string Fingerprint, int RepresentativeId, int MemberCount);
    private sealed record KeywordValidationContext(string LocationName, string CategoryDisplayName);

    private sealed record SearchVolumeBatchResult(
        bool Success,
        int? StatusCode,
        string? StatusMessage,
        IReadOnlyDictionary<string, SearchVolumeKeywordResult> ResultsByKeyword)
    {
        public static SearchVolumeBatchResult FromSuccess(int? statusCode, string? statusMessage, IReadOnlyDictionary<string, SearchVolumeKeywordResult> results)
            => new(true, statusCode, statusMessage, results);

        public static SearchVolumeBatchResult FromFailure(int? statusCode, string? statusMessage)
            => new(false, statusCode, statusMessage, new Dictionary<string, SearchVolumeKeywordResult>(StringComparer.OrdinalIgnoreCase));
    }

    private sealed record SearchVolumeKeywordResult(
        bool Success,
        int? SearchVolume,
        decimal? Cpc,
        string? Competition,
        int? CompetitionIndex,
        decimal? LowTopOfPageBid,
        decimal? HighTopOfPageBid,
        IReadOnlyList<SearchVolumePoint> MonthlySearches,
        string? LocationCode,
        string? LanguageCode,
        bool? SearchPartners,
        int? StatusCode,
        string? StatusMessage)
    {
        public static SearchVolumeKeywordResult Ok(
            int? searchVolume,
            decimal? cpc,
            string? competition,
            int? competitionIndex,
            decimal? lowTopOfPageBid,
            decimal? highTopOfPageBid,
            IReadOnlyList<SearchVolumePoint> monthlySearches,
            string? locationCode,
            string? languageCode,
            bool? searchPartners,
            int? statusCode,
            string? statusMessage)
            => new(true, searchVolume, cpc, competition, competitionIndex, lowTopOfPageBid, highTopOfPageBid, monthlySearches, locationCode, languageCode, searchPartners, statusCode, statusMessage);

        public static SearchVolumeKeywordResult Error(int? statusCode, string? statusMessage)
            => new(false, null, null, null, null, null, null, [], null, null, null, statusCode, statusMessage);
    }
}
