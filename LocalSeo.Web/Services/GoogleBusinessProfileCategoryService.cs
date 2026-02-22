using System.Data;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IGoogleBusinessProfileCategoryService
{
    Task<GoogleBusinessProfileCategoryListResult> GetPagedAsync(string? statusFilter, string? search, int page, int pageSize, CancellationToken ct);
    Task<IReadOnlyList<GoogleBusinessProfileCategoryLookupItem>> GetActiveLookupAsync(string regionCode, string languageCode, CancellationToken ct);
    Task<GoogleBusinessProfileCategorySyncSummary?> GetLatestSyncSummaryAsync(string regionCode, string languageCode, CancellationToken ct);
    Task<GoogleBusinessProfileCategoryEditModel?> GetByIdAsync(string categoryId, CancellationToken ct);
    Task AddManualAsync(GoogleBusinessProfileCategoryCreateModel model, CancellationToken ct);
    Task<bool> UpdateDisplayNameAsync(string categoryId, string displayName, CancellationToken ct);
    Task<bool> MarkInactiveAsync(string categoryId, CancellationToken ct);
    Task<GoogleBusinessProfileCategorySyncRunResult> SyncFromGoogleAsync(string regionCode, string languageCode, CancellationToken ct);
}

public sealed class GoogleBusinessProfileCategoryService(
    ISqlConnectionFactory connectionFactory,
    IHttpClientFactory httpClientFactory,
    IGoogleBusinessProfileOAuthService googleBusinessProfileOAuthService,
    IOptions<GoogleOptions> googleOptions,
    ILogger<GoogleBusinessProfileCategoryService> logger) : IGoogleBusinessProfileCategoryService
{
    private const string StatusActive = "Active";
    private const string StatusInactive = "Inactive";

    public async Task<GoogleBusinessProfileCategoryListResult> GetPagedAsync(string? statusFilter, string? search, int page, int pageSize, CancellationToken ct)
    {
        var normalizedFilter = NormalizeStatusFilter(statusFilter);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);
        var normalizedPage = Math.Max(1, page);
        var normalizedSearch = (search ?? string.Empty).Trim();

        var whereParts = new List<string>();
        if (normalizedFilter is not null)
            whereParts.Add("c.Status = @Status");
        if (!string.IsNullOrWhiteSpace(normalizedSearch))
            whereParts.Add("(c.DisplayName LIKE @SearchPattern OR c.CategoryId LIKE @SearchPattern)");

        var whereSql = whereParts.Count == 0 ? string.Empty : "WHERE " + string.Join(" AND ", whereParts);
        var parameters = new
        {
            Status = normalizedFilter,
            SearchPattern = $"%{normalizedSearch}%",
            Offset = (normalizedPage - 1) * normalizedPageSize,
            PageSize = normalizedPageSize
        };

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition($@"
SELECT COUNT(1)
FROM dbo.GoogleBusinessProfileCategory c
{whereSql};", parameters, cancellationToken: ct));

        var totalPages = Math.Max(1, (int)Math.Ceiling(totalCount / (double)normalizedPageSize));
        if (normalizedPage > totalPages)
        {
            normalizedPage = totalPages;
            parameters = new
            {
                Status = normalizedFilter,
                SearchPattern = $"%{normalizedSearch}%",
                Offset = (normalizedPage - 1) * normalizedPageSize,
                PageSize = normalizedPageSize
            };
        }

        var rows = (await conn.QueryAsync<GoogleBusinessProfileCategoryRow>(new CommandDefinition($@"
SELECT
  c.CategoryId,
  c.DisplayName,
  c.RegionCode,
  c.LanguageCode,
  c.Status,
  c.FirstSeenUtc,
  c.LastSeenUtc,
  c.LastSyncedUtc
FROM dbo.GoogleBusinessProfileCategory c
{whereSql}
ORDER BY c.DisplayName ASC, c.CategoryId ASC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;", parameters, cancellationToken: ct))).ToList();

        return new GoogleBusinessProfileCategoryListResult(rows, totalCount, normalizedPage, normalizedPageSize, totalPages);
    }

    public async Task<IReadOnlyList<GoogleBusinessProfileCategoryLookupItem>> GetActiveLookupAsync(string regionCode, string languageCode, CancellationToken ct)
    {
        var normalizedRegionCode = NormalizeRegionCode(regionCode);
        var normalizedLanguageCode = NormalizeLanguageCode(languageCode);
        if (string.IsNullOrWhiteSpace(normalizedRegionCode) || string.IsNullOrWhiteSpace(normalizedLanguageCode))
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<GoogleBusinessProfileCategoryLookupItem>(new CommandDefinition(@"
SELECT
  CategoryId,
  DisplayName,
  RegionCode,
  LanguageCode
FROM dbo.GoogleBusinessProfileCategory
WHERE Status = @StatusActive
  AND RegionCode = @RegionCode
  AND LanguageCode = @LanguageCode
ORDER BY DisplayName ASC, CategoryId ASC;", new
        {
            StatusActive,
            RegionCode = normalizedRegionCode,
            LanguageCode = normalizedLanguageCode
        }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<GoogleBusinessProfileCategorySyncSummary?> GetLatestSyncSummaryAsync(string regionCode, string languageCode, CancellationToken ct)
    {
        var normalizedRegionCode = NormalizeRegionCode(regionCode);
        var normalizedLanguageCode = NormalizeLanguageCode(languageCode);
        if (string.IsNullOrWhiteSpace(normalizedRegionCode) || string.IsNullOrWhiteSpace(normalizedLanguageCode))
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<GoogleBusinessProfileCategorySyncSummary>(new CommandDefinition(@"
SELECT TOP 1
  RanAtUtc,
  AddedCount,
  UpdatedCount,
  MarkedInactiveCount
FROM dbo.GoogleBusinessProfileCategorySyncRun
WHERE RegionCode = @RegionCode
  AND LanguageCode = @LanguageCode
ORDER BY GoogleBusinessProfileCategorySyncRunId DESC;", new
        {
            RegionCode = normalizedRegionCode,
            LanguageCode = normalizedLanguageCode
        }, cancellationToken: ct));
    }

    public async Task<GoogleBusinessProfileCategoryEditModel?> GetByIdAsync(string categoryId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<GoogleBusinessProfileCategoryEditModel>(new CommandDefinition(@"
SELECT
  CategoryId,
  DisplayName,
  RegionCode,
  LanguageCode,
  Status
FROM dbo.GoogleBusinessProfileCategory
WHERE CategoryId = @CategoryId;", new { CategoryId = normalizedCategoryId }, cancellationToken: ct));
    }

    public async Task AddManualAsync(GoogleBusinessProfileCategoryCreateModel model, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(model.CategoryId);
        var normalizedDisplayName = NormalizeDisplayName(model.DisplayName);
        var normalizedRegionCode = NormalizeRegionCode(model.RegionCode);
        var normalizedLanguageCode = NormalizeLanguageCode(model.LanguageCode);

        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Category ID is required.");
        if (string.IsNullOrWhiteSpace(normalizedDisplayName))
            throw new InvalidOperationException("Display Name is required.");
        if (string.IsNullOrWhiteSpace(normalizedRegionCode))
            throw new InvalidOperationException("Region Code is required.");
        if (string.IsNullOrWhiteSpace(normalizedLanguageCode))
            throw new InvalidOperationException("Language Code is required.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var exists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GoogleBusinessProfileCategory
WHERE CategoryId = @CategoryId;", new { CategoryId = normalizedCategoryId }, cancellationToken: ct));
        if (exists > 0)
            throw new InvalidOperationException($"Category ID '{normalizedCategoryId}' already exists.");

        var nowUtc = DateTime.UtcNow;
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.GoogleBusinessProfileCategory(
  CategoryId,
  DisplayName,
  RegionCode,
  LanguageCode,
  Status,
  FirstSeenUtc,
  LastSeenUtc,
  LastSyncedUtc
)
VALUES(
  @CategoryId,
  @DisplayName,
  @RegionCode,
  @LanguageCode,
  @Status,
  @NowUtc,
  @NowUtc,
  @NowUtc
);", new
        {
            CategoryId = normalizedCategoryId,
            DisplayName = normalizedDisplayName,
            RegionCode = normalizedRegionCode,
            LanguageCode = normalizedLanguageCode,
            Status = StatusActive,
            NowUtc = nowUtc
        }, cancellationToken: ct));
    }

    public async Task<bool> UpdateDisplayNameAsync(string categoryId, string displayName, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        var normalizedDisplayName = NormalizeDisplayName(displayName);

        if (string.IsNullOrWhiteSpace(normalizedCategoryId) || string.IsNullOrWhiteSpace(normalizedDisplayName))
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var affected = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GoogleBusinessProfileCategory
SET DisplayName = @DisplayName
WHERE CategoryId = @CategoryId;", new
        {
            CategoryId = normalizedCategoryId,
            DisplayName = normalizedDisplayName
        }, cancellationToken: ct));
        return affected > 0;
    }

    public async Task<bool> MarkInactiveAsync(string categoryId, CancellationToken ct)
    {
        var normalizedCategoryId = NormalizeCategoryId(categoryId);
        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var affected = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GoogleBusinessProfileCategory
SET
  Status = @StatusInactive,
  LastSyncedUtc = SYSUTCDATETIME()
WHERE CategoryId = @CategoryId;", new
        {
            CategoryId = normalizedCategoryId,
            StatusInactive = StatusInactive
        }, cancellationToken: ct));
        return affected > 0;
    }

    public async Task<GoogleBusinessProfileCategorySyncRunResult> SyncFromGoogleAsync(string regionCode, string languageCode, CancellationToken ct)
    {
        var normalizedRegionCode = NormalizeRegionCode(regionCode);
        var normalizedLanguageCode = NormalizeLanguageCode(languageCode);
        if (string.IsNullOrWhiteSpace(normalizedRegionCode))
            throw new InvalidOperationException("Region Code is required.");
        if (string.IsNullOrWhiteSpace(normalizedLanguageCode))
            throw new InvalidOperationException("Language Code is required.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var accessToken = await GetBusinessProfileAccessTokenAsync(ct);
        var cursor = await GetSyncCursorAsync(conn, normalizedRegionCode, normalizedLanguageCode, ct);
        var currentCycleId = cursor is null || string.IsNullOrWhiteSpace(cursor.NextPageToken)
            ? Guid.NewGuid()
            : cursor.CycleId;
        var pageToken = string.IsNullOrWhiteSpace(cursor?.NextPageToken) ? null : cursor!.NextPageToken;

        var totalAdded = 0;
        var totalUpdated = 0;
        var totalMarkedInactive = 0;
        var pagesFetched = 0;
        var ranAtUtc = DateTime.UtcNow;
        var seenTokens = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            while (true)
            {
                if (!string.IsNullOrWhiteSpace(pageToken) && !seenTokens.Add(pageToken))
                    throw new InvalidOperationException("Google categories sync stopped due to a repeated page token.");

                GoogleCategoriesPage page;
                try
                {
                    page = await FetchGoogleCategoriesPageAsync(accessToken, normalizedRegionCode, normalizedLanguageCode, pageToken, ct);
                }
                catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    ranAtUtc = DateTime.UtcNow;
                    await RecordSyncRunAsync(conn, normalizedRegionCode, normalizedLanguageCode, ranAtUtc, totalAdded, totalUpdated, totalMarkedInactive, ct);
                    return new GoogleBusinessProfileCategorySyncRunResult(
                        ranAtUtc,
                        totalAdded,
                        totalUpdated,
                        totalMarkedInactive,
                        pagesFetched,
                        IsCycleComplete: false,
                        WasRateLimited: true);
                }

                var syncedAtUtc = DateTime.UtcNow;
                await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);
                try
                {
                    var pageResult = await UpsertIncomingPageAsync(
                        conn,
                        tx,
                        page.Categories,
                        normalizedRegionCode,
                        normalizedLanguageCode,
                        currentCycleId,
                        syncedAtUtc,
                        ct);
                    totalAdded += pageResult.AddedCount;
                    totalUpdated += pageResult.UpdatedCount;
                    pagesFetched++;

                    pageToken = string.IsNullOrWhiteSpace(page.NextPageToken) ? null : page.NextPageToken;
                    await UpsertSyncCursorAsync(conn, tx, normalizedRegionCode, normalizedLanguageCode, currentCycleId, pageToken, syncedAtUtc, ct);

                    if (pageToken is null)
                    {
                        totalMarkedInactive += await MarkMissingInactiveForCycleAsync(
                            conn,
                            tx,
                            normalizedRegionCode,
                            normalizedLanguageCode,
                            currentCycleId,
                            syncedAtUtc,
                            ct);
                    }

                    await tx.CommitAsync(ct);
                    ranAtUtc = syncedAtUtc;
                }
                catch
                {
                    await tx.RollbackAsync(ct);
                    throw;
                }

                if (pageToken is null)
                    break;
            }

            await RecordSyncRunAsync(conn, normalizedRegionCode, normalizedLanguageCode, ranAtUtc, totalAdded, totalUpdated, totalMarkedInactive, ct);
            return new GoogleBusinessProfileCategorySyncRunResult(
                ranAtUtc,
                totalAdded,
                totalUpdated,
                totalMarkedInactive,
                pagesFetched,
                IsCycleComplete: true,
                WasRateLimited: false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogError(ex, "Google category sync failed for region {RegionCode} language {LanguageCode}.", normalizedRegionCode, normalizedLanguageCode);
            throw;
        }
    }

    private async Task<GoogleCategoriesPage> FetchGoogleCategoriesPageAsync(
        string accessToken,
        string regionCode,
        string languageCode,
        string? pageToken,
        CancellationToken ct)
    {
        var url = BuildCategoriesUrl(regionCode, languageCode, pageToken);
        var client = httpClientFactory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException(
                $"Google Business Profile Categories API error {(int)response.StatusCode}: {body}",
                null,
                response.StatusCode);

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        var categoriesById = new Dictionary<string, IncomingCategory>(StringComparer.OrdinalIgnoreCase);
        if (root.TryGetProperty("categories", out var categoriesNode) && categoriesNode.ValueKind == JsonValueKind.Array)
        {
            foreach (var categoryNode in categoriesNode.EnumerateArray())
            {
                var categoryId = ReadString(categoryNode, "name");
                if (string.IsNullOrWhiteSpace(categoryId))
                    continue;

                var displayName = ReadString(categoryNode, "displayName");
                if (string.IsNullOrWhiteSpace(displayName))
                    displayName = categoryId;

                categoriesById[categoryId] = new IncomingCategory(categoryId, displayName);
            }
        }

        var nextPageToken = root.TryGetProperty("nextPageToken", out var tokenNode)
            ? tokenNode.GetString()
            : null;

        return new GoogleCategoriesPage(
            categoriesById.Values
                .OrderBy(x => x.DisplayName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => x.CategoryId, StringComparer.OrdinalIgnoreCase)
                .ToList(),
            string.IsNullOrWhiteSpace(nextPageToken) ? null : nextPageToken);
    }

    private async Task<PageSyncCounts> UpsertIncomingPageAsync(
        SqlConnection conn,
        SqlTransaction tx,
        IReadOnlyList<IncomingCategory> incomingCategories,
        string regionCode,
        string languageCode,
        Guid cycleId,
        DateTime syncedAtUtc,
        CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(@"
CREATE TABLE #IncomingCategories(
  CategoryId nvarchar(255) NOT NULL PRIMARY KEY,
  DisplayName nvarchar(300) NOT NULL
);", transaction: tx, cancellationToken: ct));

        if (incomingCategories.Count > 0)
        {
            var incomingData = new DataTable();
            incomingData.Columns.Add("CategoryId", typeof(string));
            incomingData.Columns.Add("DisplayName", typeof(string));
            foreach (var incoming in incomingCategories)
                incomingData.Rows.Add(incoming.CategoryId, incoming.DisplayName);

            using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tx)
            {
                DestinationTableName = "#IncomingCategories"
            };
            bulkCopy.ColumnMappings.Add("CategoryId", "CategoryId");
            bulkCopy.ColumnMappings.Add("DisplayName", "DisplayName");
            await bulkCopy.WriteToServerAsync(incomingData, ct);
        }

        var addedCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM #IncomingCategories i
LEFT JOIN dbo.GoogleBusinessProfileCategory c
  ON c.CategoryId = i.CategoryId
WHERE c.CategoryId IS NULL;", transaction: tx, cancellationToken: ct));

        var updatedCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM #IncomingCategories i
JOIN dbo.GoogleBusinessProfileCategory c
  ON c.CategoryId = i.CategoryId
WHERE
  ISNULL(c.DisplayName, N'') <> ISNULL(i.DisplayName, N'')
  OR c.RegionCode <> @RegionCode
  OR c.LanguageCode <> @LanguageCode
  OR c.Status <> @StatusActive
  OR c.LastSeenCycleId IS NULL
  OR c.LastSeenCycleId <> @CycleId;", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode,
            StatusActive,
            CycleId = cycleId
        }, tx, cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.GoogleBusinessProfileCategory AS target
USING #IncomingCategories AS source
ON target.CategoryId = source.CategoryId
WHEN MATCHED THEN UPDATE SET
  DisplayName = source.DisplayName,
  RegionCode = @RegionCode,
  LanguageCode = @LanguageCode,
  Status = @StatusActive,
  LastSeenUtc = @SyncedAtUtc,
  LastSyncedUtc = @SyncedAtUtc,
  LastSeenCycleId = @CycleId
WHEN NOT MATCHED THEN
  INSERT(
    CategoryId,
    DisplayName,
    RegionCode,
    LanguageCode,
    Status,
    FirstSeenUtc,
    LastSeenUtc,
    LastSyncedUtc,
    LastSeenCycleId
  )
  VALUES(
    source.CategoryId,
    source.DisplayName,
    @RegionCode,
    @LanguageCode,
    @StatusActive,
    @SyncedAtUtc,
    @SyncedAtUtc,
    @SyncedAtUtc,
    @CycleId
  );", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode,
            StatusActive,
            SyncedAtUtc = syncedAtUtc,
            CycleId = cycleId
        }, tx, cancellationToken: ct));

        return new PageSyncCounts(addedCount, updatedCount);
    }

    private async Task<int> MarkMissingInactiveForCycleAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string regionCode,
        string languageCode,
        Guid cycleId,
        DateTime syncedAtUtc,
        CancellationToken ct)
    {
        return await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE c
SET
  c.Status = @StatusInactive,
  c.LastSyncedUtc = @SyncedAtUtc
FROM dbo.GoogleBusinessProfileCategory c
WHERE c.RegionCode = @RegionCode
  AND c.LanguageCode = @LanguageCode
  AND c.Status <> @StatusInactive
  AND (c.LastSeenCycleId IS NULL OR c.LastSeenCycleId <> @CycleId);", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode,
            StatusInactive,
            SyncedAtUtc = syncedAtUtc,
            CycleId = cycleId
        }, tx, cancellationToken: ct));
    }

    private async Task RecordSyncRunAsync(
        SqlConnection conn,
        string regionCode,
        string languageCode,
        DateTime ranAtUtc,
        int addedCount,
        int updatedCount,
        int markedInactiveCount,
        CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.GoogleBusinessProfileCategorySyncRun(
  RegionCode,
  LanguageCode,
  RanAtUtc,
  AddedCount,
  UpdatedCount,
  MarkedInactiveCount
)
VALUES(
  @RegionCode,
  @LanguageCode,
  @RanAtUtc,
  @AddedCount,
  @UpdatedCount,
  @MarkedInactiveCount
);", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode,
            RanAtUtc = ranAtUtc,
            AddedCount = addedCount,
            UpdatedCount = updatedCount,
            MarkedInactiveCount = markedInactiveCount
        }, cancellationToken: ct));
    }

    private async Task<SyncCursorRow?> GetSyncCursorAsync(SqlConnection conn, string regionCode, string languageCode, CancellationToken ct)
    {
        return await conn.QuerySingleOrDefaultAsync<SyncCursorRow>(new CommandDefinition(@"
SELECT
  RegionCode,
  LanguageCode,
  CycleId,
  NextPageToken
FROM dbo.GoogleBusinessProfileCategorySyncCursor
WHERE RegionCode = @RegionCode
  AND LanguageCode = @LanguageCode;", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode
        }, cancellationToken: ct));
    }

    private async Task UpsertSyncCursorAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string regionCode,
        string languageCode,
        Guid cycleId,
        string? nextPageToken,
        DateTime updatedUtc,
        CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(@"
MERGE dbo.GoogleBusinessProfileCategorySyncCursor AS target
USING (SELECT @RegionCode AS RegionCode, @LanguageCode AS LanguageCode) AS source
ON target.RegionCode = source.RegionCode
AND target.LanguageCode = source.LanguageCode
WHEN MATCHED THEN
  UPDATE SET
    CycleId = @CycleId,
    NextPageToken = @NextPageToken,
    UpdatedUtc = @UpdatedUtc
WHEN NOT MATCHED THEN
  INSERT(RegionCode, LanguageCode, CycleId, NextPageToken, UpdatedUtc)
  VALUES(@RegionCode, @LanguageCode, @CycleId, @NextPageToken, @UpdatedUtc);", new
        {
            RegionCode = regionCode,
            LanguageCode = languageCode,
            CycleId = cycleId,
            NextPageToken = nextPageToken,
            UpdatedUtc = updatedUtc
        }, tx, cancellationToken: ct));
    }

    private async Task<string> GetBusinessProfileAccessTokenAsync(CancellationToken ct)
    {
        var clientId = (googleOptions.Value.ClientId ?? string.Empty).Trim();
        var clientSecret = (googleOptions.Value.ClientSecret ?? string.Empty).Trim();
        var refreshToken = (await googleBusinessProfileOAuthService.GetRefreshTokenAsync(ct) ?? string.Empty).Trim();

        if (string.IsNullOrWhiteSpace(clientId))
            throw new InvalidOperationException("Google OAuth client ID is missing.");
        if (string.IsNullOrWhiteSpace(clientSecret))
            throw new InvalidOperationException("Google OAuth client secret is missing.");
        if (string.IsNullOrWhiteSpace(refreshToken))
            throw new InvalidOperationException("Google is not connected. Visit /admin/google/connect first.");

        var client = httpClientFactory.CreateClient();
        using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, "https://oauth2.googleapis.com/token")
        {
            Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["refresh_token"] = refreshToken,
                ["grant_type"] = "refresh_token"
            })
        };
        using var tokenResponse = await client.SendAsync(tokenRequest, ct);
        var body = await tokenResponse.Content.ReadAsStringAsync(ct);
        if (!tokenResponse.IsSuccessStatusCode)
            throw new HttpRequestException($"Google OAuth token request failed {(int)tokenResponse.StatusCode}: {body}");

        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("access_token", out var accessTokenNode) || accessTokenNode.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("Google OAuth token response did not include access_token.");

        var accessToken = accessTokenNode.GetString();
        if (string.IsNullOrWhiteSpace(accessToken))
            throw new InvalidOperationException("Google OAuth access token was empty.");

        return accessToken;
    }

    private static string BuildCategoriesUrl(string regionCode, string languageCode, string? pageToken)
    {
        var query = new List<string>
        {
            $"regionCode={Uri.EscapeDataString(regionCode)}",
            $"languageCode={Uri.EscapeDataString(languageCode)}",
            "pageSize=100",
            "view=BASIC"
        };
        if (!string.IsNullOrWhiteSpace(pageToken))
            query.Add($"pageToken={Uri.EscapeDataString(pageToken)}");

        return $"https://mybusinessbusinessinformation.googleapis.com/v1/categories?{string.Join("&", query)}";
    }

    private static string? NormalizeStatusFilter(string? statusFilter)
    {
        if (string.IsNullOrWhiteSpace(statusFilter))
            return StatusActive;
        if (string.Equals(statusFilter, "active", StringComparison.OrdinalIgnoreCase))
            return StatusActive;
        if (string.Equals(statusFilter, "inactive", StringComparison.OrdinalIgnoreCase))
            return StatusInactive;
        if (string.Equals(statusFilter, "all", StringComparison.OrdinalIgnoreCase))
            return null;
        return StatusActive;
    }

    private static string NormalizeCategoryId(string? value)
        => (value ?? string.Empty).Trim();

    private static string NormalizeDisplayName(string? value)
        => (value ?? string.Empty).Trim();

    private static string NormalizeRegionCode(string? value)
        => (value ?? string.Empty).Trim().ToUpperInvariant();

    private static string NormalizeLanguageCode(string? value)
        => (value ?? string.Empty).Trim();

    private static string? ReadString(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var node))
            return null;

        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Object when node.TryGetProperty("text", out var textNode) && textNode.ValueKind == JsonValueKind.String => textNode.GetString(),
            _ => null
        };
    }

    private sealed record IncomingCategory(string CategoryId, string DisplayName);

    private sealed record GoogleCategoriesPage(IReadOnlyList<IncomingCategory> Categories, string? NextPageToken);

    private sealed record PageSyncCounts(int AddedCount, int UpdatedCount);

    private sealed record SyncCursorRow(
        string RegionCode,
        string LanguageCode,
        Guid CycleId,
        string? NextPageToken);
}
