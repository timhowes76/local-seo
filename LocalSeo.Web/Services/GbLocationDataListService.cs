using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IGbLocationDataListService
{
    Task<GbCountyListResult> GetCountiesPagedAsync(string? statusFilter, string? search, int page, int pageSize, CancellationToken ct);
    Task<IReadOnlyList<GbCountyRow>> GetCountiesForSortAsync(CancellationToken ct);
    Task SaveCountySortOrderAsync(IReadOnlyList<long> orderedCountyIds, CancellationToken ct);
    Task<GbCountyEditModel?> GetCountyByIdAsync(long countyId, CancellationToken ct);
    Task AddCountyAsync(GbCountyCreateModel model, CancellationToken ct);
    Task<bool> UpdateCountyAsync(GbCountyEditModel model, CancellationToken ct);
    Task<bool> MarkCountyInactiveAsync(long countyId, CancellationToken ct);
    Task<IReadOnlyList<GbCountyLookupItem>> GetCountyLookupAsync(bool includeInactive, CancellationToken ct);
    Task<IReadOnlyList<GbTownLookupItem>> GetTownLookupByCountyAsync(long countyId, bool includeInactive, CancellationToken ct);
    Task<GbTownLookupItem?> GetTownLookupByIdAsync(long townId, CancellationToken ct);

    Task<GbTownListResult> GetTownsPagedAsync(string? statusFilter, string? search, long? countyId, int page, int pageSize, CancellationToken ct);
    Task<IReadOnlyList<GbTownRow>> GetTownsForSortAsync(long countyId, CancellationToken ct);
    Task SaveTownSortOrderAsync(long countyId, IReadOnlyList<long> orderedTownIds, CancellationToken ct);
    Task<GbTownEditModel?> GetTownByIdAsync(long townId, CancellationToken ct);
    Task AddTownAsync(GbTownCreateModel model, CancellationToken ct);
    Task<bool> UpdateTownAsync(GbTownEditModel model, CancellationToken ct);
    Task<bool> MarkTownInactiveAsync(long townId, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetRunsByTownAsync(long townId, CancellationToken ct);
    Task<IReadOnlyList<SearchRun>> GetRunsByCountyAsync(long countyId, CancellationToken ct);
}

public sealed class GbLocationDataListService(ISqlConnectionFactory connectionFactory) : IGbLocationDataListService
{
    public async Task<GbCountyListResult> GetCountiesPagedAsync(string? statusFilter, string? search, int page, int pageSize, CancellationToken ct)
    {
        var normalizedStatus = NormalizeStatusFilter(statusFilter);
        var normalizedSearch = (search ?? string.Empty).Trim();
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);
        var whereParts = new List<string>();
        if (normalizedStatus is not null)
            whereParts.Add("c.IsActive = @IsActiveFilter");
        if (!string.IsNullOrWhiteSpace(normalizedSearch))
            whereParts.Add("(c.Name LIKE @SearchPattern OR c.Slug LIKE @SearchPattern)");
        var whereSql = whereParts.Count == 0 ? string.Empty : $"WHERE {string.Join(" AND ", whereParts)}";

        var parameters = new
        {
            IsActiveFilter = normalizedStatus,
            SearchPattern = $"%{normalizedSearch}%",
            Offset = (normalizedPage - 1) * normalizedPageSize,
            PageSize = normalizedPageSize
        };

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition($@"
SELECT COUNT(1)
FROM dbo.GbCounty c
{whereSql};", parameters, cancellationToken: ct));

        var totalPages = Math.Max(1, (int)Math.Ceiling(totalCount / (double)normalizedPageSize));
        if (normalizedPage > totalPages)
        {
            normalizedPage = totalPages;
            parameters = new
            {
                IsActiveFilter = normalizedStatus,
                SearchPattern = $"%{normalizedSearch}%",
                Offset = (normalizedPage - 1) * normalizedPageSize,
                PageSize = normalizedPageSize
            };
        }

        var rows = (await conn.QueryAsync<GbCountyRow>(new CommandDefinition($@"
SELECT
  c.CountyId,
  c.Name,
  c.Slug,
  c.IsActive,
  c.SortOrder,
  c.CreatedUtc,
  c.UpdatedUtc
FROM dbo.GbCounty c
{whereSql}
ORDER BY
  CASE WHEN c.SortOrder IS NULL THEN 1 ELSE 0 END,
  c.SortOrder,
  c.Name
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;", parameters, cancellationToken: ct))).ToList();

        return new GbCountyListResult(rows, totalCount, normalizedPage, normalizedPageSize, totalPages);
    }

    public async Task<IReadOnlyList<GbCountyRow>> GetCountiesForSortAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<GbCountyRow>(new CommandDefinition(@"
SELECT
  CountyId,
  Name,
  Slug,
  IsActive,
  SortOrder,
  CreatedUtc,
  UpdatedUtc
FROM dbo.GbCounty
ORDER BY
  CASE WHEN SortOrder IS NULL THEN 1 ELSE 0 END,
  SortOrder,
  Name;", cancellationToken: ct));
        return rows.ToList();
    }

    public async Task SaveCountySortOrderAsync(IReadOnlyList<long> orderedCountyIds, CancellationToken ct)
    {
        if (orderedCountyIds.Count == 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        for (var i = 0; i < orderedCountyIds.Count; i++)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbCounty
SET
  SortOrder = @SortOrder,
  UpdatedUtc = SYSUTCDATETIME()
WHERE CountyId = @CountyId;", new
            {
                CountyId = orderedCountyIds[i],
                SortOrder = (i + 1) * 10
            }, tx, cancellationToken: ct));
        }

        await tx.CommitAsync(ct);
    }

    public async Task<GbCountyEditModel?> GetCountyByIdAsync(long countyId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<GbCountyEditModel>(new CommandDefinition(@"
SELECT
  CountyId,
  Name,
  Slug,
  IsActive,
  SortOrder
FROM dbo.GbCounty
WHERE CountyId = @CountyId;", new { CountyId = countyId }, cancellationToken: ct));
    }

    public async Task AddCountyAsync(GbCountyCreateModel model, CancellationToken ct)
    {
        var normalizedName = NormalizeRequiredName(model.Name, "County name");
        var normalizedSlug = NormalizeNullable(model.Slug);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var nextSortOrder = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT ISNULL(MAX(SortOrder), 0) + 10
FROM dbo.GbCounty;", cancellationToken: ct));
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.GbCounty(Name, Slug, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
VALUES(@Name, @Slug, @IsActive, @SortOrder, SYSUTCDATETIME(), SYSUTCDATETIME());", new
        {
            Name = normalizedName,
            Slug = normalizedSlug,
            model.IsActive,
            SortOrder = nextSortOrder
        }, cancellationToken: ct));
    }

    public async Task<bool> UpdateCountyAsync(GbCountyEditModel model, CancellationToken ct)
    {
        var normalizedName = NormalizeRequiredName(model.Name, "County name");
        var normalizedSlug = NormalizeNullable(model.Slug);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var touched = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbCounty
SET
  Name = @Name,
  Slug = @Slug,
  IsActive = @IsActive,
  UpdatedUtc = SYSUTCDATETIME()
WHERE CountyId = @CountyId;", new
        {
            model.CountyId,
            Name = normalizedName,
            Slug = normalizedSlug,
            model.IsActive
        }, cancellationToken: ct));
        return touched > 0;
    }

    public async Task<bool> MarkCountyInactiveAsync(long countyId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var touched = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbCounty
SET
  IsActive = 0,
  UpdatedUtc = SYSUTCDATETIME()
WHERE CountyId = @CountyId;", new { CountyId = countyId }, cancellationToken: ct));
        return touched > 0;
    }

    public async Task<IReadOnlyList<GbCountyLookupItem>> GetCountyLookupAsync(bool includeInactive, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var whereSql = includeInactive ? string.Empty : "WHERE IsActive = 1";
        var rows = await conn.QueryAsync<GbCountyLookupItem>(new CommandDefinition($@"
SELECT
  CountyId,
  Name,
  IsActive
FROM dbo.GbCounty
{whereSql}
ORDER BY
  CASE WHEN SortOrder IS NULL THEN 1 ELSE 0 END,
  SortOrder,
  Name;", cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<GbTownLookupItem>> GetTownLookupByCountyAsync(long countyId, bool includeInactive, CancellationToken ct)
    {
        if (countyId <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var whereSql = includeInactive ? string.Empty : "AND t.IsActive = 1";
        var rows = await conn.QueryAsync<GbTownLookupItem>(new CommandDefinition($@"
SELECT
  t.TownId,
  t.CountyId,
  c.Name AS CountyName,
  t.Name,
  t.IsActive,
  t.Latitude,
  t.Longitude
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
WHERE t.CountyId = @CountyId
  {whereSql}
ORDER BY
  CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END,
  t.SortOrder,
  t.Name;", new { CountyId = countyId }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<GbTownLookupItem?> GetTownLookupByIdAsync(long townId, CancellationToken ct)
    {
        if (townId <= 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<GbTownLookupItem>(new CommandDefinition(@"
SELECT TOP 1
  t.TownId,
  t.CountyId,
  c.Name AS CountyName,
  t.Name,
  t.IsActive,
  t.Latitude,
  t.Longitude
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
WHERE t.TownId = @TownId;", new { TownId = townId }, cancellationToken: ct));
    }

    public async Task<GbTownListResult> GetTownsPagedAsync(string? statusFilter, string? search, long? countyId, int page, int pageSize, CancellationToken ct)
    {
        var normalizedStatus = NormalizeStatusFilter(statusFilter);
        var normalizedSearch = (search ?? string.Empty).Trim();
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);
        var whereParts = new List<string>();
        if (normalizedStatus is not null)
            whereParts.Add("t.IsActive = @IsActiveFilter");
        if (!string.IsNullOrWhiteSpace(normalizedSearch))
            whereParts.Add("(t.Name LIKE @SearchPattern OR t.Slug LIKE @SearchPattern OR t.ExternalId LIKE @SearchPattern)");
        if (countyId.HasValue && countyId.Value > 0)
            whereParts.Add("t.CountyId = @CountyIdFilter");
        var whereSql = whereParts.Count == 0 ? string.Empty : $"WHERE {string.Join(" AND ", whereParts)}";

        var parameters = new
        {
            IsActiveFilter = normalizedStatus,
            SearchPattern = $"%{normalizedSearch}%",
            CountyIdFilter = countyId,
            Offset = (normalizedPage - 1) * normalizedPageSize,
            PageSize = normalizedPageSize
        };

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var totalCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition($@"
SELECT COUNT(1)
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
{whereSql};", parameters, cancellationToken: ct));

        var totalPages = Math.Max(1, (int)Math.Ceiling(totalCount / (double)normalizedPageSize));
        if (normalizedPage > totalPages)
        {
            normalizedPage = totalPages;
            parameters = new
            {
                IsActiveFilter = normalizedStatus,
                SearchPattern = $"%{normalizedSearch}%",
                CountyIdFilter = countyId,
                Offset = (normalizedPage - 1) * normalizedPageSize,
                PageSize = normalizedPageSize
            };
        }

        var rows = (await conn.QueryAsync<GbTownRow>(new CommandDefinition($@"
SELECT
  t.TownId,
  t.CountyId,
  c.Name AS CountyName,
  t.Name,
  t.Slug,
  t.Latitude,
  t.Longitude,
  t.ExternalId,
  t.IsActive,
  t.SortOrder,
  t.CreatedUtc,
  t.UpdatedUtc
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
{whereSql}
ORDER BY
  CASE WHEN c.SortOrder IS NULL THEN 1 ELSE 0 END,
  c.SortOrder,
  c.Name,
  CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END,
  t.SortOrder,
  t.Name
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;", parameters, cancellationToken: ct))).ToList();

        return new GbTownListResult(rows, totalCount, normalizedPage, normalizedPageSize, totalPages);
    }

    public async Task<IReadOnlyList<GbTownRow>> GetTownsForSortAsync(long countyId, CancellationToken ct)
    {
        if (countyId <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<GbTownRow>(new CommandDefinition(@"
SELECT
  t.TownId,
  t.CountyId,
  c.Name AS CountyName,
  t.Name,
  t.Slug,
  t.Latitude,
  t.Longitude,
  t.ExternalId,
  t.IsActive,
  t.SortOrder,
  t.CreatedUtc,
  t.UpdatedUtc
FROM dbo.GbTown t
JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
WHERE t.CountyId = @CountyId
ORDER BY
  CASE WHEN t.SortOrder IS NULL THEN 1 ELSE 0 END,
  t.SortOrder,
  t.Name;", new { CountyId = countyId }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task SaveTownSortOrderAsync(long countyId, IReadOnlyList<long> orderedTownIds, CancellationToken ct)
    {
        if (countyId <= 0 || orderedTownIds.Count == 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        for (var i = 0; i < orderedTownIds.Count; i++)
        {
            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbTown
SET
  SortOrder = @SortOrder,
  UpdatedUtc = SYSUTCDATETIME()
WHERE TownId = @TownId
  AND CountyId = @CountyId;", new
            {
                TownId = orderedTownIds[i],
                CountyId = countyId,
                SortOrder = (i + 1) * 10
            }, tx, cancellationToken: ct));
        }

        await tx.CommitAsync(ct);
    }

    public async Task<GbTownEditModel?> GetTownByIdAsync(long townId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<GbTownEditModel>(new CommandDefinition(@"
SELECT
  TownId,
  CountyId,
  Name,
  Slug,
  Latitude,
  Longitude,
  ExternalId,
  IsActive,
  SortOrder
FROM dbo.GbTown
WHERE TownId = @TownId;", new { TownId = townId }, cancellationToken: ct));
    }

    public async Task AddTownAsync(GbTownCreateModel model, CancellationToken ct)
    {
        var normalizedName = NormalizeRequiredName(model.Name, "Town name");
        var normalizedSlug = NormalizeNullable(model.Slug);
        var normalizedExternalId = NormalizeNullable(model.ExternalId);
        if (model.CountyId <= 0)
            throw new InvalidOperationException("County is required.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var countyExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GbCounty
WHERE CountyId = @CountyId;", new { CountyId = model.CountyId }, cancellationToken: ct));
        if (countyExists == 0)
            throw new InvalidOperationException("Selected county does not exist.");

        var existing = await conn.QuerySingleOrDefaultAsync<ExistingTownRow>(new CommandDefinition(@"
SELECT TOP 1
  TownId,
  IsActive
FROM dbo.GbTown
WHERE CountyId = @CountyId
  AND Name = @Name;", new
        {
            CountyId = model.CountyId,
            Name = normalizedName
        }, cancellationToken: ct));

        if (existing is not null)
        {
            if (existing.IsActive)
                throw new InvalidOperationException("A town with the same name already exists in the selected county.");

            await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbTown
SET
  Slug = @Slug,
  Latitude = COALESCE(@Latitude, Latitude),
  Longitude = COALESCE(@Longitude, Longitude),
  ExternalId = @ExternalId,
  IsActive = @IsActive,
  UpdatedUtc = SYSUTCDATETIME()
WHERE TownId = @TownId;", new
            {
                existing.TownId,
                Slug = normalizedSlug,
                Latitude = (decimal?)null,
                Longitude = (decimal?)null,
                ExternalId = normalizedExternalId,
                model.IsActive
            }, cancellationToken: ct));
            return;
        }

        var nextSortOrder = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT ISNULL(MAX(SortOrder), 0) + 10
FROM dbo.GbTown
WHERE CountyId = @CountyId;", new { model.CountyId }, cancellationToken: ct));

        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.GbTown(CountyId, Name, Slug, Latitude, Longitude, ExternalId, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
VALUES(@CountyId, @Name, @Slug, @Latitude, @Longitude, @ExternalId, @IsActive, @SortOrder, SYSUTCDATETIME(), SYSUTCDATETIME());", new
        {
            model.CountyId,
            Name = normalizedName,
            Slug = normalizedSlug,
            Latitude = (decimal?)null,
            Longitude = (decimal?)null,
            ExternalId = normalizedExternalId,
            model.IsActive,
            SortOrder = nextSortOrder
        }, cancellationToken: ct));
    }

    public async Task<bool> UpdateTownAsync(GbTownEditModel model, CancellationToken ct)
    {
        var normalizedName = NormalizeRequiredName(model.Name, "Town name");
        var normalizedSlug = NormalizeNullable(model.Slug);
        var normalizedExternalId = NormalizeNullable(model.ExternalId);
        await ValidateTownInputsAsync(model.CountyId, normalizedName, model.TownId, ct);

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var touched = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbTown
SET
  CountyId = @CountyId,
  Name = @Name,
  Slug = @Slug,
  Latitude = @Latitude,
  Longitude = @Longitude,
  ExternalId = @ExternalId,
  IsActive = @IsActive,
  UpdatedUtc = SYSUTCDATETIME()
WHERE TownId = @TownId;", new
        {
            model.TownId,
            model.CountyId,
            Name = normalizedName,
            Slug = normalizedSlug,
            model.Latitude,
            model.Longitude,
            ExternalId = normalizedExternalId,
            model.IsActive
        }, cancellationToken: ct));
        return touched > 0;
    }

    public async Task<bool> MarkTownInactiveAsync(long townId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var touched = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.GbTown
SET
  IsActive = 0,
  UpdatedUtc = SYSUTCDATETIME()
WHERE TownId = @TownId;", new { TownId = townId }, cancellationToken: ct));
        return touched > 0;
    }

    public async Task<IReadOnlyList<SearchRun>> GetRunsByTownAsync(long townId, CancellationToken ct)
    {
        if (townId <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT
  r.SearchRunId,
  r.CategoryId,
  r.TownId,
  t.CountyId,
  c.DisplayName AS SeedKeyword,
  CONCAT(t.Name, N', ', county.Name) AS LocationName,
  t.Latitude AS CenterLat,
  t.Longitude AS CenterLng,
  r.RadiusMeters,
  r.ResultLimit,
  r.FetchDetailedData,
  r.FetchGoogleReviews,
  r.FetchGoogleUpdates,
  r.FetchGoogleQuestionsAndAnswers,
  r.FetchGoogleSocialProfiles,
  r.RanAtUtc
FROM dbo.SearchRun r
JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
JOIN dbo.GbTown t ON t.TownId = r.TownId
JOIN dbo.GbCounty county ON county.CountyId = t.CountyId
WHERE r.TownId = @TownId
ORDER BY r.SearchRunId DESC;", new { TownId = townId }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<SearchRun>> GetRunsByCountyAsync(long countyId, CancellationToken ct)
    {
        if (countyId <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SearchRun>(new CommandDefinition(@"
SELECT
  r.SearchRunId,
  r.CategoryId,
  r.TownId,
  t.CountyId,
  c.DisplayName AS SeedKeyword,
  t.Name AS LocationName,
  t.Latitude AS CenterLat,
  t.Longitude AS CenterLng,
  r.RadiusMeters,
  r.ResultLimit,
  r.FetchDetailedData,
  r.FetchGoogleReviews,
  r.FetchGoogleUpdates,
  r.FetchGoogleQuestionsAndAnswers,
  r.FetchGoogleSocialProfiles,
  r.RanAtUtc
FROM dbo.SearchRun r
JOIN dbo.GoogleBusinessProfileCategory c ON c.CategoryId = r.CategoryId
JOIN dbo.GbTown t ON t.TownId = r.TownId
WHERE t.CountyId = @CountyId
ORDER BY r.SearchRunId DESC;", new { CountyId = countyId }, cancellationToken: ct));
        return rows.ToList();
    }

    private async Task ValidateTownInputsAsync(long countyId, string townName, long? excludeTownId, CancellationToken ct)
    {
        if (countyId <= 0)
            throw new InvalidOperationException("County is required.");

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var countyExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GbCounty
WHERE CountyId = @CountyId;", new { CountyId = countyId }, cancellationToken: ct));
        if (countyExists == 0)
            throw new InvalidOperationException("Selected county does not exist.");

        var duplicateCount = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.GbTown
WHERE CountyId = @CountyId
  AND Name = @Name
  AND (@ExcludeTownId IS NULL OR TownId <> @ExcludeTownId);", new
        {
            CountyId = countyId,
            Name = townName,
            ExcludeTownId = excludeTownId
        }, cancellationToken: ct));
        if (duplicateCount > 0)
            throw new InvalidOperationException("A town with the same name already exists in the selected county.");
    }

    private static bool? NormalizeStatusFilter(string? statusFilter)
    {
        if (string.Equals(statusFilter, "inactive", StringComparison.OrdinalIgnoreCase))
            return false;
        if (string.Equals(statusFilter, "all", StringComparison.OrdinalIgnoreCase))
            return null;
        return true;
    }

    private static string NormalizeRequiredName(string value, string fieldName)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalized))
            throw new InvalidOperationException($"{fieldName} is required.");
        return normalized;
    }

    private static string? NormalizeNullable(string? value)
    {
        var normalized = (value ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(normalized) ? null : normalized;
    }

    private sealed record ExistingTownRow(long TownId, bool IsActive);
}
