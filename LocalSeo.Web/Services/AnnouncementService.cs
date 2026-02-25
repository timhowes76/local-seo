using System.Text.RegularExpressions;
using Dapper;
using Ganss.Xss;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using Microsoft.Data.SqlClient;

namespace LocalSeo.Web.Services;

public interface IAnnouncementHtmlSanitizer
{
    string Sanitize(string? html);
}

public sealed class AnnouncementHtmlSanitizer : IAnnouncementHtmlSanitizer
{
    private readonly HtmlSanitizer sanitizer = BuildSanitizer();

    public string Sanitize(string? html)
    {
        var source = (html ?? string.Empty).Trim();
        if (source.Length == 0)
            return string.Empty;
        return sanitizer.Sanitize(source);
    }

    private static HtmlSanitizer BuildSanitizer()
    {
        var sanitizer = new HtmlSanitizer();
        sanitizer.AllowedTags.Clear();
        sanitizer.AllowedAttributes.Clear();
        sanitizer.AllowedCssProperties.Clear();
        sanitizer.AllowedAtRules.Clear();
        sanitizer.AllowedSchemes.Clear();

        sanitizer.AllowedTags.UnionWith(
        [
            "p",
            "br",
            "ul",
            "ol",
            "li",
            "strong",
            "em",
            "a",
            "h1",
            "h2",
            "h3",
            "b",
            "i"
        ]);

        sanitizer.AllowedAttributes.UnionWith(["href", "title"]);
        sanitizer.AllowedSchemes.UnionWith(["http", "https", "mailto", "tel"]);
        return sanitizer;
    }
}

public interface IAnnouncementRepository
{
    Task<IReadOnlyList<AnnouncementAdminListDbRow>> GetAdminListAsync(CancellationToken ct);
    Task<AnnouncementAdminDetailRow?> GetAdminByIdAsync(long announcementId, CancellationToken ct);
    Task<IReadOnlyList<AnnouncementReadLogRow>> GetReadLogAsync(long announcementId, CancellationToken ct);
    Task<int> GetReadCountAsync(long announcementId, CancellationToken ct);
    Task<long> CreateAsync(string title, string bodyHtml, int? createdByUserId, DateTime nowUtc, CancellationToken ct);
    Task<bool> UpdateAsync(long announcementId, string title, string bodyHtml, int? updatedByUserId, DateTime nowUtc, CancellationToken ct);
    Task<bool> SoftDeleteAsync(long announcementId, int? deletedByUserId, DateTime nowUtc, CancellationToken ct);
    Task<int> GetUnreadCountAsync(int userId, CancellationToken ct);
    Task<IReadOnlyList<AnnouncementModalItem>> GetUnreadFeedAsync(int userId, int take, CancellationToken ct);
    Task<IReadOnlyList<AnnouncementModalItem>> GetLatestFeedAsync(int take, CancellationToken ct);
    Task MarkReadAsync(long announcementId, int userId, DateTime nowUtc, CancellationToken ct);
}

public sealed class AnnouncementAdminListDbRow
{
    public long AnnouncementId { get; init; }
    public string Title { get; init; } = string.Empty;
    public string BodyHtml { get; init; } = string.Empty;
    public DateTime CreatedAtUtc { get; init; }
    public int? CreatedByUserId { get; init; }
    public string? CreatedByName { get; init; }
    public DateTime? UpdatedAtUtc { get; init; }
    public int? UpdatedByUserId { get; init; }
    public string? UpdatedByName { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime? DeletedAtUtc { get; init; }
    public int? DeletedByUserId { get; init; }
    public string? DeletedByName { get; init; }
}

public sealed class AnnouncementRepository(ISqlConnectionFactory connectionFactory) : IAnnouncementRepository
{
    public async Task<IReadOnlyList<AnnouncementAdminListDbRow>> GetAdminListAsync(CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AnnouncementAdminListDbRow>(new CommandDefinition(@"
SELECT
  a.AnnouncementId,
  a.Title,
  a.BodyHtml,
  a.CreatedAtUtc,
  a.CreatedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(cu.FirstName, ' ', cu.LastName))), ''), cu.EmailAddress) AS CreatedByName,
  a.UpdatedAtUtc,
  a.UpdatedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(uu.FirstName, ' ', uu.LastName))), ''), uu.EmailAddress) AS UpdatedByName,
  a.IsDeleted,
  a.DeletedAtUtc,
  a.DeletedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(du.FirstName, ' ', du.LastName))), ''), du.EmailAddress) AS DeletedByName
FROM dbo.Announcements a
LEFT JOIN dbo.[User] cu ON cu.Id = a.CreatedByUserId
LEFT JOIN dbo.[User] uu ON uu.Id = a.UpdatedByUserId
LEFT JOIN dbo.[User] du ON du.Id = a.DeletedByUserId
ORDER BY a.CreatedAtUtc DESC, a.AnnouncementId DESC;",
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<AnnouncementAdminDetailRow?> GetAdminByIdAsync(long announcementId, CancellationToken ct)
    {
        if (announcementId <= 0)
            return null;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<AnnouncementAdminDetailRow>(new CommandDefinition(@"
SELECT
  a.AnnouncementId,
  a.Title,
  a.BodyHtml,
  a.CreatedAtUtc,
  a.CreatedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(cu.FirstName, ' ', cu.LastName))), ''), cu.EmailAddress) AS CreatedByName,
  cu.EmailAddress AS CreatedByEmail,
  a.UpdatedAtUtc,
  a.UpdatedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(uu.FirstName, ' ', uu.LastName))), ''), uu.EmailAddress) AS UpdatedByName,
  uu.EmailAddress AS UpdatedByEmail,
  a.IsDeleted,
  a.DeletedAtUtc,
  a.DeletedByUserId,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(du.FirstName, ' ', du.LastName))), ''), du.EmailAddress) AS DeletedByName,
  du.EmailAddress AS DeletedByEmail
FROM dbo.Announcements a
LEFT JOIN dbo.[User] cu ON cu.Id = a.CreatedByUserId
LEFT JOIN dbo.[User] uu ON uu.Id = a.UpdatedByUserId
LEFT JOIN dbo.[User] du ON du.Id = a.DeletedByUserId
WHERE a.AnnouncementId = @AnnouncementId;",
            new { AnnouncementId = announcementId },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<AnnouncementReadLogRow>> GetReadLogAsync(long announcementId, CancellationToken ct)
    {
        if (announcementId <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AnnouncementReadLogRow>(new CommandDefinition(@"
SELECT
  ar.AnnouncementReadId,
  ar.AnnouncementId,
  ar.UserId,
  ar.ReadAtUtc,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(u.FirstName, ' ', u.LastName))), ''), u.EmailAddress, CONCAT('User #', CAST(ar.UserId AS nvarchar(20)))) AS UserName,
  u.EmailAddress
FROM dbo.AnnouncementReads ar
LEFT JOIN dbo.[User] u ON u.Id = ar.UserId
WHERE ar.AnnouncementId = @AnnouncementId
ORDER BY ar.ReadAtUtc DESC, ar.AnnouncementReadId DESC;",
            new { AnnouncementId = announcementId },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<int> GetReadCountAsync(long announcementId, CancellationToken ct)
    {
        if (announcementId <= 0)
            return 0;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.AnnouncementReads
WHERE AnnouncementId = @AnnouncementId;",
            new { AnnouncementId = announcementId },
            cancellationToken: ct));
    }

    public async Task<long> CreateAsync(string title, string bodyHtml, int? createdByUserId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<long>(new CommandDefinition(@"
INSERT INTO dbo.Announcements(
  Title,
  BodyHtml,
  CreatedAtUtc,
  CreatedByUserId,
  UpdatedAtUtc,
  UpdatedByUserId,
  IsDeleted)
VALUES(
  @Title,
  @BodyHtml,
  @NowUtc,
  @CreatedByUserId,
  @NowUtc,
  @CreatedByUserId,
  0);
SELECT CAST(SCOPE_IDENTITY() AS bigint);",
            new
            {
                Title = Truncate(title, 200) ?? string.Empty,
                BodyHtml = bodyHtml ?? string.Empty,
                NowUtc = nowUtc,
                CreatedByUserId = NormalizeUserId(createdByUserId)
            },
            cancellationToken: ct));
    }

    public async Task<bool> UpdateAsync(long announcementId, string title, string bodyHtml, int? updatedByUserId, DateTime nowUtc, CancellationToken ct)
    {
        if (announcementId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var changed = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Announcements
SET
  Title = @Title,
  BodyHtml = @BodyHtml,
  UpdatedAtUtc = @NowUtc,
  UpdatedByUserId = @UpdatedByUserId
WHERE AnnouncementId = @AnnouncementId
  AND IsDeleted = 0;",
            new
            {
                AnnouncementId = announcementId,
                Title = Truncate(title, 200) ?? string.Empty,
                BodyHtml = bodyHtml ?? string.Empty,
                NowUtc = nowUtc,
                UpdatedByUserId = NormalizeUserId(updatedByUserId)
            },
            cancellationToken: ct));
        return changed > 0;
    }

    public async Task<bool> SoftDeleteAsync(long announcementId, int? deletedByUserId, DateTime nowUtc, CancellationToken ct)
    {
        if (announcementId <= 0)
            return false;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var changed = await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Announcements
SET
  IsDeleted = 1,
  DeletedAtUtc = @NowUtc,
  DeletedByUserId = @DeletedByUserId,
  UpdatedAtUtc = @NowUtc,
  UpdatedByUserId = @DeletedByUserId
WHERE AnnouncementId = @AnnouncementId
  AND IsDeleted = 0;",
            new
            {
                AnnouncementId = announcementId,
                NowUtc = nowUtc,
                DeletedByUserId = NormalizeUserId(deletedByUserId)
            },
            cancellationToken: ct));
        return changed > 0;
    }

    public async Task<int> GetUnreadCountAsync(int userId, CancellationToken ct)
    {
        if (userId <= 0)
            return 0;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.Announcements a
WHERE a.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1
    FROM dbo.AnnouncementReads ar
    WHERE ar.UserId = @UserId
      AND ar.AnnouncementId = a.AnnouncementId
  );",
            new { UserId = userId },
            cancellationToken: ct));
    }

    public async Task<IReadOnlyList<AnnouncementModalItem>> GetUnreadFeedAsync(int userId, int take, CancellationToken ct)
    {
        if (userId <= 0 || take <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AnnouncementModalItem>(new CommandDefinition(@"
SELECT TOP (@Take)
  a.AnnouncementId AS Id,
  a.Title,
  a.BodyHtml AS HtmlBody,
  a.CreatedAtUtc,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(cu.FirstName, ' ', cu.LastName))), ''), cu.EmailAddress, N'Unknown') AS CreatedByName
FROM dbo.Announcements a
LEFT JOIN dbo.[User] cu ON cu.Id = a.CreatedByUserId
WHERE a.IsDeleted = 0
  AND NOT EXISTS (
    SELECT 1
    FROM dbo.AnnouncementReads ar
    WHERE ar.UserId = @UserId
      AND ar.AnnouncementId = a.AnnouncementId
  )
ORDER BY a.CreatedAtUtc DESC, a.AnnouncementId DESC;",
            new
            {
                Take = take,
                UserId = userId
            },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<AnnouncementModalItem>> GetLatestFeedAsync(int take, CancellationToken ct)
    {
        if (take <= 0)
            return [];

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<AnnouncementModalItem>(new CommandDefinition(@"
SELECT TOP (@Take)
  a.AnnouncementId AS Id,
  a.Title,
  a.BodyHtml AS HtmlBody,
  a.CreatedAtUtc,
  COALESCE(NULLIF(LTRIM(RTRIM(CONCAT(cu.FirstName, ' ', cu.LastName))), ''), cu.EmailAddress, N'Unknown') AS CreatedByName
FROM dbo.Announcements a
LEFT JOIN dbo.[User] cu ON cu.Id = a.CreatedByUserId
WHERE a.IsDeleted = 0
ORDER BY a.CreatedAtUtc DESC, a.AnnouncementId DESC;",
            new { Take = take },
            cancellationToken: ct));
        return rows.ToList();
    }

    public async Task MarkReadAsync(long announcementId, int userId, DateTime nowUtc, CancellationToken ct)
    {
        if (announcementId <= 0 || userId <= 0)
            return;

        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.AnnouncementReads(
  AnnouncementId,
  UserId,
  ReadAtUtc)
SELECT
  @AnnouncementId,
  @UserId,
  @NowUtc
WHERE EXISTS (
  SELECT 1
  FROM dbo.Announcements a
  WHERE a.AnnouncementId = @AnnouncementId
    AND a.IsDeleted = 0
)
AND NOT EXISTS (
  SELECT 1
  FROM dbo.AnnouncementReads ar
  WHERE ar.AnnouncementId = @AnnouncementId
    AND ar.UserId = @UserId
);",
            new
            {
                AnnouncementId = announcementId,
                UserId = userId,
                NowUtc = nowUtc
            },
            cancellationToken: ct));
    }

    private static int? NormalizeUserId(int? userId)
    {
        if (!userId.HasValue || userId.Value <= 0)
            return null;
        return userId.Value;
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}

public interface IAnnouncementService
{
    Task<IReadOnlyList<AnnouncementAdminListRow>> GetAdminListAsync(CancellationToken ct);
    Task<AnnouncementAdminDetailViewModel?> GetAdminDetailAsync(long announcementId, CancellationToken ct);
    Task<AnnouncementEditModel?> GetAdminEditModelAsync(long announcementId, CancellationToken ct);
    Task<(bool Success, string Message, long? AnnouncementId)> CreateAsync(AnnouncementEditModel model, int? actorUserId, CancellationToken ct);
    Task<(bool Success, string Message)> UpdateAsync(long announcementId, AnnouncementEditModel model, int? actorUserId, CancellationToken ct);
    Task<(bool Success, string Message)> SoftDeleteAsync(long announcementId, int? actorUserId, CancellationToken ct);
    Task<int> GetUnreadCountAsync(int userId, CancellationToken ct);
    Task<AnnouncementModalFeedResponse> GetModalFeedAsync(int userId, int take, CancellationToken ct);
    Task<int> MarkReadAndGetUnreadCountAsync(long announcementId, int userId, CancellationToken ct);
}

public sealed partial class AnnouncementService(
    IAnnouncementRepository repository,
    IAnnouncementHtmlSanitizer htmlSanitizer,
    TimeProvider timeProvider,
    ILogger<AnnouncementService> logger) : IAnnouncementService
{
    private const int DefaultPreviewMaxLength = 120;

    public async Task<IReadOnlyList<AnnouncementAdminListRow>> GetAdminListAsync(CancellationToken ct)
    {
        var rows = await repository.GetAdminListAsync(ct);
        return rows.Select(row => new AnnouncementAdminListRow
        {
            AnnouncementId = row.AnnouncementId,
            Title = row.Title,
            BodyHtml = row.BodyHtml,
            BodyPreview = BuildPreviewText(row.BodyHtml, DefaultPreviewMaxLength),
            CreatedAtUtc = row.CreatedAtUtc,
            CreatedByUserId = row.CreatedByUserId,
            CreatedByName = row.CreatedByName,
            UpdatedAtUtc = row.UpdatedAtUtc,
            UpdatedByUserId = row.UpdatedByUserId,
            UpdatedByName = row.UpdatedByName,
            IsDeleted = row.IsDeleted,
            DeletedAtUtc = row.DeletedAtUtc,
            DeletedByUserId = row.DeletedByUserId,
            DeletedByName = row.DeletedByName
        }).ToList();
    }

    public async Task<AnnouncementAdminDetailViewModel?> GetAdminDetailAsync(long announcementId, CancellationToken ct)
    {
        var announcement = await repository.GetAdminByIdAsync(announcementId, ct);
        if (announcement is null)
            return null;

        var readLogRows = await repository.GetReadLogAsync(announcementId, ct);
        var readCount = await repository.GetReadCountAsync(announcementId, ct);
        return new AnnouncementAdminDetailViewModel
        {
            Announcement = announcement,
            ReadLogRows = readLogRows,
            ReadCount = readCount
        };
    }

    public async Task<AnnouncementEditModel?> GetAdminEditModelAsync(long announcementId, CancellationToken ct)
    {
        var announcement = await repository.GetAdminByIdAsync(announcementId, ct);
        if (announcement is null)
            return null;

        return new AnnouncementEditModel
        {
            AnnouncementId = announcement.AnnouncementId,
            Title = announcement.Title,
            BodyHtml = announcement.BodyHtml
        };
    }

    public async Task<(bool Success, string Message, long? AnnouncementId)> CreateAsync(AnnouncementEditModel model, int? actorUserId, CancellationToken ct)
    {
        if (!TryNormalize(model, out var title, out var bodyHtml))
            return (false, "Title and body are required.", null);

        try
        {
            var id = await repository.CreateAsync(
                title,
                bodyHtml,
                actorUserId,
                timeProvider.GetUtcNow().UtcDateTime,
                ct);
            return (true, "Announcement created.", id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create announcement. ActorUserId={ActorUserId}", actorUserId);
            return (false, "Unable to create announcement right now.", null);
        }
    }

    public async Task<(bool Success, string Message)> UpdateAsync(long announcementId, AnnouncementEditModel model, int? actorUserId, CancellationToken ct)
    {
        if (announcementId <= 0)
            return (false, "Announcement not found.");
        if (!TryNormalize(model, out var title, out var bodyHtml))
            return (false, "Title and body are required.");

        try
        {
            var updated = await repository.UpdateAsync(
                announcementId,
                title,
                bodyHtml,
                actorUserId,
                timeProvider.GetUtcNow().UtcDateTime,
                ct);

            return updated
                ? (true, "Announcement updated.")
                : (false, "Announcement not found or has been deleted.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update announcement. AnnouncementId={AnnouncementId} ActorUserId={ActorUserId}", announcementId, actorUserId);
            return (false, "Unable to update announcement right now.");
        }
    }

    public async Task<(bool Success, string Message)> SoftDeleteAsync(long announcementId, int? actorUserId, CancellationToken ct)
    {
        if (announcementId <= 0)
            return (false, "Announcement not found.");

        try
        {
            var deleted = await repository.SoftDeleteAsync(
                announcementId,
                actorUserId,
                timeProvider.GetUtcNow().UtcDateTime,
                ct);
            return deleted
                ? (true, "Announcement deleted.")
                : (false, "Announcement not found or already deleted.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to soft-delete announcement. AnnouncementId={AnnouncementId} ActorUserId={ActorUserId}", announcementId, actorUserId);
            return (false, "Unable to delete announcement right now.");
        }
    }

    public Task<int> GetUnreadCountAsync(int userId, CancellationToken ct)
    {
        return repository.GetUnreadCountAsync(userId, ct);
    }

    public async Task<AnnouncementModalFeedResponse> GetModalFeedAsync(int userId, int take, CancellationToken ct)
    {
        var normalizedTake = take <= 0 ? 10 : Math.Min(100, take);
        var unread = await repository.GetUnreadFeedAsync(userId, normalizedTake, ct);
        if (unread.Count > 0)
        {
            return new AnnouncementModalFeedResponse
            {
                Mode = "unread",
                Items = unread
            };
        }

        var latest = await repository.GetLatestFeedAsync(normalizedTake, ct);
        return new AnnouncementModalFeedResponse
        {
            Mode = "latest",
            Items = latest
        };
    }

    public async Task<int> MarkReadAndGetUnreadCountAsync(long announcementId, int userId, CancellationToken ct)
    {
        if (announcementId > 0 && userId > 0)
        {
            await repository.MarkReadAsync(announcementId, userId, timeProvider.GetUtcNow().UtcDateTime, ct);
        }

        return await repository.GetUnreadCountAsync(userId, ct);
    }

    private bool TryNormalize(AnnouncementEditModel model, out string title, out string bodyHtml)
    {
        title = NormalizeRequired(model.Title, 200) ?? string.Empty;
        var sanitizedBody = htmlSanitizer.Sanitize(model.BodyHtml);
        bodyHtml = sanitizedBody;

        if (title.Length == 0)
            return false;
        if (StripHtml(sanitizedBody).Length == 0)
            return false;

        return true;
    }

    private static string? NormalizeRequired(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string BuildPreviewText(string? html, int maxLength)
    {
        var plain = StripHtml(html ?? string.Empty);
        if (plain.Length <= maxLength)
            return plain;
        return $"{plain[..maxLength]}...";
    }

    private static string StripHtml(string html)
    {
        var noTags = HtmlTagRegex().Replace(html ?? string.Empty, " ");
        var decoded = System.Net.WebUtility.HtmlDecode(noTags);
        var normalized = MultiWhitespaceRegex().Replace(decoded, " ").Trim();
        return normalized;
    }

    [GeneratedRegex("<[^>]*>", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex HtmlTagRegex();

    [GeneratedRegex(@"\s+", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex MultiWhitespaceRegex();
}
