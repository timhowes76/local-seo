namespace LocalSeo.Web.Models;

public sealed class AnnouncementEditModel
{
    public long? AnnouncementId { get; set; }
    public string Title { get; set; } = string.Empty;
    public string BodyHtml { get; set; } = string.Empty;
}

public sealed class AnnouncementAdminListRow
{
    public long AnnouncementId { get; init; }
    public string Title { get; init; } = string.Empty;
    public string BodyHtml { get; init; } = string.Empty;
    public string BodyPreview { get; init; } = string.Empty;
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

public sealed class AnnouncementAdminDetailRow
{
    public long AnnouncementId { get; init; }
    public string Title { get; init; } = string.Empty;
    public string BodyHtml { get; init; } = string.Empty;
    public DateTime CreatedAtUtc { get; init; }
    public int? CreatedByUserId { get; init; }
    public string? CreatedByName { get; init; }
    public string? CreatedByEmail { get; init; }
    public DateTime? UpdatedAtUtc { get; init; }
    public int? UpdatedByUserId { get; init; }
    public string? UpdatedByName { get; init; }
    public string? UpdatedByEmail { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime? DeletedAtUtc { get; init; }
    public int? DeletedByUserId { get; init; }
    public string? DeletedByName { get; init; }
    public string? DeletedByEmail { get; init; }
}

public sealed class AnnouncementReadLogRow
{
    public long AnnouncementReadId { get; init; }
    public long AnnouncementId { get; init; }
    public int UserId { get; init; }
    public DateTime ReadAtUtc { get; init; }
    public string UserName { get; init; } = string.Empty;
    public string? EmailAddress { get; init; }
}

public sealed class AnnouncementAdminListViewModel
{
    public IReadOnlyList<AnnouncementAdminListRow> Rows { get; init; } = [];
}

public sealed class AnnouncementAdminEditViewModel
{
    public string Mode { get; init; } = "create";
    public string? Message { get; init; }
    public AnnouncementEditModel Announcement { get; init; } = new();
}

public sealed class AnnouncementAdminDetailViewModel
{
    public AnnouncementAdminDetailRow Announcement { get; init; } = new();
    public IReadOnlyList<AnnouncementReadLogRow> ReadLogRows { get; init; } = [];
    public int ReadCount { get; init; }
}

public sealed class AnnouncementModalItem
{
    public long Id { get; init; }
    public string Title { get; init; } = string.Empty;
    public string HtmlBody { get; init; } = string.Empty;
    public DateTime CreatedAtUtc { get; init; }
    public string CreatedByName { get; init; } = string.Empty;
}

public sealed class AnnouncementModalFeedResponse
{
    public string Mode { get; init; } = "latest";
    public IReadOnlyList<AnnouncementModalItem> Items { get; init; } = [];
}

public sealed class AnnouncementMarkReadRequest
{
    public long AnnouncementId { get; init; }
}
