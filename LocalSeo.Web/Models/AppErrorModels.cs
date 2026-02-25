namespace LocalSeo.Web.Models;

public sealed class AppErrorRow
{
    public long AppErrorId { get; init; }
    public DateTime CreatedUtc { get; init; }
    public int? UserId { get; init; }
    public string? UserFirstName { get; init; }
    public string? UserLastName { get; init; }
    public string? IpAddress { get; init; }
    public string? TraceId { get; init; }
    public string? HttpMethod { get; init; }
    public int StatusCode { get; init; }
    public string? FullUrl { get; init; }
    public string? Referrer { get; init; }
    public string? UserAgentRaw { get; init; }
    public string? BrowserName { get; init; }
    public string? BrowserVersion { get; init; }
    public string? OsName { get; init; }
    public string? OsVersion { get; init; }
    public string? DeviceType { get; init; }
    public string? ExceptionType { get; init; }
    public string? ExceptionMessage { get; init; }
    public string? ExceptionDetail { get; init; }
}

public sealed record BackgroundContext(
    string JobName,
    string? TraceId = null,
    int? UserId = null,
    string? ExtraText = null);

public sealed class AppErrorListViewModel
{
    public IReadOnlyList<AppErrorRow> Rows { get; init; } = [];
    public int PageSize { get; init; } = 50;
    public int PageNumber { get; init; } = 1;
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
    public bool IsDevelopment { get; init; }
}

public sealed class AppErrorDetailViewModel
{
    public AppErrorRow Row { get; init; } = new();
    public bool IsDevelopment { get; init; }
}
