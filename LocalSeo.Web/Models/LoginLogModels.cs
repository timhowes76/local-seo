namespace LocalSeo.Web.Models;

public enum LoginLogSucceededFilter
{
    All = 0,
    Succeeded = 1,
    Failed = 2
}

public sealed class LoginLogQuery
{
    public string SearchText { get; init; } = string.Empty;
    public LoginLogSucceededFilter SucceededFilter { get; init; } = LoginLogSucceededFilter.All;
    public int PageSize { get; init; } = 100;
    public int PageNumber { get; init; } = 1;
}

public sealed class UserLoginAttempt
{
    public DateTime AttemptedAtUtc { get; init; }
    public string? EmailEntered { get; init; }
    public string? EmailNormalized { get; init; }
    public int? UserId { get; init; }
    public string IpAddress { get; init; } = string.Empty;
    public bool Succeeded { get; init; }
    public string? FailureReason { get; init; }
    public string? AuthStage { get; init; }
    public string? UserAgent { get; init; }
    public string? CorrelationId { get; init; }
}

public sealed class UserLoginLogRow
{
    public long UserLoginId { get; init; }
    public DateTime AttemptedAtUtc { get; init; }
    public string? EmailEntered { get; init; }
    public string? EmailNormalized { get; init; }
    public int? UserId { get; init; }
    public string IpAddress { get; init; } = string.Empty;
    public bool Succeeded { get; init; }
    public string? FailureReason { get; init; }
    public string? AuthStage { get; init; }
    public string? UserAgent { get; init; }
    public string? CorrelationId { get; init; }
}

public sealed class PagedResult<T>
{
    public PagedResult(IReadOnlyList<T> items, int totalCount)
    {
        Items = items;
        TotalCount = totalCount;
    }

    public IReadOnlyList<T> Items { get; }
    public int TotalCount { get; }
}

public sealed class LoginLogListViewModel
{
    public IReadOnlyList<UserLoginLogRow> Rows { get; init; } = [];
    public string SearchText { get; init; } = string.Empty;
    public string SucceededFilter { get; init; } = "all";
    public int PageSize { get; init; } = 100;
    public int PageNumber { get; init; } = 1;
    public int TotalCount { get; init; }
    public int TotalPages { get; init; } = 1;
}
