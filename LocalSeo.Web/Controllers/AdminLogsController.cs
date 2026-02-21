using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminLogsController(IUserLoginLogRepository userLoginLogRepository) : Controller
{
    [HttpGet("/admin/logs")]
    public IActionResult Index() => View();

    [HttpGet("/admin/logs/login-log")]
    public async Task<IActionResult> LoginLog([FromQuery] string? q, [FromQuery] string? succeeded, [FromQuery] int page = 1, [FromQuery] int pageSize = 100, CancellationToken ct = default)
    {
        var normalizedSearch = NormalizeSearchText(q);
        var normalizedSucceeded = ParseSucceededFilter(succeeded);
        var normalizedPageSize = NormalizePageSize(pageSize);
        var normalizedPage = Math.Max(1, page);

        var query = new LoginLogQuery
        {
            SearchText = normalizedSearch,
            SucceededFilter = normalizedSucceeded,
            PageSize = normalizedPageSize,
            PageNumber = normalizedPage
        };

        var result = await userLoginLogRepository.SearchAsync(query, ct);
        var totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedPageSize));
        if (normalizedPage > totalPages)
        {
            normalizedPage = totalPages;
            query = new LoginLogQuery
            {
                SearchText = normalizedSearch,
                SucceededFilter = normalizedSucceeded,
                PageSize = normalizedPageSize,
                PageNumber = normalizedPage
            };
            result = await userLoginLogRepository.SearchAsync(query, ct);
            totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedPageSize));
        }

        return View(new LoginLogListViewModel
        {
            Rows = result.Items,
            SearchText = normalizedSearch,
            SucceededFilter = ToSucceededFilterKey(normalizedSucceeded),
            PageSize = normalizedPageSize,
            PageNumber = normalizedPage,
            TotalCount = result.TotalCount,
            TotalPages = totalPages
        });
    }

    private static string NormalizeSearchText(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        return trimmed.Length <= 200 ? trimmed : trimmed[..200];
    }

    private static LoginLogSucceededFilter ParseSucceededFilter(string? value)
    {
        if (string.Equals(value, "succeeded", StringComparison.OrdinalIgnoreCase))
            return LoginLogSucceededFilter.Succeeded;
        if (string.Equals(value, "failed", StringComparison.OrdinalIgnoreCase))
            return LoginLogSucceededFilter.Failed;
        return LoginLogSucceededFilter.All;
    }

    private static string ToSucceededFilterKey(LoginLogSucceededFilter filter)
    {
        return filter switch
        {
            LoginLogSucceededFilter.Succeeded => "succeeded",
            LoginLogSucceededFilter.Failed => "failed",
            _ => "all"
        };
    }

    private static int NormalizePageSize(int value)
    {
        return value switch
        {
            25 => 25,
            50 => 50,
            100 => 100,
            500 => 500,
            1000 => 1000,
            _ => 100
        };
    }
}
