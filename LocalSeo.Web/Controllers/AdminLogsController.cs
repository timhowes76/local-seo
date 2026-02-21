using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminLogsController(
    IUserLoginLogRepository userLoginLogRepository,
    IEmailLogQueryService emailLogQueryService,
    IEmailDeliveryStatusSyncService emailDeliveryStatusSyncService,
    IEmailTemplateService emailTemplateService) : Controller
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

    [HttpGet("/admin/logs/emails")]
    public async Task<IActionResult> Emails(
        [FromQuery] DateTime? fromUtc,
        [FromQuery] DateTime? toUtc,
        [FromQuery] string? templateKey,
        [FromQuery] string? status,
        [FromQuery] string? providerEventType,
        [FromQuery] string? recipient,
        [FromQuery] string? correlationId,
        [FromQuery] string? messageId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 100,
        CancellationToken ct = default)
    {
        var normalizedQuery = new EmailLogQuery
        {
            DateFromUtc = fromUtc,
            DateToUtc = toUtc,
            TemplateKey = NormalizeSearchText(templateKey, 100),
            Status = NormalizeSearchText(status, 20),
            ProviderEventType = NormalizeSearchText(providerEventType, 50),
            RecipientContains = NormalizeSearchText(recipient, 320),
            CorrelationId = NormalizeSearchText(correlationId, 64),
            MessageId = NormalizeSearchText(messageId, 200),
            PageNumber = Math.Max(1, page),
            PageSize = NormalizePageSize(pageSize)
        };

        var result = await emailLogQueryService.SearchAsync(normalizedQuery, ct);
        var totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedQuery.PageSize));
        if (normalizedQuery.PageNumber > totalPages)
        {
            normalizedQuery = new EmailLogQuery
            {
                DateFromUtc = normalizedQuery.DateFromUtc,
                DateToUtc = normalizedQuery.DateToUtc,
                TemplateKey = normalizedQuery.TemplateKey,
                Status = normalizedQuery.Status,
                ProviderEventType = normalizedQuery.ProviderEventType,
                RecipientContains = normalizedQuery.RecipientContains,
                CorrelationId = normalizedQuery.CorrelationId,
                MessageId = normalizedQuery.MessageId,
                PageSize = normalizedQuery.PageSize,
                PageNumber = totalPages
            };
            result = await emailLogQueryService.SearchAsync(normalizedQuery, ct);
            totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedQuery.PageSize));
        }

        var refreshed = await emailDeliveryStatusSyncService.RefreshPendingAsync(result.Items, ct);
        if (refreshed > 0)
        {
            result = await emailLogQueryService.SearchAsync(normalizedQuery, ct);
            totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedQuery.PageSize));
        }

        var templateKeys = (await emailTemplateService.ListAsync(ct))
            .Select(x => x.Key)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return View(new EmailLogListViewModel
        {
            Rows = result.Items,
            Query = normalizedQuery,
            TotalCount = result.TotalCount,
            TotalPages = totalPages,
            TemplateKeys = templateKeys
        });
    }

    [HttpGet("/admin/logs/emails/{id:long}")]
    public async Task<IActionResult> EmailDetails(long id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var viewModel = await emailLogQueryService.GetDetailsAsync(id, ct);
        if (viewModel is null)
            return NotFound();

        var refreshed = await emailDeliveryStatusSyncService.RefreshPendingAsync(viewModel.Log, ct);
        if (refreshed)
        {
            viewModel = await emailLogQueryService.GetDetailsAsync(id, ct);
            if (viewModel is null)
                return NotFound();
        }

        return View(viewModel);
    }

    private static string NormalizeSearchText(string? value, int maxLength = 200)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
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
