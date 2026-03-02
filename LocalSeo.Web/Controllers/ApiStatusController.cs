using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public sealed class ApiStatusController(
    IApiStatusService apiStatusService,
    IExternalApiHealthService externalApiHealthService,
    IApiStatusRefreshRateLimiter refreshRateLimiter,
    TimeProvider timeProvider) : Controller
{
    [HttpGet("/api-status")]
    public async Task<IActionResult> Index([FromQuery] string? category, [FromQuery] string? q, CancellationToken ct)
    {
        var internalModel = await apiStatusService.GetDetailsAsync(null, null, ct);
        var externalWidgets = await externalApiHealthService.GetDashboardWidgetsAsync(ct);

        var rows = new List<ApiStatusWidgetModel>(internalModel.Rows.Count + externalWidgets.Count);
        rows.AddRange(internalModel.Rows);
        for (var i = 0; i < externalWidgets.Count; i++)
            rows.Add(ToExternalWidgetModel(externalWidgets[i], i));

        var normalizedCategory = NormalizeOptional(category);
        var normalizedSearch = NormalizeOptional(q);
        var categories = rows
            .Select(x => x.Category)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (!string.IsNullOrWhiteSpace(normalizedCategory))
        {
            rows = rows
                .Where(x => string.Equals(x.Category, normalizedCategory, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        if (!string.IsNullOrWhiteSpace(normalizedSearch))
        {
            rows = rows
                .Where(x =>
                    ContainsIgnoreCase(x.DisplayName, normalizedSearch)
                    || ContainsIgnoreCase(x.Key, normalizedSearch)
                    || ContainsIgnoreCase(x.Message, normalizedSearch))
                .ToList();
        }

        var model = new ApiStatusDetailsViewModel
        {
            Rows = rows,
            CategoryOptions = categories,
            SelectedCategory = normalizedCategory,
            Search = normalizedSearch
        };

        return View(model);
    }

    [HttpPost("/api-status/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Refresh(CancellationToken ct)
    {
        var rateLimitKey = User.Identity?.Name
            ?? HttpContext.Connection.RemoteIpAddress?.ToString()
            ?? "anonymous";
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var decision = refreshRateLimiter.TryAcquire(rateLimitKey, nowUtc);
        if (!decision.Allowed)
        {
            Response.Headers["Retry-After"] = decision.RetryAfterSeconds.ToString();
            return StatusCode(StatusCodes.Status429TooManyRequests, new
            {
                success = false,
                message = $"Refresh is rate-limited. Try again in {decision.RetryAfterSeconds} seconds."
            });
        }

        var snapshot = await apiStatusService.RefreshAllChecksAsync(ct);
        await externalApiHealthService.RunChecksAsync(ct);
        var rows = snapshot.Widgets.Select(x => new
        {
            x.Key,
            x.DisplayName,
            x.Category,
            Status = (int)x.Status,
            StatusLabel = ToStatusLabel(x.Status),
            x.CheckedUtc,
            x.LatencyMs,
            x.Message
        });

        return Json(new
        {
            success = true,
            checkedUtc = snapshot.RetrievedUtc,
            rows
        });
    }

    private static string? NormalizeOptional(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = value.Trim();
        return normalized.Length == 0 ? null : normalized;
    }

    private static bool ContainsIgnoreCase(string? value, string needle)
    {
        if (string.IsNullOrWhiteSpace(value) || string.IsNullOrWhiteSpace(needle))
            return false;

        return value.Contains(needle, StringComparison.OrdinalIgnoreCase);
    }

    private static ApiStatusWidgetModel ToExternalWidgetModel(ExternalApiHealthWidgetModel row, int index)
    {
        var status = !row.HasData
            ? ApiHealthStatus.Unknown
            : row.IsUp
                ? ApiHealthStatus.Up
                : row.IsDegraded
                    ? ApiHealthStatus.Degraded
                    : ApiHealthStatus.Down;

        var key = $"external.{BuildExternalKey(row.Name)}";
        var message = !string.IsNullOrWhiteSpace(row.LastError)
            ? $"Last error: {row.LastError}"
            : $"Endpoint: {row.EndpointCalled}; HTTP {(row.HttpStatusCode?.ToString() ?? "N/A")}.";

        return new ApiStatusWidgetModel(
            DefinitionId: -1000 - index,
            Key: key,
            DisplayName: row.Name,
            Category: "External Maps",
            IsEnabled: true,
            Status: status,
            CheckedUtc: row.CheckedAtUtc,
            LatencyMs: row.LatencyMs,
            Message: message);
    }

    private static string BuildExternalKey(string name)
    {
        var chars = name
            .Trim()
            .ToLowerInvariant()
            .Select(ch => char.IsLetterOrDigit(ch) ? ch : '-')
            .ToArray();
        var key = new string(chars).Trim('-');
        return string.IsNullOrWhiteSpace(key) ? "unknown" : key;
    }

    private static string ToStatusLabel(ApiHealthStatus status)
    {
        return status switch
        {
            ApiHealthStatus.Up => "Up",
            ApiHealthStatus.Degraded => "Degraded",
            ApiHealthStatus.Down => "Down",
            _ => "Unknown"
        };
    }
}
