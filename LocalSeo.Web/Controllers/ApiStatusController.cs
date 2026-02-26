using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public sealed class ApiStatusController(
    IApiStatusService apiStatusService,
    IApiStatusRefreshRateLimiter refreshRateLimiter,
    TimeProvider timeProvider) : Controller
{
    [HttpGet("/api-status")]
    public async Task<IActionResult> Index([FromQuery] string? category, [FromQuery] string? q, CancellationToken ct)
    {
        var model = await apiStatusService.GetDetailsAsync(category, q, ct);
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
