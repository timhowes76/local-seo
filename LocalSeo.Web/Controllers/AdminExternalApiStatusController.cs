using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminExternalApiStatusController(
    IServiceScopeFactory scopeFactory,
    ILogger<AdminExternalApiStatusController> logger) : Controller
{
    [HttpPost("/admin/api-status/check-now")]
    public IActionResult CheckNow()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var service = scope.ServiceProvider.GetRequiredService<Services.IExternalApiHealthService>();
                await service.RunChecksAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Manual external API health check failed.");
            }
        });

        return Accepted(new { success = true, message = "External API health checks started." });
    }
}
