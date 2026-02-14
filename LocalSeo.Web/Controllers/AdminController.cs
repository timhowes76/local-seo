using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class AdminController(IAdminMaintenanceService adminMaintenanceService) : Controller
{
    [HttpGet("/admin")]
    public IActionResult Index() => View();

    [HttpPost("/admin/clear-runs")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ClearRuns(CancellationToken ct)
    {
        var result = await adminMaintenanceService.ClearRunDataAsync(ct);
        TempData["Status"] = $"Cleared run data: {result.SearchRunDeleted} runs, {result.PlaceSnapshotDeleted} snapshots, {result.PlaceDeleted} places.";
        return RedirectToAction(nameof(Index));
    }
}
