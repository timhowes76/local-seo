using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class AdminController(
    IAdminMaintenanceService adminMaintenanceService,
    IDataForSeoTaskTracker dataForSeoTaskTracker) : Controller
{
    [HttpGet("/admin")]
    public IActionResult Index() => View();

    [HttpGet("/admin/dataforseo-tasks")]
    public async Task<IActionResult> DataForSeoTasks(CancellationToken ct)
    {
        var rows = await dataForSeoTaskTracker.GetLatestTasksAsync(500, ct);
        return View(rows);
    }

    [HttpPost("/admin/dataforseo-tasks/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshDataForSeoTasks(CancellationToken ct)
    {
        var touched = await dataForSeoTaskTracker.RefreshTaskStatusesAsync(ct);
        TempData["Status"] = $"Refreshed DataForSEO task statuses. Updated {touched} row(s).";
        return RedirectToAction(nameof(DataForSeoTasks));
    }

    [HttpPost("/admin/dataforseo-tasks/{id:long}/populate")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> PopulateDataForSeoTask(long id, CancellationToken ct)
    {
        var result = await dataForSeoTaskTracker.PopulateTaskAsync(id, ct);
        TempData["Status"] = result.Success
            ? $"Populate succeeded: {result.Message}"
            : $"Populate failed: {result.Message}";
        return RedirectToAction(nameof(DataForSeoTasks));
    }

    [HttpPost("/admin/clear-runs")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ClearRuns(CancellationToken ct)
    {
        var result = await adminMaintenanceService.ClearRunDataAsync(ct);
        TempData["Status"] = $"Cleared run data: {result.DataForSeoTaskDeleted} DataForSEO tasks, {result.PlaceReviewDeleted} reviews, {result.SearchRunDeleted} runs, {result.PlaceSnapshotDeleted} snapshots, {result.PlaceDeleted} places.";
        return RedirectToAction(nameof(Index));
    }
}
