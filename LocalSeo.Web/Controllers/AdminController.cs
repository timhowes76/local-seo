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
    public async Task<IActionResult> DataForSeoTasks([FromQuery] string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = NormalizeTaskTypeFilter(taskType);
        var rows = await dataForSeoTaskTracker.GetLatestTasksAsync(500, normalizedTaskType, ct);
        ViewBag.TaskType = normalizedTaskType ?? "all";
        return View(rows);
    }

    [HttpPost("/admin/dataforseo-tasks/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshDataForSeoTasks([FromQuery] string? taskType, CancellationToken ct)
    {
        var touched = await dataForSeoTaskTracker.RefreshTaskStatusesAsync(ct);
        TempData["Status"] = $"Refreshed DataForSEO task statuses. Updated {touched} row(s).";
        return RedirectToAction(nameof(DataForSeoTasks), new { taskType = NormalizeTaskTypeFilter(taskType) ?? "all" });
    }

    [HttpPost("/admin/dataforseo-tasks/delete-errors")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteDataForSeoErrorTasks([FromQuery] string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = NormalizeTaskTypeFilter(taskType);
        var deleted = await dataForSeoTaskTracker.DeleteErrorTasksAsync(normalizedTaskType, ct);
        TempData["Status"] = $"Deleted {deleted} DataForSEO task row(s) with status Error.";
        return RedirectToAction(nameof(DataForSeoTasks), new { taskType = normalizedTaskType ?? "all" });
    }

    [HttpPost("/admin/dataforseo-tasks/{id:long}/populate")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> PopulateDataForSeoTask(long id, [FromQuery] string? taskType, CancellationToken ct)
    {
        var result = await dataForSeoTaskTracker.PopulateTaskAsync(id, ct);
        TempData["Status"] = result.Success
            ? $"Populate succeeded: {result.Message}"
            : $"Populate failed: {result.Message}";
        return RedirectToAction(nameof(DataForSeoTasks), new { taskType = NormalizeTaskTypeFilter(taskType) ?? "all" });
    }

    [HttpPost("/admin/clear-runs")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ClearRuns(CancellationToken ct)
    {
        var result = await adminMaintenanceService.ClearRunDataAsync(ct);
        TempData["Status"] = $"Cleared run data: {result.DataForSeoTaskDeleted} DataForSEO tasks, {result.PlaceReviewDeleted} reviews, {result.SearchRunDeleted} runs, {result.PlaceSnapshotDeleted} snapshots, {result.PlaceDeleted} places.";
        return RedirectToAction(nameof(Index));
    }

    private static string? NormalizeTaskTypeFilter(string? taskType)
    {
        if (string.IsNullOrWhiteSpace(taskType) || string.Equals(taskType, "all", StringComparison.OrdinalIgnoreCase))
            return null;
        if (string.Equals(taskType, "my_business_info", StringComparison.OrdinalIgnoreCase))
            return "my_business_info";
        if (string.Equals(taskType, "reviews", StringComparison.OrdinalIgnoreCase))
            return "reviews";
        return null;
    }
}
