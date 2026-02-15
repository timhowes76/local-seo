using LocalSeo.Web.Services;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class AdminController(
    IAdminMaintenanceService adminMaintenanceService,
    IDataForSeoTaskTracker dataForSeoTaskTracker,
    IAdminSettingsService adminSettingsService) : Controller
{
    [HttpGet("/admin")]
    public IActionResult Index() => View();

    [HttpGet("/admin/settings")]
    public async Task<IActionResult> Settings(CancellationToken ct)
    {
        var model = await adminSettingsService.GetAsync(ct);
        return View(model);
    }

    [HttpPost("/admin/settings")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveSettings(AdminSettingsModel model, CancellationToken ct)
    {
        if (model.EnhancedGoogleDataRefreshHours < 1)
            ModelState.AddModelError(nameof(model.EnhancedGoogleDataRefreshHours), "Value must be at least 1 hour.");
        if (model.GoogleReviewsRefreshHours < 1)
            ModelState.AddModelError(nameof(model.GoogleReviewsRefreshHours), "Value must be at least 1 hour.");
        if (model.GoogleUpdatesRefreshHours < 1)
            ModelState.AddModelError(nameof(model.GoogleUpdatesRefreshHours), "Value must be at least 1 hour.");
        if (model.GoogleQuestionsAndAnswersRefreshHours < 1)
            ModelState.AddModelError(nameof(model.GoogleQuestionsAndAnswersRefreshHours), "Value must be at least 1 hour.");

        if (!ModelState.IsValid)
            return View("Settings", model);

        await adminSettingsService.SaveAsync(model, ct);
        TempData["Status"] = "Settings saved.";
        return RedirectToAction(nameof(Settings));
    }

    [HttpGet("/admin/dataforseo-tasks")]
    public async Task<IActionResult> DataForSeoTasks([FromQuery] string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = NormalizeTaskTypeFilter(taskType);
        var rows = await dataForSeoTaskTracker.GetLatestTasksAsync(2000, normalizedTaskType, ct);
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

    [HttpPost("/admin/dataforseo-tasks/populate-ready")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> PopulateReadyDataForSeoTasks([FromQuery] string? taskType, CancellationToken ct)
    {
        var normalizedTaskType = NormalizeTaskTypeFilter(taskType);
        var result = await dataForSeoTaskTracker.PopulateReadyTasksAsync(normalizedTaskType, ct);
        TempData["Status"] =
            $"Populate ready tasks complete. Attempted {result.Attempted}, succeeded {result.Succeeded}, failed {result.Failed}, upserted {result.ReviewsUpserted} review row(s).";
        return RedirectToAction(nameof(DataForSeoTasks), new { taskType = normalizedTaskType ?? "all" });
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
        if (string.Equals(taskType, "my_business_updates", StringComparison.OrdinalIgnoreCase))
            return "my_business_updates";
        if (string.Equals(taskType, "questions_and_answers", StringComparison.OrdinalIgnoreCase))
            return "questions_and_answers";
        if (string.Equals(taskType, "reviews", StringComparison.OrdinalIgnoreCase))
            return "reviews";
        return null;
    }
}
