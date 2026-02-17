using LocalSeo.Web.Services;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class AdminController(
    IDataForSeoTaskTracker dataForSeoTaskTracker,
    IAdminSettingsService adminSettingsService,
    IGbLocationDataListService gbLocationDataListService,
    IGoogleBusinessProfileCategoryService googleBusinessProfileCategoryService,
    IGoogleBusinessProfileOAuthService googleBusinessProfileOAuthService) : Controller
{
    private const string UkRegionCode = "GB";
    private const string UkLanguageCode = "en-GB";

    [HttpGet("/admin")]
    public IActionResult Index() => View();

    [HttpGet("/admin/data-lists")]
    public IActionResult DataLists() => View();

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
        if (model.SearchVolumeRefreshCooldownDays < 1)
            ModelState.AddModelError(nameof(model.SearchVolumeRefreshCooldownDays), "Value must be at least 1 day.");
        ValidatePercent(nameof(model.MapPackClickSharePercent), model.MapPackClickSharePercent);
        ValidatePercent(nameof(model.MapPackCtrPosition1Percent), model.MapPackCtrPosition1Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition2Percent), model.MapPackCtrPosition2Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition3Percent), model.MapPackCtrPosition3Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition4Percent), model.MapPackCtrPosition4Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition5Percent), model.MapPackCtrPosition5Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition6Percent), model.MapPackCtrPosition6Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition7Percent), model.MapPackCtrPosition7Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition8Percent), model.MapPackCtrPosition8Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition9Percent), model.MapPackCtrPosition9Percent);
        ValidatePercent(nameof(model.MapPackCtrPosition10Percent), model.MapPackCtrPosition10Percent);

        if (!ModelState.IsValid)
            return View("Settings", model);

        await adminSettingsService.SaveAsync(model, ct);
        TempData["Status"] = "Settings saved.";
        return RedirectToAction(nameof(Settings));

        void ValidatePercent(string fieldName, int value)
        {
            if (value < 0 || value > 100)
                ModelState.AddModelError(fieldName, "Value must be between 0 and 100.");
        }
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

    // OAuth checklist:
    // 1) Google Cloud Console -> OAuth Client -> Authorized redirect URI:
    //    https://briskly-viceless-kayleen.ngrok-free.dev/admin/google/oauth/callback
    // 2) Authorized JavaScript origin (if needed):
    //    https://briskly-viceless-kayleen.ngrok-free.dev
    [HttpGet("/admin/google/connect")]
    public IActionResult ConnectGoogleBusinessProfile()
    {
        try
        {
            var connectUrl = googleBusinessProfileOAuthService.BuildConnectUrl(GetCurrentUserIdentityKey());
            return Redirect(connectUrl);
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Google connect failed: {ex.Message}";
            return RedirectToAction(nameof(GoogleBusinessProfileCategories));
        }
    }

    [HttpGet("/admin/google/oauth/callback")]
    public async Task<IActionResult> GoogleBusinessProfileOAuthCallback(
        [FromQuery] string? code,
        [FromQuery] string? state,
        [FromQuery] string? error,
        [FromQuery] string? error_description,
        CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(error))
        {
            var message = string.IsNullOrWhiteSpace(error_description)
                ? $"Google authorization failed: {error}."
                : $"Google authorization failed: {error} ({error_description}).";
            return View("GoogleBusinessProfileConnected", new GoogleOAuthConnectionResult(false, message, null));
        }

        try
        {
            var result = await googleBusinessProfileOAuthService.CompleteConnectionAsync(
                code ?? string.Empty,
                state ?? string.Empty,
                GetCurrentUserIdentityKey(),
                ct);
            return View("GoogleBusinessProfileConnected", result);
        }
        catch (Exception ex)
        {
            return View("GoogleBusinessProfileConnected", new GoogleOAuthConnectionResult(false, ex.Message, null));
        }
    }

    [HttpGet("/admin/data-lists/google-business-profile-categories")]
    public async Task<IActionResult> GoogleBusinessProfileCategories([FromQuery] string? q, [FromQuery] string? status, [FromQuery] int page = 1, [FromQuery] int pageSize = 50, CancellationToken ct = default)
    {
        var normalizedStatus = NormalizeCategoryStatusFilter(status);
        var normalizedSearch = (q ?? string.Empty).Trim();
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);

        var listResult = await googleBusinessProfileCategoryService.GetPagedAsync(normalizedStatus, normalizedSearch, normalizedPage, normalizedPageSize, ct);
        var syncSummary = await googleBusinessProfileCategoryService.GetLatestSyncSummaryAsync(UkRegionCode, UkLanguageCode, ct);
        var model = new GoogleBusinessProfileCategoryListViewModel
        {
            Rows = listResult.Rows,
            Search = normalizedSearch,
            StatusFilter = normalizedStatus,
            Page = listResult.Page,
            PageSize = listResult.PageSize,
            TotalCount = listResult.TotalCount,
            TotalPages = listResult.TotalPages,
            LastUkSyncSummary = syncSummary
        };

        return View(model);
    }

    [HttpGet("/admin/data-lists/google-business-profile-categories/add")]
    public IActionResult AddGoogleBusinessProfileCategory(
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50)
    {
        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeCategoryStatusFilter(status);
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(new GoogleBusinessProfileCategoryCreateModel
        {
            RegionCode = UkRegionCode,
            LanguageCode = UkLanguageCode
        });
    }

    [HttpPost("/admin/data-lists/google-business-profile-categories/add")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> AddGoogleBusinessProfileCategory(
        GoogleBusinessProfileCategoryCreateModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        try
        {
            await googleBusinessProfileCategoryService.AddManualAsync(model, ct);
            TempData["Status"] = $"Added category '{model.CategoryId}'.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Manual add failed: {ex.Message}";
        }

        return RedirectToAction(nameof(GoogleBusinessProfileCategories), BuildCategoryListRouteValues(status, q, page, pageSize));
    }

    [HttpGet("/admin/data-lists/google-business-profile-categories/edit")]
    public async Task<IActionResult> EditGoogleBusinessProfileCategory(
        [FromQuery] string categoryId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(categoryId))
            return BadRequest("Category ID is required.");

        var model = await googleBusinessProfileCategoryService.GetByIdAsync(categoryId, ct);
        if (model is null)
            return NotFound();

        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeCategoryStatusFilter(status);
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(model);
    }

    [HttpPost("/admin/data-lists/google-business-profile-categories/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveGoogleBusinessProfileCategoryEdit(
        GoogleBusinessProfileCategoryEditModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(model.CategoryId))
            return BadRequest("Category ID is required.");
        if (string.IsNullOrWhiteSpace(model.DisplayName))
            ModelState.AddModelError(nameof(model.DisplayName), "Display Name is required.");

        if (!ModelState.IsValid)
        {
            ViewBag.ReturnQ = (q ?? string.Empty).Trim();
            ViewBag.ReturnStatus = NormalizeCategoryStatusFilter(status);
            ViewBag.ReturnPage = Math.Max(1, page);
            ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
            return View("EditGoogleBusinessProfileCategory", model);
        }

        var updated = await googleBusinessProfileCategoryService.UpdateDisplayNameAsync(model.CategoryId, model.DisplayName, ct);
        TempData["Status"] = updated
            ? $"Updated category '{model.CategoryId}'."
            : $"Category '{model.CategoryId}' was not found.";
        return RedirectToAction(nameof(GoogleBusinessProfileCategories), BuildCategoryListRouteValues(status, q, page, pageSize));
    }

    [HttpPost("/admin/data-lists/google-business-profile-categories/mark-inactive")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> MarkGoogleBusinessProfileCategoryInactive(
        [FromForm] string categoryId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(categoryId))
            return BadRequest("Category ID is required.");

        var marked = await googleBusinessProfileCategoryService.MarkInactiveAsync(categoryId, ct);
        TempData["Status"] = marked
            ? $"Marked '{categoryId}' as Inactive."
            : $"Category '{categoryId}' was not found.";
        return RedirectToAction(nameof(GoogleBusinessProfileCategories), BuildCategoryListRouteValues(status, q, page, pageSize));
    }

    [HttpPost("/admin/data-lists/google-business-profile-categories/sync-uk")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SyncGoogleBusinessProfileCategoriesUk(
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        try
        {
            var summary = await googleBusinessProfileCategoryService.SyncFromGoogleAsync(UkRegionCode, UkLanguageCode, ct);
            TempData["Status"] = $"Sync completed at {summary.RanAtUtc:u}. Added: {summary.AddedCount}, Updated: {summary.UpdatedCount}, Marked Inactive: {summary.MarkedInactiveCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Sync failed: {ex.Message}";
        }

        return RedirectToAction(nameof(GoogleBusinessProfileCategories), BuildCategoryListRouteValues(status, q, page, pageSize));
    }

    [HttpGet("/admin/data-lists/counties-gb")]
    public async Task<IActionResult> CountiesGb([FromQuery] string? q, [FromQuery] string? status, [FromQuery] int page = 1, [FromQuery] int pageSize = 50, CancellationToken ct = default)
    {
        var normalizedStatus = NormalizeActiveFilter(status);
        var normalizedSearch = (q ?? string.Empty).Trim();
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);

        var listResult = await gbLocationDataListService.GetCountiesPagedAsync(normalizedStatus, normalizedSearch, normalizedPage, normalizedPageSize, ct);
        var model = new GbCountyListViewModel
        {
            Rows = listResult.Rows,
            Search = normalizedSearch,
            StatusFilter = normalizedStatus,
            Page = listResult.Page,
            PageSize = listResult.PageSize,
            TotalCount = listResult.TotalCount,
            TotalPages = listResult.TotalPages
        };
        return View(model);
    }

    [HttpGet("/admin/data-lists/counties-gb/add")]
    public IActionResult AddCountyGb(
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50)
    {
        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(new GbCountyCreateModel { IsActive = true });
    }

    [HttpPost("/admin/data-lists/counties-gb/add")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> AddCountyGb(
        GbCountyCreateModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        try
        {
            await gbLocationDataListService.AddCountyAsync(model, ct);
            TempData["Status"] = $"Added county '{model.Name}'.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Add county failed: {ex.Message}";
        }

        return RedirectToAction(nameof(CountiesGb), BuildCountyListRouteValues(status, q, page, pageSize));
    }

    [HttpGet("/admin/data-lists/counties-gb/edit")]
    public async Task<IActionResult> EditCountyGb(
        [FromQuery] long countyId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (countyId <= 0)
            return BadRequest("County ID is required.");

        var model = await gbLocationDataListService.GetCountyByIdAsync(countyId, ct);
        if (model is null)
            return NotFound();

        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(model);
    }

    [HttpPost("/admin/data-lists/counties-gb/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveCountyGbEdit(
        GbCountyEditModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (model.CountyId <= 0)
            return BadRequest("County ID is required.");
        if (string.IsNullOrWhiteSpace(model.Name))
            ModelState.AddModelError(nameof(model.Name), "Name is required.");

        if (!ModelState.IsValid)
        {
            ViewBag.ReturnQ = (q ?? string.Empty).Trim();
            ViewBag.ReturnStatus = NormalizeActiveFilter(status);
            ViewBag.ReturnPage = Math.Max(1, page);
            ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
            return View("EditCountyGb", model);
        }

        try
        {
            var touched = await gbLocationDataListService.UpdateCountyAsync(model, ct);
            TempData["Status"] = touched
                ? $"Updated county '{model.Name}'."
                : "County was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Update county failed: {ex.Message}";
        }
        return RedirectToAction(nameof(CountiesGb), BuildCountyListRouteValues(status, q, page, pageSize));
    }

    [HttpPost("/admin/data-lists/counties-gb/mark-inactive")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> MarkCountyGbInactive(
        [FromForm] long countyId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (countyId <= 0)
            return BadRequest("County ID is required.");

        var marked = await gbLocationDataListService.MarkCountyInactiveAsync(countyId, ct);
        TempData["Status"] = marked
            ? "County marked as inactive."
            : "County was not found.";
        return RedirectToAction(nameof(CountiesGb), BuildCountyListRouteValues(status, q, page, pageSize));
    }

    [HttpGet("/admin/data-lists/counties-gb/view")]
    public async Task<IActionResult> ViewCountyGb(
        [FromQuery] long countyId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (countyId <= 0)
            return BadRequest("County ID is required.");

        var county = await gbLocationDataListService.GetCountyByIdAsync(countyId, ct);
        if (county is null)
            return NotFound();

        var towns = await gbLocationDataListService.GetTownLookupByCountyAsync(countyId, includeInactive: true, ct);
        var runs = await gbLocationDataListService.GetRunsByCountyAsync(countyId, ct);
        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);

        return View(new GbCountyDetailsViewModel
        {
            County = county,
            Towns = towns,
            Runs = runs
        });
    }

    [HttpGet("/admin/data-lists/counties-gb/sort")]
    public async Task<IActionResult> SortCountiesGb(CancellationToken ct)
    {
        var rows = await gbLocationDataListService.GetCountiesForSortAsync(ct);
        return View(new GbCountySortViewModel { Rows = rows });
    }

    [HttpPost("/admin/data-lists/counties-gb/sort")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveCountySortOrder([FromForm] string order, CancellationToken ct)
    {
        var orderedIds = ParseOrderedIds(order);
        if (orderedIds.Count == 0)
        {
            TempData["Status"] = "Sort save failed: no county order was submitted.";
            return RedirectToAction(nameof(SortCountiesGb));
        }

        await gbLocationDataListService.SaveCountySortOrderAsync(orderedIds, ct);
        TempData["Status"] = "County sort order saved.";
        return RedirectToAction(nameof(CountiesGb));
    }

    [HttpGet("/admin/data-lists/towns-gb")]
    public async Task<IActionResult> TownsGb([FromQuery] string? q, [FromQuery] string? status, [FromQuery] long? countyId, [FromQuery] int page = 1, [FromQuery] int pageSize = 50, CancellationToken ct = default)
    {
        var normalizedStatus = NormalizeActiveFilter(status);
        var normalizedSearch = (q ?? string.Empty).Trim();
        var normalizedCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = Math.Clamp(pageSize, 10, 200);

        var countyOptions = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
        var listResult = await gbLocationDataListService.GetTownsPagedAsync(normalizedStatus, normalizedSearch, normalizedCountyId, normalizedPage, normalizedPageSize, ct);
        var model = new GbTownListViewModel
        {
            Rows = listResult.Rows,
            CountyOptions = countyOptions,
            Search = normalizedSearch,
            StatusFilter = normalizedStatus,
            CountyIdFilter = normalizedCountyId,
            Page = listResult.Page,
            PageSize = listResult.PageSize,
            TotalCount = listResult.TotalCount,
            TotalPages = listResult.TotalPages
        };
        return View(model);
    }

    [HttpGet("/admin/data-lists/towns-gb/add")]
    public async Task<IActionResult> AddTownGb(
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        var counties = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
        var normalizedCountyId = countyId.HasValue && countyId.Value > 0 ? countyId.Value : 0;
        if (normalizedCountyId <= 0 || counties.All(x => x.CountyId != normalizedCountyId))
            normalizedCountyId = counties.FirstOrDefault(x => x.IsActive)?.CountyId ?? counties.FirstOrDefault()?.CountyId ?? 0;

        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);

        var model = new GbTownCreateModel
        {
            CountyId = normalizedCountyId,
            IsActive = true,
            CountyOptions = counties
        };
        return View(model);
    }

    [HttpPost("/admin/data-lists/towns-gb/add")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> AddTownGb(
        [FromForm] GbTownCreateModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        try
        {
            await gbLocationDataListService.AddTownAsync(model, ct);
            TempData["Status"] = $"Added town '{model.Name}'.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Add town failed: {ex.Message}";
        }

        return RedirectToAction(nameof(TownsGb), BuildTownListRouteValues(status, q, countyId, page, pageSize));
    }

    [HttpGet("/admin/data-lists/towns-gb/edit")]
    public async Task<IActionResult> EditTownGb(
        [FromQuery] long townId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (townId <= 0)
            return BadRequest("Town ID is required.");

        var town = await gbLocationDataListService.GetTownByIdAsync(townId, ct);
        if (town is null)
            return NotFound();

        var counties = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
        town.CountyOptions = counties;

        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(town);
    }

    [HttpGet("/admin/data-lists/towns-gb/view")]
    public async Task<IActionResult> ViewTownGb(
        [FromQuery] long townId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (townId <= 0)
            return BadRequest("Town ID is required.");

        var town = await gbLocationDataListService.GetTownByIdAsync(townId, ct);
        if (town is null)
            return NotFound();

        var counties = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
        var countyName = counties.FirstOrDefault(x => x.CountyId == town.CountyId)?.Name ?? "Unknown";
        var runs = await gbLocationDataListService.GetRunsByTownAsync(townId, ct);
        ViewBag.ReturnQ = (q ?? string.Empty).Trim();
        ViewBag.ReturnStatus = NormalizeActiveFilter(status);
        ViewBag.ReturnCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
        ViewBag.ReturnPage = Math.Max(1, page);
        ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
        return View(new GbTownDetailsViewModel
        {
            Town = town,
            CountyName = countyName,
            Runs = runs
        });
    }

    [HttpGet("/admin/data-lists/towns-gb/sort")]
    public async Task<IActionResult> SortTownsGb([FromQuery] long? countyId, CancellationToken ct = default)
    {
        var countyOptions = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
        var selectedCountyId = countyId.HasValue && countyId.Value > 0 ? countyId.Value : 0;
        if (selectedCountyId <= 0 || countyOptions.All(x => x.CountyId != selectedCountyId))
            selectedCountyId = countyOptions.FirstOrDefault(x => x.IsActive)?.CountyId ?? countyOptions.FirstOrDefault()?.CountyId ?? 0;

        var rows = selectedCountyId > 0
            ? await gbLocationDataListService.GetTownsForSortAsync(selectedCountyId, ct)
            : [];

        return View(new GbTownSortViewModel
        {
            CountyOptions = countyOptions,
            SelectedCountyId = selectedCountyId <= 0 ? null : selectedCountyId,
            Rows = rows
        });
    }

    [HttpPost("/admin/data-lists/towns-gb/sort")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveTownSortOrder([FromForm] long countyId, [FromForm] string order, CancellationToken ct)
    {
        if (countyId <= 0)
        {
            TempData["Status"] = "Sort save failed: county is required.";
            return RedirectToAction(nameof(SortTownsGb));
        }

        var orderedIds = ParseOrderedIds(order);
        if (orderedIds.Count == 0)
        {
            TempData["Status"] = "Sort save failed: no town order was submitted.";
            return RedirectToAction(nameof(SortTownsGb), new { countyId });
        }

        await gbLocationDataListService.SaveTownSortOrderAsync(countyId, orderedIds, ct);
        TempData["Status"] = "Town sort order saved.";
        return RedirectToAction(nameof(TownsGb), new { countyId });
    }

    [HttpPost("/admin/data-lists/towns-gb/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SaveTownGbEdit(
        [FromForm] GbTownEditModel model,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (model.TownId <= 0)
            return BadRequest("Town ID is required.");
        if (model.CountyId <= 0)
            ModelState.AddModelError(nameof(model.CountyId), "County is required.");
        if (string.IsNullOrWhiteSpace(model.Name))
            ModelState.AddModelError(nameof(model.Name), "Name is required.");

        if (!ModelState.IsValid)
        {
            var counties = await gbLocationDataListService.GetCountyLookupAsync(includeInactive: true, ct);
            model.CountyOptions = counties;
            ViewBag.ReturnQ = (q ?? string.Empty).Trim();
            ViewBag.ReturnStatus = NormalizeActiveFilter(status);
            ViewBag.ReturnCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
            ViewBag.ReturnPage = Math.Max(1, page);
            ViewBag.ReturnPageSize = Math.Clamp(pageSize, 10, 200);
            return View("EditTownGb", model);
        }

        try
        {
            var touched = await gbLocationDataListService.UpdateTownAsync(model, ct);
            TempData["Status"] = touched
                ? $"Updated town '{model.Name}'."
                : "Town was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Update town failed: {ex.Message}";
        }

        return RedirectToAction(nameof(TownsGb), BuildTownListRouteValues(status, q, countyId, page, pageSize));
    }

    [HttpPost("/admin/data-lists/towns-gb/mark-inactive")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> MarkTownGbInactive(
        [FromForm] long townId,
        [FromQuery] string? q,
        [FromQuery] string? status,
        [FromQuery] long? countyId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        CancellationToken ct = default)
    {
        if (townId <= 0)
            return BadRequest("Town ID is required.");

        var marked = await gbLocationDataListService.MarkTownInactiveAsync(townId, ct);
        TempData["Status"] = marked
            ? "Town marked as inactive."
            : "Town was not found.";
        return RedirectToAction(nameof(TownsGb), BuildTownListRouteValues(status, q, countyId, page, pageSize));
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

    private static string NormalizeCategoryStatusFilter(string? status)
    {
        if (string.Equals(status, "inactive", StringComparison.OrdinalIgnoreCase))
            return "inactive";
        if (string.Equals(status, "all", StringComparison.OrdinalIgnoreCase))
            return "all";
        return "active";
    }

    private static string NormalizeActiveFilter(string? status)
    {
        if (string.Equals(status, "inactive", StringComparison.OrdinalIgnoreCase))
            return "inactive";
        if (string.Equals(status, "all", StringComparison.OrdinalIgnoreCase))
            return "all";
        return "active";
    }

    private static object BuildCategoryListRouteValues(string? status, string? q, int page, int pageSize)
    {
        var normalizedStatus = NormalizeCategoryStatusFilter(status);
        var normalizedSearch = string.IsNullOrWhiteSpace(q) ? null : q.Trim();
        return new
        {
            status = normalizedStatus,
            q = normalizedSearch,
            page = Math.Max(1, page),
            pageSize = Math.Clamp(pageSize, 10, 200)
        };
    }

    private static object BuildCountyListRouteValues(string? status, string? q, int page, int pageSize)
    {
        var normalizedStatus = NormalizeActiveFilter(status);
        var normalizedSearch = string.IsNullOrWhiteSpace(q) ? null : q.Trim();
        return new
        {
            status = normalizedStatus,
            q = normalizedSearch,
            page = Math.Max(1, page),
            pageSize = Math.Clamp(pageSize, 10, 200)
        };
    }

    private static object BuildTownListRouteValues(string? status, string? q, long? countyId, int page, int pageSize)
    {
        var normalizedStatus = NormalizeActiveFilter(status);
        var normalizedSearch = string.IsNullOrWhiteSpace(q) ? null : q.Trim();
        var normalizedCountyId = countyId.HasValue && countyId.Value > 0 ? countyId : null;
        return new
        {
            status = normalizedStatus,
            q = normalizedSearch,
            countyId = normalizedCountyId,
            page = Math.Max(1, page),
            pageSize = Math.Clamp(pageSize, 10, 200)
        };
    }

    private string GetCurrentUserIdentityKey()
    {
        var email = User.FindFirstValue(ClaimTypes.Email);
        if (!string.IsNullOrWhiteSpace(email))
            return email;
        if (!string.IsNullOrWhiteSpace(User.Identity?.Name))
            return User.Identity.Name;
        return "unknown";
    }

    private static List<long> ParseOrderedIds(string order)
    {
        return (order ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(x => long.TryParse(x, out var id) ? id : 0L)
            .Where(id => id > 0)
            .Distinct()
            .ToList();
    }
}
