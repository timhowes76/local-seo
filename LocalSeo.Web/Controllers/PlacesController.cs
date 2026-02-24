using LocalSeo.Web.Services;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class PlacesController(
    ISearchIngestionService ingestionService,
    IReviewVelocityService reviewVelocityService,
    IDataForSeoTaskTracker dataForSeoTaskTracker,
    IReviewsProviderResolver reviewsProviderResolver,
    IZohoLeadSyncService zohoLeadSyncService,
    IReportsService reportsService) : Controller
{
    private static readonly int[] AllowedTakeOptions = [25, 50, 100, 500, 1000];

    [HttpGet("/places")]
    public async Task<IActionResult> Index([FromQuery] string? sort, [FromQuery] string? direction, [FromQuery] string? placeName, [FromQuery] string? keyword, [FromQuery] string? location, [FromQuery] int? take, CancellationToken ct)
    {
        var normalizedTake = NormalizeTake(take);
        var rows = await reviewVelocityService.GetPlaceVelocityListAsync(sort, direction, placeName, keyword, location, normalizedTake, ct);
        var filters = await reviewVelocityService.GetRunFilterOptionsAsync(ct);
        var model = new PlacesIndexViewModel
        {
            Rows = rows,
            KeywordOptions = filters.Keywords,
            LocationOptions = filters.Locations,
            SelectedTake = normalizedTake,
            SelectedKeyword = keyword,
            SelectedLocation = location,
            PlaceNameQuery = placeName,
            Sort = sort,
            Direction = direction
        };
        return View(model);
    }

    private static int NormalizeTake(int? value)
    {
        if (!value.HasValue)
            return 100;

        return AllowedTakeOptions.Contains(value.Value) ? value.Value : 100;
    }

    [HttpGet("/places/{id}")]
    public async Task<IActionResult> Details(string id, long? runId, [FromQuery] int reviewPage = 1, CancellationToken ct = default)
    {
        var model = await ingestionService.GetPlaceDetailsAsync(id, runId, ct, reviewPage);
        if (model is null)
            return NotFound();

        var reportRunId = runId ?? model.ActiveRunId;
        if (reportRunId.HasValue && reportRunId.Value > 0)
        {
            model.FirstContactReportAvailability = await reportsService.GetFirstContactAvailabilityAsync(id, reportRunId.Value, ct);
        }
        else
        {
            model.FirstContactReportAvailability = new FirstContactReportAvailability
            {
                PlaceId = id,
                RunId = 0,
                IsAvailable = false,
                Message = "Open this place from a run to generate reports.",
                BusinessName = model.DisplayName
            };
        }

        ViewBag.RequestedRunId = runId;
        return View(model);
    }

    [HttpGet("/places/{id}/edit")]
    public async Task<IActionResult> Edit(string id, [FromQuery] long? runId, CancellationToken ct = default)
    {
        var model = await ingestionService.GetPlaceSocialLinksForEditAsync(id, runId, ct);
        if (model is null)
            return NotFound();

        return View(model);
    }

    [HttpPost("/places/{id}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(string id, [FromQuery] long? runId, PlaceSocialLinksEditModel model, CancellationToken ct = default)
    {
        model.PlaceId = id;
        model.RunId = runId;

        try
        {
            var updated = await ingestionService.UpdatePlaceSocialLinksAsync(model, ct);
            if (!updated)
                return NotFound();

            TempData["Status"] = "Social media links updated.";
            return RedirectToAction(nameof(Details), new { id, runId });
        }
        catch (InvalidOperationException ex)
        {
            ModelState.AddModelError(string.Empty, ex.Message);
            var existing = await ingestionService.GetPlaceSocialLinksForEditAsync(id, runId, ct);
            if (existing is not null && string.IsNullOrWhiteSpace(model.DisplayName))
                model.DisplayName = existing.DisplayName;
            return View(model);
        }
    }

    [HttpPost("/places/{id}/zoho/create-lead")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreateZohoLead(string id, [FromQuery] long? runId, CancellationToken ct)
    {
        try
        {
            var place = await ingestionService.GetPlaceDetailsAsync(id, runId, ct);
            if (place is null)
                return NotFound();

            var localSeoLink = $"{Request.Scheme}://{Request.Host}/places/{Uri.EscapeDataString(id)}";
            var result = await zohoLeadSyncService.CreateLeadForPlaceAsync(id, localSeoLink, ct);
            if (!result.Success && result.RequiresZohoTokenRefresh)
            {
                TempData["Status"] = "Something went wrong. Click here to refresh the Zoho token and try again.";
                TempData["ZohoTokenRefreshUrl"] = "/integrations/zoho/connect";
            }
            else
            {
                TempData["Status"] = result.Message;
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            TempData["Status"] = $"Zoho sync failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Details), new { id, runId });
    }

    [HttpPost("/places/{id}/data/populate")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> PopulateTaskForPlace(string id, [FromForm] long taskRowId, [FromQuery] long? runId, CancellationToken ct)
    {
        var result = await dataForSeoTaskTracker.PopulateTaskAsync(taskRowId, ct);
        TempData["Status"] = result.Success
            ? $"Populate succeeded: {result.Message}"
            : $"Populate failed: {result.Message}";

        return RedirectToAction(nameof(Details), new { id, runId });
    }

    [HttpPost("/places/{id}/data/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshTaskForPlace(string id, [FromForm] string taskType, [FromQuery] long? runId, CancellationToken ct)
    {
        var normalizedTaskType = NormalizeTaskType(taskType);
        if (normalizedTaskType is null)
        {
            TempData["Status"] = "Invalid task type for refresh.";
            return RedirectToAction(nameof(Details), new { id, runId });
        }

        var place = await ingestionService.GetPlaceDetailsAsync(id, runId, ct);
        if (place is null)
            return NotFound();

        var provider = reviewsProviderResolver.Resolve(out _);
        var fetchGoogleReviews = string.Equals(normalizedTaskType, "reviews", StringComparison.OrdinalIgnoreCase);
        var fetchMyBusinessInfo = string.Equals(normalizedTaskType, "my_business_info", StringComparison.OrdinalIgnoreCase);
        var fetchGoogleUpdates = string.Equals(normalizedTaskType, "my_business_updates", StringComparison.OrdinalIgnoreCase);
        var fetchGoogleQuestionsAndAnswers = string.Equals(normalizedTaskType, "questions_and_answers", StringComparison.OrdinalIgnoreCase);
        var fetchGoogleSocialProfiles = string.Equals(normalizedTaskType, "social_profiles", StringComparison.OrdinalIgnoreCase);

        await provider.FetchAndStoreReviewsAsync(
            id,
            place.ActiveUserRatingCount,
            place.SearchLocationName,
            place.Lat,
            place.Lng,
            null,
            fetchGoogleReviews,
            fetchMyBusinessInfo,
            fetchGoogleUpdates,
            fetchGoogleQuestionsAndAnswers,
            fetchGoogleSocialProfiles,
            ct);

        var typeLabel = normalizedTaskType switch
        {
            "my_business_info" => "Google Enhanced Data",
            "my_business_updates" => "Google Updates",
            "questions_and_answers" => "Google Question & Answers",
            "social_profiles" => "Social Profiles",
            _ => "Google Reviews"
        };
        TempData["Status"] = $"Refresh requested for {typeLabel}.";
        return RedirectToAction(nameof(Details), new { id, runId });
    }

    private static string? NormalizeTaskType(string? taskType)
    {
        if (string.IsNullOrWhiteSpace(taskType))
            return null;
        if (string.Equals(taskType, "my_business_info", StringComparison.OrdinalIgnoreCase))
            return "my_business_info";
        if (string.Equals(taskType, "my_business_updates", StringComparison.OrdinalIgnoreCase))
            return "my_business_updates";
        if (string.Equals(taskType, "questions_and_answers", StringComparison.OrdinalIgnoreCase))
            return "questions_and_answers";
        if (string.Equals(taskType, "social_profiles", StringComparison.OrdinalIgnoreCase))
            return "social_profiles";
        if (string.Equals(taskType, "reviews", StringComparison.OrdinalIgnoreCase))
            return "reviews";
        return null;
    }
}
