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
    IReviewsProviderResolver reviewsProviderResolver) : Controller
{
    [HttpGet("/places")]
    public async Task<IActionResult> Index([FromQuery] string? sort, [FromQuery] string? direction, [FromQuery] string? keyword, [FromQuery] string? location, CancellationToken ct)
    {
        ViewBag.Sort = sort;
        ViewBag.Direction = direction;
        ViewBag.Keyword = keyword;
        ViewBag.Location = location;
        var rows = await reviewVelocityService.GetPlaceVelocityListAsync(sort, direction, keyword, location, ct);
        return View(rows);
    }

    [HttpGet("/places/{id}")]
    public async Task<IActionResult> Details(string id, long? runId, CancellationToken ct)
    {
        var model = await ingestionService.GetPlaceDetailsAsync(id, runId, ct);
        if (model is null)
            return NotFound();

        ViewBag.RequestedRunId = runId;
        return View(model);
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
            ct);

        var typeLabel = normalizedTaskType switch
        {
            "my_business_info" => "Google Enhanced Data",
            "my_business_updates" => "Google Updates",
            "questions_and_answers" => "Google Question & Answers",
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
        if (string.Equals(taskType, "reviews", StringComparison.OrdinalIgnoreCase))
            return "reviews";
        return null;
    }
}
