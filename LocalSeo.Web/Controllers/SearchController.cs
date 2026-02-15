using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class SearchController(ISearchIngestionService ingestionService, IOptions<PlacesOptions> placesOptions) : Controller
{
    [HttpGet("/search")]
    public async Task<IActionResult> Index(long? rerunId, CancellationToken ct)
    {
        var model = new SearchFormModel
        {
            RadiusMeters = placesOptions.Value.DefaultRadiusMeters,
            ResultLimit = placesOptions.Value.DefaultResultLimit
        };

        if (rerunId.HasValue)
        {
            var run = await ingestionService.GetRunAsync(rerunId.Value, ct);
            if (run is not null)
            {
                model.SeedKeyword = run.SeedKeyword;
                model.LocationName = run.LocationName;
                model.RadiusMeters = run.RadiusMeters ?? placesOptions.Value.DefaultRadiusMeters;
                model.ResultLimit = run.ResultLimit;
                model.FetchEnhancedGoogleData = run.FetchDetailedData;
                model.FetchGoogleReviews = run.FetchGoogleReviews;
                model.FetchGoogleUpdates = run.FetchGoogleUpdates;
                model.FetchGoogleQuestionsAndAnswers = run.FetchGoogleQuestionsAndAnswers;
                model.CenterLat = run.CenterLat;
                model.CenterLng = run.CenterLng;
                model.RerunSourceRunId = run.SearchRunId;
            }
            else
            {
                TempData["Status"] = $"Run #{rerunId.Value} was not found. Defaults loaded.";
            }
        }

        return View(model);
    }

    [HttpPost("/search")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Run(SearchFormModel model, CancellationToken ct)
    {
        model.ResultLimit = Math.Clamp(model.ResultLimit, 1, 20);

        try
        {
            var runId = await ingestionService.RunAsync(model, ct);
            return RedirectToAction("Details", "Runs", new { id = runId });
        }
        catch (InvalidOperationException ex)
        {
            ModelState.AddModelError(string.Empty, ex.Message);
            return View("Index", model);
        }
        catch (HttpRequestException ex)
        {
            ModelState.AddModelError(string.Empty, $"Google request failed: {ex.Message}");
            return View("Index", model);
        }
    }
}
