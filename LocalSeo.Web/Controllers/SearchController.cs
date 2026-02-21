using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class SearchController(
    ISearchIngestionService ingestionService,
    ISearchRunExecutor searchRunExecutor,
    IGoogleBusinessProfileCategoryService googleBusinessProfileCategoryService,
    IGbLocationDataListService gbLocationDataListService,
    ICategoryLocationKeywordService categoryLocationKeywordService,
    IOptions<PlacesOptions> placesOptions) : Controller
{
    [HttpGet("/search")]
    public async Task<IActionResult> Index(long? rerunId, string? categoryId, long? countyId, long? townId, CancellationToken ct)
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
                model.CategoryId = run.CategoryId;
                model.CountyId = run.CountyId;
                model.TownId = run.TownId;
                model.RadiusMeters = run.RadiusMeters ?? placesOptions.Value.DefaultRadiusMeters;
                model.ResultLimit = run.ResultLimit;
                model.FetchEnhancedGoogleData = run.FetchDetailedData;
                model.FetchGoogleReviews = run.FetchGoogleReviews;
                model.FetchGoogleUpdates = run.FetchGoogleUpdates;
                model.FetchGoogleQuestionsAndAnswers = run.FetchGoogleQuestionsAndAnswers;
                model.FetchGoogleSocialProfiles = run.FetchGoogleSocialProfiles;
                model.RerunSourceRunId = run.SearchRunId;
            }
            else
            {
                TempData["Status"] = $"Run #{rerunId.Value} was not found. Defaults loaded.";
            }
        }
        else
        {
            if (!string.IsNullOrWhiteSpace(categoryId))
                model.CategoryId = categoryId.Trim();
            if (countyId.HasValue && countyId.Value > 0)
                model.CountyId = countyId.Value;
            if (townId.HasValue && townId.Value > 0)
                model.TownId = townId.Value;
        }

        await PopulateOptionsAsync(model, ct, includeInactiveSelections: true);
        await LoadKeyphrasesAsync(model, ct);
        return View(model);
    }

    [HttpGet("/search/towns")]
    public async Task<IActionResult> GetTowns([FromQuery] long countyId, CancellationToken ct)
    {
        var rows = await gbLocationDataListService.GetTownLookupByCountyAsync(countyId, includeInactive: false, ct);
        return Json(rows.Select(x => new { x.TownId, x.Name }));
    }

    [HttpPost("/search")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Run(SearchFormModel model, CancellationToken ct)
    {
        model.ResultLimit = Math.Clamp(model.ResultLimit, 1, 20);
        await PopulateOptionsAsync(model, ct, includeInactiveSelections: false);
        await LoadKeyphrasesAsync(model, ct);

        if (string.IsNullOrWhiteSpace(model.CategoryId))
            ModelState.AddModelError(nameof(model.CategoryId), "Category is required.");
        if (!model.CountyId.HasValue || model.CountyId.Value <= 0)
            ModelState.AddModelError(nameof(model.CountyId), "County is required.");
        if (!model.TownId.HasValue || model.TownId.Value <= 0)
            ModelState.AddModelError(nameof(model.TownId), "Town is required.");

        if (!ModelState.IsValid)
            return View("Index", model);

        try
        {
            var runId = await ingestionService.CreateQueuedRunAsync(model, ct);
            searchRunExecutor.EnsureRunning(runId);
            return RedirectToAction(nameof(Progress), new { runId });
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

    [HttpGet("/search/progress/{runId:long}")]
    public async Task<IActionResult> Progress(long runId, CancellationToken ct)
    {
        var snapshot = await ingestionService.GetRunProgressAsync(runId, ct);
        if (snapshot is null)
            return NotFound();

        if (!IsTerminalStatus(snapshot.Status))
            searchRunExecutor.EnsureRunning(runId);

        var progressStreamUrl = Url.Action(nameof(ProgressStream), "Search", new { runId }) ?? $"/search/progress-stream/{runId}";
        var completedRedirectUrl = Url.Action("Details", "Runs", new { id = runId }) ?? $"/runs/{runId}";
        var retryUrl = Url.Action(nameof(Retry), "Search", new { runId }) ?? $"/search/retry/{runId}";
        return View("Progress", new SearchProgressPageModel
        {
            RunId = runId,
            ProgressStreamUrl = progressStreamUrl,
            CompletedRedirectUrl = completedRedirectUrl,
            RetryUrl = retryUrl,
            Initial = snapshot
        });
    }

    [HttpGet("/search/progress-stream/{runId:long}")]
    public async Task ProgressStream(long runId, CancellationToken ct)
    {
        var snapshot = await ingestionService.GetRunProgressAsync(runId, ct);
        if (snapshot is null)
        {
            Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        if (!IsTerminalStatus(snapshot.Status))
            searchRunExecutor.EnsureRunning(runId);

        Response.StatusCode = StatusCodes.Status200OK;
        Response.ContentType = "text/event-stream";
        Response.Headers.CacheControl = "no-cache, no-transform";
        Response.Headers.Connection = "keep-alive";
        Response.Headers["X-Accel-Buffering"] = "no";
        HttpContext.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

        static string BuildPayload(SearchRunProgressSnapshot progress)
        {
            var percent = progress.PercentComplete;
            var total = progress.TotalApiCalls.GetValueOrDefault();
            if (!percent.HasValue && total > 0)
            {
                var completed = progress.CompletedApiCalls.GetValueOrDefault();
                percent = Math.Clamp((int)Math.Floor((completed * 100d) / total), 0, 100);
            }

            return JsonSerializer.Serialize(new
            {
                runId = progress.SearchRunId,
                status = progress.Status,
                totalApiCalls = progress.TotalApiCalls,
                completedApiCalls = progress.CompletedApiCalls,
                percentComplete = percent,
                startedUtc = progress.StartedUtc,
                lastUpdatedUtc = progress.LastUpdatedUtc,
                completedUtc = progress.CompletedUtc,
                errorMessage = progress.ErrorMessage
            });
        }

        static async Task WriteSseDataAsync(HttpResponse response, string payload, CancellationToken cancellationToken)
        {
            await response.WriteAsync($"data: {payload}\n\n", cancellationToken);
            await response.Body.FlushAsync(cancellationToken);
        }

        var lastPayload = string.Empty;
        var lastEventUtc = DateTime.UtcNow - TimeSpan.FromSeconds(30);
        var lastKeepAliveUtc = DateTime.UtcNow;

        while (!ct.IsCancellationRequested)
        {
            var current = await ingestionService.GetRunProgressAsync(runId, ct);
            if (current is null)
            {
                await WriteSseDataAsync(Response, JsonSerializer.Serialize(new
                {
                    runId,
                    status = "Failed",
                    errorMessage = "Run not found."
                }), ct);
                break;
            }

            var payload = BuildPayload(current);
            var nowUtc = DateTime.UtcNow;
            var shouldEmitEvent = !string.Equals(lastPayload, payload, StringComparison.Ordinal)
                                  || (nowUtc - lastEventUtc) >= TimeSpan.FromSeconds(5);
            if (shouldEmitEvent)
            {
                await WriteSseDataAsync(Response, payload, ct);
                lastPayload = payload;
                lastEventUtc = nowUtc;
            }

            if ((nowUtc - lastKeepAliveUtc) >= TimeSpan.FromSeconds(12))
            {
                await Response.WriteAsync(": ping\n\n", ct);
                await Response.Body.FlushAsync(ct);
                lastKeepAliveUtc = nowUtc;
            }

            if (IsTerminalStatus(current.Status))
                break;

            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }

    [HttpPost("/search/retry/{runId:long}")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Retry(long runId, CancellationToken ct)
    {
        var existingRun = await ingestionService.GetRunAsync(runId, ct);
        if (existingRun is null)
            return NotFound();

        var queuedRunId = await ingestionService.CreateQueuedRunAsync(new SearchFormModel
        {
            CategoryId = existingRun.CategoryId,
            CountyId = existingRun.CountyId,
            TownId = existingRun.TownId,
            RadiusMeters = existingRun.RadiusMeters ?? placesOptions.Value.DefaultRadiusMeters,
            ResultLimit = existingRun.ResultLimit,
            FetchEnhancedGoogleData = existingRun.FetchDetailedData,
            FetchGoogleReviews = existingRun.FetchGoogleReviews,
            FetchGoogleUpdates = existingRun.FetchGoogleUpdates,
            FetchGoogleQuestionsAndAnswers = existingRun.FetchGoogleQuestionsAndAnswers,
            FetchGoogleSocialProfiles = existingRun.FetchGoogleSocialProfiles
        }, ct);

        searchRunExecutor.EnsureRunning(queuedRunId);
        return RedirectToAction(nameof(Progress), new { runId = queuedRunId });
    }

    [HttpPost("/search/keyphrases/add")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> AddKeyphrase(
        [FromForm] long countyId,
        [FromForm] long townId,
        [FromForm] string categoryId,
        [FromForm] CategoryLocationKeywordCreateModel model,
        CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.AddKeywordAndRefreshAsync(townId, categoryId, model, ct);
            TempData["Status"] = $"Keyphrase added. Refreshed: {summary.RefreshedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Add keyphrase failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Index), new
        {
            categoryId,
            countyId,
            townId
        });
    }

    [HttpPost("/search/keyphrases/refresh-eligible")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshEligibleKeyphrases(
        [FromForm] long countyId,
        [FromForm] long townId,
        [FromForm] string categoryId,
        CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.RefreshEligibleKeywordsAsync(townId, categoryId, ct);
            TempData["Status"] = $"Refresh completed. Requested: {summary.RequestedCount}, Refreshed: {summary.RefreshedCount}, Skipped: {summary.SkippedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Refresh failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Index), new
        {
            categoryId,
            countyId,
            townId
        });
    }

    [HttpPost("/search/keyphrases/{keywordId:int}/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshKeyphrase(
        int keywordId,
        [FromForm] long countyId,
        [FromForm] long townId,
        [FromForm] string categoryId,
        CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.RefreshKeywordAsync(townId, categoryId, keywordId, ct);
            TempData["Status"] = $"Refresh completed. Refreshed: {summary.RefreshedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Refresh failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Index), new
        {
            categoryId,
            countyId,
            townId
        });
    }

    [HttpPost("/search/keyphrases/{keywordId:int}/set-type")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetKeyphraseType(
        int keywordId,
        [FromForm] int keywordType,
        [FromForm] long countyId,
        [FromForm] long townId,
        [FromForm] string categoryId,
        CancellationToken ct)
    {
        try
        {
            var changed = await categoryLocationKeywordService.SetKeywordTypeAsync(townId, categoryId, keywordId, keywordType, ct);
            TempData["Status"] = changed ? "Keyword type updated." : "Keyword was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Set keyword type failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Index), new
        {
            categoryId,
            countyId,
            townId
        });
    }

    [HttpPost("/search/keyphrases/{keywordId:int}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteKeyphrase(
        int keywordId,
        [FromForm] long countyId,
        [FromForm] long townId,
        [FromForm] string categoryId,
        CancellationToken ct)
    {
        try
        {
            var changed = await categoryLocationKeywordService.DeleteKeywordAsync(townId, categoryId, keywordId, ct);
            TempData["Status"] = changed ? "Keyword deleted." : "Keyword was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Delete failed: {ex.Message}";
        }

        return RedirectToAction(nameof(Index), new
        {
            categoryId,
            countyId,
            townId
        });
    }

    private async Task PopulateOptionsAsync(SearchFormModel model, CancellationToken ct, bool includeInactiveSelections)
    {
        var categories = await googleBusinessProfileCategoryService.GetActiveLookupAsync("GB", "en-GB", ct);
        model.CategoryOptions = categories;
        if (string.IsNullOrWhiteSpace(model.CategoryId))
            model.CategoryId = categories.FirstOrDefault()?.CategoryId ?? string.Empty;

        var counties = await gbLocationDataListService.GetCountyLookupAsync(includeInactiveSelections, ct);
        model.CountyOptions = counties;

        if (!model.CountyId.HasValue || model.CountyId.Value <= 0 || counties.All(x => x.CountyId != model.CountyId.Value))
            model.CountyId = counties.FirstOrDefault(x => x.IsActive)?.CountyId ?? counties.FirstOrDefault()?.CountyId;

        var towns = model.CountyId.HasValue && model.CountyId.Value > 0
            ? await gbLocationDataListService.GetTownLookupByCountyAsync(model.CountyId.Value, includeInactiveSelections, ct)
            : [];
        model.TownOptions = towns;

        if (!model.TownId.HasValue || model.TownId.Value <= 0 || towns.All(x => x.TownId != model.TownId.Value))
            model.TownId = towns.FirstOrDefault(x => x.IsActive)?.TownId ?? towns.FirstOrDefault()?.TownId;
    }

    private async Task LoadKeyphrasesAsync(SearchFormModel model, CancellationToken ct)
    {
        model.Keyphrases = null;
        model.KeyphrasesError = null;
        if (string.IsNullOrWhiteSpace(model.CategoryId) || !model.TownId.HasValue || model.TownId.Value <= 0)
            return;

        try
        {
            model.Keyphrases = await categoryLocationKeywordService.GetKeyphrasesAsync(model.TownId.Value, model.CategoryId, ct);
        }
        catch (Exception ex)
        {
            model.KeyphrasesError = ex.Message;
        }
    }

    private static bool IsTerminalStatus(string? status)
    {
        if (string.IsNullOrWhiteSpace(status))
            return false;
        return status.Equals("Completed", StringComparison.OrdinalIgnoreCase)
               || status.Equals("Failed", StringComparison.OrdinalIgnoreCase);
    }
}
