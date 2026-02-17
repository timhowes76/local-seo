using LocalSeo.Web.Services;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class RunsController(ISearchIngestionService ingestionService) : Controller
{
    [HttpGet("/runs")]
    public async Task<IActionResult> Index(CancellationToken ct)
        => View(await ingestionService.GetLatestRunsAsync(20, ct));

    [HttpGet("/runs/{id:long}")]
    public async Task<IActionResult> Details(long id, CancellationToken ct)
    {
        var run = await ingestionService.GetRunAsync(id, ct);
        if (run is null)
            return NotFound();

        var snapshots = await ingestionService.GetRunSnapshotsAsync(id, ct);
        var keyphraseTraffic = await ingestionService.GetRunKeyphraseTrafficSummaryAsync(id, ct);
        var taskProgress = await ingestionService.GetRunTaskProgressAsync(run, ct);
        return View(new RunDetailsViewModel(run, snapshots, taskProgress)
        {
            KeyphraseTraffic = keyphraseTraffic
        });
    }

    [HttpGet("/runs/{id:long}/compare-reviews")]
    public async Task<IActionResult> CompareReviews(long id, CancellationToken ct)
    {
        var model = await ingestionService.GetRunReviewComparisonAsync(id, ct);
        if (model is null)
            return NotFound();

        return View(model);
    }
}
