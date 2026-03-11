using LocalSeo.Web.Services;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class RunsController(
    ISearchIngestionService ingestionService,
    ISeoAuditService seoAuditService) : Controller
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

    [HttpPost("/runs/{id:long}/actions/recalculate-audits")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RecalculateAudits(long id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var run = await ingestionService.GetRunAsync(id, ct);
        if (run is null)
            return NotFound();

        var snapshots = await ingestionService.GetRunSnapshotsAsync(id, ct);
        var placeIds = snapshots
            .Select(x => x.PlaceId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (placeIds.Count == 0)
        {
            TempData["Status"] = $"Run {id} has no places to recalculate.";
            return RedirectToAction(nameof(Details), new { id });
        }

        var evaluated = await seoAuditService.EvaluatePlacesAsync(placeIds!, ct);
        TempData["Status"] = $"Recalculated audits for {evaluated} place(s) in run {id}.";
        return RedirectToAction(nameof(Details), new { id });
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
