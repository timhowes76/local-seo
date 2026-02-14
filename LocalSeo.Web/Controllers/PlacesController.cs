using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class PlacesController(ISearchIngestionService ingestionService, IReviewVelocityService reviewVelocityService) : Controller
{
    [HttpGet("/places")]
    public async Task<IActionResult> Index([FromQuery] string? sort, [FromQuery] string? direction, CancellationToken ct)
    {
        ViewBag.Sort = sort;
        ViewBag.Direction = direction;
        var rows = await reviewVelocityService.GetPlaceVelocityListAsync(sort, direction, ct);
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
}
