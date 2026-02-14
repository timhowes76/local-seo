using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public class PlacesController(ISearchIngestionService ingestionService) : Controller
{
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
