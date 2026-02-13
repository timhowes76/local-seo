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
    public IActionResult Index()
    {
        return View(new SearchFormModel
        {
            RadiusMeters = placesOptions.Value.DefaultRadiusMeters,
            ResultLimit = placesOptions.Value.DefaultResultLimit
        });
    }

    [HttpPost("/search")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Run(SearchFormModel model, CancellationToken ct)
    {
        model.ResultLimit = Math.Clamp(model.ResultLimit, 1, 20);
        var runId = await ingestionService.RunAsync(model, ct);
        TempData["Status"] = $"Search complete. Run #{runId}";
        return RedirectToAction("Index", new { runId });
    }
}
