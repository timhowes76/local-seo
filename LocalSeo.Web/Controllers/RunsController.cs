using LocalSeo.Web.Services;
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
        ViewBag.RunId = id;
        return View(await ingestionService.GetRunSnapshotsAsync(id, ct));
    }
}
