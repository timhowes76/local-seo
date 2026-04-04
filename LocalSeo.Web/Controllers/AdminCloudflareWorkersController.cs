using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminCloudflareWorkersController(
    ICloudflareWorkerService cloudflareWorkerService) : Controller
{
    private const string SchemaUnavailableMessage = "Cloudflare worker settings schema is not available yet. Run the homepage analysis migration or startup schema bootstrap first.";

    [HttpGet("/admin/settings/cloudflare-workers")]
    public async Task<IActionResult> Index([FromQuery] string? search, CancellationToken ct)
    {
        var isAvailable = await cloudflareWorkerService.IsAvailableAsync(ct);
        var rows = await cloudflareWorkerService.GetListAsync(search, ct);
        return View(new CloudflareWorkerListViewModel
        {
            Search = search,
            Message = isAvailable ? null : SchemaUnavailableMessage,
            Rows = rows
        });
    }

    [HttpGet("/admin/settings/cloudflare-workers/create")]
    public async Task<IActionResult> Create(CancellationToken ct)
    {
        if (!await cloudflareWorkerService.IsAvailableAsync(ct))
        {
            TempData["Status"] = SchemaUnavailableMessage;
            return RedirectToAction(nameof(Index));
        }

        return View(new CloudflareWorkerEditViewModel
        {
            Mode = "create",
            Worker = new CloudflareWorkerEditModel
            {
                IsEnabled = true,
                TimeoutSeconds = 30,
                RoutePath = "/",
                DisplayOrder = 0
            }
        });
    }

    [HttpPost("/admin/settings/cloudflare-workers/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreatePost([Bind(Prefix = "Worker")][FromForm] CloudflareWorkerEditModel model, CancellationToken ct)
    {
        var result = await cloudflareWorkerService.CreateAsync(model, ct);
        if (!result.Success || !result.CloudflareWorkerId.HasValue)
        {
            return View("Create", new CloudflareWorkerEditViewModel
            {
                Mode = "create",
                Message = result.Message,
                Worker = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id = result.CloudflareWorkerId.Value });
    }

    [HttpGet("/admin/settings/cloudflare-workers/{id:int}/edit")]
    public async Task<IActionResult> Edit(int id, CancellationToken ct)
    {
        if (!await cloudflareWorkerService.IsAvailableAsync(ct))
        {
            TempData["Status"] = SchemaUnavailableMessage;
            return RedirectToAction(nameof(Index));
        }

        var worker = await cloudflareWorkerService.GetEditModelAsync(id, ct);
        if (worker is null)
            return NotFound();

        return View(new CloudflareWorkerEditViewModel
        {
            Mode = "edit",
            Worker = worker
        });
    }

    [HttpPost("/admin/settings/cloudflare-workers/{id:int}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost(int id, [Bind(Prefix = "Worker")][FromForm] CloudflareWorkerEditModel model, CancellationToken ct)
    {
        model.CloudflareWorkerId = id;
        var result = await cloudflareWorkerService.UpdateAsync(id, model, ct);
        if (!result.Success)
        {
            return View("Edit", new CloudflareWorkerEditViewModel
            {
                Mode = "edit",
                Message = result.Message,
                Worker = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id });
    }
}
