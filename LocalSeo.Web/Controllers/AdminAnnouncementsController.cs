using System.Security.Claims;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminAnnouncementsController(
    IAnnouncementService announcementService) : Controller
{
    [HttpGet("/admin/announcements")]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        var rows = await announcementService.GetAdminListAsync(ct);
        return View(new AnnouncementAdminListViewModel
        {
            Rows = rows
        });
    }

    [HttpGet("/admin/announcements/create")]
    public IActionResult Create()
    {
        return View(new AnnouncementAdminEditViewModel
        {
            Mode = "create",
            Announcement = new AnnouncementEditModel()
        });
    }

    [HttpPost("/admin/announcements/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreatePost([FromForm] AnnouncementEditModel model, CancellationToken ct)
    {
        var result = await announcementService.CreateAsync(model, GetCurrentUserId(), ct);
        if (!result.Success || !result.AnnouncementId.HasValue)
        {
            return View("Create", new AnnouncementAdminEditViewModel
            {
                Mode = "create",
                Message = result.Message,
                Announcement = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(View), new { id = result.AnnouncementId.Value });
    }

    [HttpGet("/admin/announcements/{id:long}")]
    public async Task<IActionResult> View(long id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var model = await announcementService.GetAdminDetailAsync(id, ct);
        if (model is null)
            return NotFound();
        return base.View(model);
    }

    [HttpGet("/admin/announcements/{id:long}/edit")]
    public async Task<IActionResult> Edit(long id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var model = await announcementService.GetAdminEditModelAsync(id, ct);
        if (model is null)
            return NotFound();

        return View(new AnnouncementAdminEditViewModel
        {
            Mode = "edit",
            Announcement = model
        });
    }

    [HttpPost("/admin/announcements/{id:long}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost(long id, [FromForm] AnnouncementEditModel model, CancellationToken ct)
    {
        model.AnnouncementId = id;
        var result = await announcementService.UpdateAsync(id, model, GetCurrentUserId(), ct);
        if (!result.Success)
        {
            return View("Edit", new AnnouncementAdminEditViewModel
            {
                Mode = "edit",
                Message = result.Message,
                Announcement = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(View), new { id });
    }

    [HttpPost("/admin/announcements/{id:long}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Delete(long id, CancellationToken ct)
    {
        var result = await announcementService.SoftDeleteAsync(id, GetCurrentUserId(), ct);
        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Index));
    }

    private int? GetCurrentUserId()
    {
        var raw = User.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(raw, out var id) && id > 0 ? id : null;
    }
}
