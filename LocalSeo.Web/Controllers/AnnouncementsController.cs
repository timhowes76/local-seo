using System.Security.Claims;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public sealed class AnnouncementsController(
    IAnnouncementService announcementService) : Controller
{
    private const int ModalFeedTake = 10;

    [HttpGet("/announcements/unread-count")]
    public async Task<IActionResult> UnreadCount(CancellationToken ct)
    {
        var userId = GetCurrentUserId();
        if (!userId.HasValue)
            return Unauthorized();

        var count = await announcementService.GetUnreadCountAsync(userId.Value, ct);
        return Json(new { count });
    }

    [HttpGet("/announcements/modal-feed")]
    public async Task<IActionResult> ModalFeed(CancellationToken ct)
    {
        var userId = GetCurrentUserId();
        if (!userId.HasValue)
            return Unauthorized();

        var feed = await announcementService.GetModalFeedAsync(userId.Value, ModalFeedTake, ct);
        return Json(feed);
    }

    [HttpPost("/announcements/mark-read")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> MarkRead([FromBody] AnnouncementMarkReadRequest? request, CancellationToken ct)
    {
        var userId = GetCurrentUserId();
        if (!userId.HasValue)
            return Unauthorized();

        var announcementId = request?.AnnouncementId ?? 0;
        if (announcementId <= 0)
            return BadRequest(new { message = "announcementId is required." });

        var unreadCount = await announcementService.MarkReadAndGetUnreadCountAsync(announcementId, userId.Value, ct);
        return Json(new { count = unreadCount });
    }

    private int? GetCurrentUserId()
    {
        var raw = User.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(raw, out var id) && id > 0 ? id : null;
    }
}
