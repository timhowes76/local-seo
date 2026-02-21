using System.Security.Claims;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public sealed class ProfileController(
    IUserRepository userRepository,
    TimeProvider timeProvider,
    ILogger<ProfileController> logger) : Controller
{
    [HttpGet("/profile/edit")]
    public async Task<IActionResult> Edit(CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");

        return View(new ProfileEditViewModel
        {
            Message = TempData["Status"] as string,
            Profile = new ProfileEditRequestModel
            {
                FirstName = user.FirstName,
                LastName = user.LastName,
                UseGravatar = user.UseGravatar
            }
        });
    }

    [HttpPost("/profile/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost([FromForm] ProfileEditRequestModel model, CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");

        var firstName = TrimAndBound(model.FirstName, 100);
        var lastName = TrimAndBound(model.LastName, 100);
        if (string.IsNullOrWhiteSpace(firstName) || string.IsNullOrWhiteSpace(lastName))
        {
            return View("Edit", new ProfileEditViewModel
            {
                Message = "First name and last name are required.",
                Profile = new ProfileEditRequestModel
                {
                    FirstName = firstName ?? string.Empty,
                    LastName = lastName ?? string.Empty,
                    UseGravatar = model.UseGravatar
                }
            });
        }

        var updated = await userRepository.UpdateProfileAsync(user.Id, firstName, lastName, model.UseGravatar, ct);
        if (!updated)
        {
            return View("Edit", new ProfileEditViewModel
            {
                Message = "Profile could not be updated right now.",
                Profile = new ProfileEditRequestModel
                {
                    FirstName = firstName,
                    LastName = lastName,
                    UseGravatar = model.UseGravatar
                }
            });
        }

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        logger.LogInformation(
            "Audit ProfileUpdated UserId={UserId} AtUtc={AtUtc} UseGravatar={UseGravatar}",
            user.Id,
            nowUtc,
            model.UseGravatar);

        if (user.UseGravatar != model.UseGravatar)
        {
            logger.LogInformation(
                "Audit UseGravatarToggled UserId={UserId} AtUtc={AtUtc} Enabled={Enabled}",
                user.Id,
                nowUtc,
                model.UseGravatar);
        }

        TempData["Status"] = "Profile updated.";
        return RedirectToAction(nameof(Edit));
    }

    private async Task<UserRecord?> GetCurrentUserAsync(CancellationToken ct)
    {
        var idClaim = User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(idClaim, out var userId) || userId <= 0)
            return null;

        return await userRepository.GetByIdAsync(userId, ct);
    }

    private static string? TrimAndBound(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
