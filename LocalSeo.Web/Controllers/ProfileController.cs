using System.Security.Claims;
using System.Diagnostics;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
public sealed class ProfileController(
    IUserRepository userRepository,
    IPasswordChangeService passwordChangeService,
    ISecuritySettingsProvider securitySettingsProvider,
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

    [HttpGet("/profile/change-password")]
    public IActionResult ChangePassword()
    {
        return View(new ChangePasswordStartViewModel
        {
            Message = TempData["Status"] as string
        });
    }

    [HttpPost("/profile/change-password/start")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ChangePasswordStart([FromForm] ChangePasswordStartRequestModel model, CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");

        var result = await passwordChangeService.StartAsync(
            user.Id,
            model.CurrentPassword,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            Activity.Current?.Id ?? HttpContext.TraceIdentifier,
            ct);

        if (!result.Success || string.IsNullOrWhiteSpace(result.CorrelationId))
        {
            return View("ChangePassword", new ChangePasswordStartViewModel
            {
                Message = result.Message,
                Form = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(ChangePasswordVerify), new { c = result.CorrelationId });
    }

    [HttpGet("/profile/change-password/verify")]
    public async Task<IActionResult> ChangePasswordVerify([FromQuery(Name = "c")] string? correlationId, CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");
        var securitySettings = await securitySettingsProvider.GetAsync(ct);

        var challenge = await passwordChangeService.GetChallengeAsync(user.Id, correlationId, ct);
        if (!challenge.Success || challenge.Challenge is null)
        {
            return View(new ChangePasswordVerifyViewModel
            {
                Message = challenge.Message,
                IsInvalidOrExpired = true,
                PasswordPolicy = new PasswordPolicyViewModel
                {
                    MinimumPasswordLength = securitySettings.PasswordPolicy.MinimumLength,
                    RequiresNumber = securitySettings.PasswordPolicy.RequiresNumber,
                    RequiresCapitalLetter = securitySettings.PasswordPolicy.RequiresCapitalLetter,
                    RequiresSpecialCharacter = securitySettings.PasswordPolicy.RequiresSpecialCharacter
                }
            });
        }

        return View(new ChangePasswordVerifyViewModel
        {
            Message = TempData["Status"] as string,
            CorrelationId = challenge.Challenge.CorrelationId ?? string.Empty,
            ExpiresAtUtc = challenge.Challenge.ExpiresAtUtc,
            LockedUntilUtc = challenge.Challenge.LockedUntilUtc,
            Form = new ChangePasswordVerifyRequestModel
            {
                CorrelationId = challenge.Challenge.CorrelationId ?? string.Empty
            },
            PasswordPolicy = new PasswordPolicyViewModel
            {
                MinimumPasswordLength = securitySettings.PasswordPolicy.MinimumLength,
                RequiresNumber = securitySettings.PasswordPolicy.RequiresNumber,
                RequiresCapitalLetter = securitySettings.PasswordPolicy.RequiresCapitalLetter,
                RequiresSpecialCharacter = securitySettings.PasswordPolicy.RequiresSpecialCharacter
            }
        });
    }

    [HttpPost("/profile/change-password/resend")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ChangePasswordResend([FromForm] ChangePasswordResendRequestModel model, CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");

        var result = await passwordChangeService.ResendAsync(
            user.Id,
            model.CorrelationId,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            Activity.Current?.Id ?? HttpContext.TraceIdentifier,
            ct);

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(ChangePasswordVerify), new { c = model.CorrelationId });
    }

    [HttpPost("/profile/change-password/verify")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ChangePasswordVerifyPost([FromForm] ChangePasswordVerifyRequestModel model, CancellationToken ct)
    {
        var user = await GetCurrentUserAsync(ct);
        if (user is null)
            return Redirect("/login");
        var securitySettings = await securitySettingsProvider.GetAsync(ct);

        var result = await passwordChangeService.VerifyAndChangePasswordAsync(
            user.Id,
            model.CorrelationId,
            model.OtpCode,
            model.NewPassword,
            model.ConfirmNewPassword,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            Activity.Current?.Id ?? HttpContext.TraceIdentifier,
            ct);

        if (result.Success)
        {
            await HttpContext.SignOutAsync("LocalCookie");
            TempData["Status"] = "Password changed successfully. Please sign in again.";
            return Redirect("/login");
        }

        var challenge = await passwordChangeService.GetChallengeAsync(user.Id, model.CorrelationId, ct);
        return View("ChangePasswordVerify", new ChangePasswordVerifyViewModel
        {
            Message = result.Message,
            IsInvalidOrExpired = !challenge.Success || challenge.Challenge is null,
            CorrelationId = challenge.Challenge?.CorrelationId ?? model.CorrelationId,
            ExpiresAtUtc = challenge.Challenge?.ExpiresAtUtc,
            LockedUntilUtc = challenge.Challenge?.LockedUntilUtc,
            Form = new ChangePasswordVerifyRequestModel
            {
                CorrelationId = challenge.Challenge?.CorrelationId ?? model.CorrelationId
            },
            PasswordPolicy = new PasswordPolicyViewModel
            {
                MinimumPasswordLength = securitySettings.PasswordPolicy.MinimumLength,
                RequiresNumber = securitySettings.PasswordPolicy.RequiresNumber,
                RequiresCapitalLetter = securitySettings.PasswordPolicy.RequiresCapitalLetter,
                RequiresSpecialCharacter = securitySettings.PasswordPolicy.RequiresSpecialCharacter
            }
        });
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
