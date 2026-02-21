using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[AllowAnonymous]
public sealed class InviteController(
    IInviteService inviteService,
    ISecuritySettingsProvider securitySettingsProvider) : Controller
{
    [HttpGet("/invite/accept")]
    public async Task<IActionResult> Accept([FromQuery] string? token, CancellationToken ct)
    {
        var validation = await inviteService.ValidateInviteTokenAsync(token, ct);
        if (!validation.Success || validation.Invite is null || string.IsNullOrWhiteSpace(token))
            return View("InviteInvalid");

        return View(BuildAcceptViewModel(token, validation.Invite, message: null, otpSent: false));
    }

    [HttpPost("/invite/send-otp")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SendOtp([FromForm] InviteTokenFormModel model, CancellationToken ct)
    {
        var result = await inviteService.SendOtpAsync(model.Token, HttpContext.Connection.RemoteIpAddress?.ToString(), ct);
        if (result.Invite is null || string.IsNullOrWhiteSpace(model.Token))
            return View("InviteInvalid");

        return View("Accept", BuildAcceptViewModel(model.Token, result.Invite, result.Message, result.OtpSent));
    }

    [HttpPost("/invite/verify-otp")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> VerifyOtp([FromForm] InviteVerifyOtpFormModel model, CancellationToken ct)
    {
        var result = await inviteService.VerifyOtpAsync(model.Token, model.Code, HttpContext.Connection.RemoteIpAddress?.ToString(), ct);
        if (result.Invite is null || string.IsNullOrWhiteSpace(model.Token))
            return View("InviteInvalid");

        if (result.Success && result.OtpVerified)
            return Redirect($"/invite/set-password?token={Uri.EscapeDataString(model.Token)}");

        return View("Accept", BuildAcceptViewModel(model.Token, result.Invite, result.Message, result.OtpSent));
    }

    [HttpGet("/invite/set-password")]
    public async Task<IActionResult> SetPassword([FromQuery] string? token, CancellationToken ct)
    {
        var validation = await inviteService.ValidateInviteTokenAsync(token, ct);
        if (!validation.Success || validation.Invite is null || string.IsNullOrWhiteSpace(token))
            return View("InviteInvalid");

        if (!validation.Invite.OtpVerifiedAtUtc.HasValue)
            return View("Accept", BuildAcceptViewModel(token, validation.Invite, "Please verify your email first.", otpSent: false));

        var securitySettings = await securitySettingsProvider.GetAsync(ct);
        return View(BuildSetPasswordViewModel(token, validation.Invite, message: null, useGravatar: false, securitySettings.PasswordPolicy));
    }

    [HttpPost("/invite/set-password")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetPassword([FromForm] InviteSetPasswordFormModel model, CancellationToken ct)
    {
        var validation = await inviteService.ValidateInviteTokenAsync(model.Token, ct);
        if (!validation.Success || validation.Invite is null || string.IsNullOrWhiteSpace(model.Token))
            return View("InviteInvalid");

        if (!validation.Invite.OtpVerifiedAtUtc.HasValue)
            return View("Accept", BuildAcceptViewModel(model.Token, validation.Invite, "Please verify your email first.", otpSent: false));

        var result = await inviteService.SetPasswordAsync(model.Token, model.NewPassword, model.ConfirmPassword, model.UseGravatar, ct);
        if (result.Success)
        {
            TempData["Status"] = result.Message;
            return Redirect("/login");
        }

        var securitySettings = await securitySettingsProvider.GetAsync(ct);
        return View(BuildSetPasswordViewModel(model.Token, validation.Invite, result.Message, model.UseGravatar, securitySettings.PasswordPolicy));
    }

    private InviteAcceptViewModel BuildAcceptViewModel(string token, UserInviteRecord invite, string? message, bool otpSent)
    {
        var otpVerified = invite.OtpVerifiedAtUtc.HasValue;
        return new InviteAcceptViewModel
        {
            Token = token,
            EmailAddressMasked = inviteService.MaskEmailAddress(invite.EmailAddress),
            CanSendOtp = !otpVerified,
            OtpSent = otpSent,
            OtpVerified = otpVerified,
            Message = message
        };
    }

    private InviteSetPasswordViewModel BuildSetPasswordViewModel(string token, UserInviteRecord invite, string? message, bool useGravatar, PasswordPolicyRules passwordPolicy)
    {
        return new InviteSetPasswordViewModel
        {
            Token = token,
            EmailAddressMasked = inviteService.MaskEmailAddress(invite.EmailAddress),
            Message = message,
            UseGravatar = useGravatar,
            PasswordPolicy = new PasswordPolicyViewModel
            {
                MinimumPasswordLength = passwordPolicy.MinimumLength,
                RequiresNumber = passwordPolicy.RequiresNumber,
                RequiresCapitalLetter = passwordPolicy.RequiresCapitalLetter,
                RequiresSpecialCharacter = passwordPolicy.RequiresSpecialCharacter
            }
        };
    }
}
