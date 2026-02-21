using System.Security.Claims;
using System.Diagnostics;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

public class LoginController(IAuthService authService) : Controller
{
    [AllowAnonymous]
    [HttpGet("/login")]
    public IActionResult Index()
    {
        var statusMessage = TempData["Status"] as string;
        if (!string.IsNullOrWhiteSpace(statusMessage))
            ViewBag.Message = statusMessage;
        return View(new LoginRequestModel());
    }

    [AllowAnonymous]
    [HttpPost("/login")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Login(LoginRequestModel model, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(model.Email) || string.IsNullOrWhiteSpace(model.Password))
        {
            ViewBag.Message = "Email and password are required.";
            return View("Index", model);
        }

        var result = await authService.BeginLoginAsync(
            model.Email,
            model.Password,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            Activity.Current?.Id ?? HttpContext.TraceIdentifier,
            ct);

        if (!result.Success)
        {
            ViewBag.Message = result.Message;
            model.Password = string.Empty;
            return View("Index", model);
        }

        return RedirectToAction(nameof(TwoFactor), new { rid = result.Rid, email = result.EmailAddress });
    }

    [AllowAnonymous]
    [HttpGet("/twofactor")]
    public IActionResult TwoFactor([FromQuery] int rid, [FromQuery] string? email)
    {
        return View(new TwoFactorRequestModel
        {
            Rid = rid,
            Email = email ?? string.Empty
        });
    }

    [AllowAnonymous]
    [HttpPost("/twofactor")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> VerifyTwoFactor(TwoFactorRequestModel model, CancellationToken ct)
    {
        var result = await authService.CompleteTwoFactorLoginAsync(
            model.Rid,
            model.Email,
            model.Code,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            Activity.Current?.Id ?? HttpContext.TraceIdentifier,
            ct);
        if (!result.Success || result.User is null)
        {
            ViewBag.Message = result.Message;
            return View("TwoFactor", model);
        }

        await SignInUserAsync(result.User);
        return RedirectToAction("Index", "Home");
    }

    [AllowAnonymous]
    [HttpGet("/forgot-password")]
    public IActionResult ForgotPassword() => View(new ForgotPasswordRequestModel());

    [AllowAnonymous]
    [HttpPost("/forgot-password")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ForgotPassword(ForgotPasswordRequestModel model, CancellationToken ct)
    {
        var appBaseUrl = $"{Request.Scheme}://{Request.Host.Value}";
        var message = await authService.RequestForgotPasswordAsync(
            model.Email,
            appBaseUrl,
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            Request.Headers.UserAgent.ToString(),
            ct);

        ViewBag.Message = message;
        return View("ForgotPassword", model);
    }

    [AllowAnonymous]
    [HttpGet("/reset-password")]
    public IActionResult ResetPassword([FromQuery] int rid)
    {
        return View(new ResetPasswordRequestModel { Rid = rid });
    }

    [AllowAnonymous]
    [HttpPost("/reset-password")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ResetPassword(ResetPasswordRequestModel model, CancellationToken ct)
    {
        var result = await authService.ResetPasswordAsync(
            model.Rid,
            model.Email,
            model.Code,
            model.NewPassword,
            model.ConfirmPassword,
            ct);

        if (!result.Success || result.User is null)
        {
            ViewBag.Message = result.Message;
            return View("ResetPassword", model);
        }

        await SignInUserAsync(result.User);
        return RedirectToAction("Index", "Home");
    }

    [Authorize]
    [HttpPost("/logout")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Logout()
    {
        await HttpContext.SignOutAsync("LocalCookie");
        return RedirectToAction("Index");
    }

    private async Task SignInUserAsync(UserRecord user)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, user.Id.ToString()),
            new(ClaimTypes.Email, user.EmailAddress),
            new(ClaimTypes.Name, $"{user.FirstName} {user.LastName}".Trim()),
            new(AuthClaimTypes.IsAdmin, user.IsAdmin ? "true" : "false")
        };

        if (user.IsAdmin)
            claims.Add(new Claim(ClaimTypes.Role, "Admin"));

        var identity = new ClaimsIdentity(claims, "LocalCookie");
        await HttpContext.SignInAsync("LocalCookie", new ClaimsPrincipal(identity));
    }
}
