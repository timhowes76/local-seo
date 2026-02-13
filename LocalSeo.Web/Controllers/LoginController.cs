using System.Security.Claims;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[AllowAnonymous]
public class LoginController(ILoginService loginService) : Controller
{
    [HttpGet("/login")]
    public IActionResult Index() => View();

    [HttpPost("/login/request")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RequestCode(LoginEmailModel model, CancellationToken ct)
    {
        var result = await loginService.RequestCodeAsync(model.Email, ct);
        ViewBag.Email = model.Email;
        ViewBag.Message = result.message;
        ViewBag.Step = result.ok ? "verify" : "request";
        return View("Index");
    }

    [HttpPost("/login/verify")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> VerifyCode(LoginCodeModel model, CancellationToken ct)
    {
        var result = await loginService.VerifyCodeAsync(model.Email, model.Code, ct);
        if (!result.ok)
        {
            ViewBag.Email = model.Email;
            ViewBag.Message = result.message;
            ViewBag.Step = "verify";
            return View("Index");
        }

        var claims = new[] { new Claim(ClaimTypes.Email, model.Email), new Claim(ClaimTypes.Name, model.Email) };
        var identity = new ClaimsIdentity(claims, "LocalCookie");
        await HttpContext.SignInAsync("LocalCookie", new ClaimsPrincipal(identity));
        return RedirectToAction("Index", "Search");
    }

    [Authorize]
    [HttpPost("/logout")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Logout()
    {
        await HttpContext.SignOutAsync("LocalCookie");
        return RedirectToAction("Index");
    }
}
