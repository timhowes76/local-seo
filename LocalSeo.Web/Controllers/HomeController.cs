using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

public class HomeController : Controller
{
    public IActionResult Index() => User.Identity?.IsAuthenticated == true ? RedirectToAction("Index", "Search") : RedirectToAction("Index", "Login");

    [AllowAnonymous]
    public IActionResult Error() => View();
}
