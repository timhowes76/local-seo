using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Controllers;

public class HomeController(
    IDataForSeoAccountStatusService dataForSeoAccountStatusService,
    IApiStatusService apiStatusService) : Controller
{
    [AllowAnonymous]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        if (User.Identity?.IsAuthenticated != true)
            return RedirectToAction("Index", "Login");

        var model = await dataForSeoAccountStatusService.GetDashboardAsync(ct);
        var apiStatusSnapshot = await apiStatusService.GetDashboardSnapshotAsync(ct);
        model.ApiStatusRetrievedAtUtc = apiStatusSnapshot.RetrievedUtc;
        model.ApiStatusWidgets = apiStatusSnapshot.Widgets;
        return View(model);
    }

    [AllowAnonymous]
    public IActionResult Error() => View();
}
