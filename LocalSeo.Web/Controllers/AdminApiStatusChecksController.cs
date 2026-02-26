using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminApiStatusChecksController(IApiStatusService apiStatusService) : Controller
{
    [HttpGet("/admin/settings/api-status-checks")]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        var rows = await apiStatusService.GetAdminDefinitionsAsync(ct);
        return View(new AdminApiStatusDefinitionsViewModel
        {
            Rows = rows.ToList()
        });
    }

    [HttpPost("/admin/settings/api-status-checks")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Save([FromForm] AdminApiStatusDefinitionsViewModel model, CancellationToken ct)
    {
        for (var i = 0; i < model.Rows.Count; i++)
            ValidateRow(model.Rows[i], i);

        if (!ModelState.IsValid)
            return View("Index", model);

        await apiStatusService.UpdateAdminDefinitionsAsync(model.Rows, ct);
        TempData["Status"] = "API status checks saved.";
        return RedirectToAction(nameof(Index));
    }

    private void ValidateRow(AdminApiStatusDefinitionRowModel row, int index)
    {
        if (row.IntervalSeconds < 5 || row.IntervalSeconds > 86400)
            ModelState.AddModelError($"Rows[{index}].IntervalSeconds", "Interval must be between 5 and 86400 seconds.");
        if (row.TimeoutSeconds < 1 || row.TimeoutSeconds > 120)
            ModelState.AddModelError($"Rows[{index}].TimeoutSeconds", "Timeout must be between 1 and 120 seconds.");
        if (row.DegradedThresholdMs.HasValue && (row.DegradedThresholdMs.Value < 1 || row.DegradedThresholdMs.Value > 300000))
            ModelState.AddModelError($"Rows[{index}].DegradedThresholdMs", "Degraded threshold must be between 1 and 300000 ms, or empty.");
    }
}

