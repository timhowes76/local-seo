using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminAuditRulesController(
    ISeoAuditService seoAuditService) : Controller
{
    private const int MinimumParameterSlots = 6;

    [HttpGet("/admin/audit-rules")]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        var rows = await seoAuditService.GetAdminRuleListAsync(ct);
        return View(new SeoAuditRuleListViewModel
        {
            Rows = rows
        });
    }

    [HttpGet("/admin/audit-rules/create")]
    public IActionResult Create()
    {
        return View(new SeoAuditRuleEditViewModel
        {
            Mode = "create",
            Rule = EnsureParameterSlots(new SeoAuditRuleEditModel
            {
                RuleMode = SeoAuditRuleModes.Fixed,
                IsActive = true,
                ShowInActionsTab = true
            })
        });
    }

    [HttpPost("/admin/audit-rules/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreatePost([FromForm] SeoAuditRuleEditModel model, CancellationToken ct)
    {
        var result = await seoAuditService.CreateRuleAsync(model, ct);
        if (!result.Success || !result.RuleId.HasValue)
        {
            return View("Create", new SeoAuditRuleEditViewModel
            {
                Mode = "create",
                Message = result.Message,
                Rule = EnsureParameterSlots(model)
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id = result.RuleId.Value });
    }

    [HttpGet("/admin/audit-rules/{id:long}/edit")]
    public async Task<IActionResult> Edit(long id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var rule = await seoAuditService.GetAdminRuleEditModelAsync(id, ct);
        if (rule is null)
            return NotFound();

        return View(new SeoAuditRuleEditViewModel
        {
            Mode = "edit",
            Rule = EnsureParameterSlots(rule)
        });
    }

    [HttpPost("/admin/audit-rules/{id:long}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost(long id, [FromForm] SeoAuditRuleEditModel model, CancellationToken ct)
    {
        model.SeoAuditRuleId = id;
        var result = await seoAuditService.UpdateRuleAsync(id, model, ct);
        if (!result.Success)
        {
            return View("Edit", new SeoAuditRuleEditViewModel
            {
                Mode = "edit",
                Message = result.Message,
                Rule = EnsureParameterSlots(model)
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id });
    }

    [HttpPost("/admin/audit-rules/{id:long}/toggle-active")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ToggleActive(long id, [FromForm] bool isActive, CancellationToken ct)
    {
        var result = await seoAuditService.ToggleRuleActiveAsync(id, isActive, ct);
        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Index));
    }

    [HttpPost("/admin/audit-rules/recalculate-missing")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RecalculateMissing(CancellationToken ct)
    {
        var count = await seoAuditService.RecalculateAllMissingAuditResultsAsync(ct);
        TempData["Status"] = $"Recalculated audits for {count} place(s) missing current results.";
        return RedirectToAction(nameof(Index));
    }

    [HttpPost("/admin/audit-rules/recalculate-all")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RecalculateAll(CancellationToken ct)
    {
        var count = await seoAuditService.RecalculateAllAuditResultsAsync(ct);
        TempData["Status"] = $"Recalculated audits for {count} place(s).";
        return RedirectToAction(nameof(Index));
    }

    [HttpPost("/admin/audit-rules/recalculate-place")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RecalculatePlace([FromForm] string placeId, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
        {
            TempData["Status"] = "PlaceId is required.";
            return RedirectToAction(nameof(Index));
        }

        try
        {
            await seoAuditService.RecalculateAuditForPlaceAsync(normalizedPlaceId, ct);
            TempData["Status"] = $"Recalculated audit for place '{normalizedPlaceId}'.";
        }
        catch (InvalidOperationException ex)
        {
            TempData["Status"] = ex.Message;
        }

        return RedirectToAction(nameof(Index));
    }

    private static SeoAuditRuleEditModel EnsureParameterSlots(SeoAuditRuleEditModel model)
    {
        model.Parameters ??= [];
        while (model.Parameters.Count < MinimumParameterSlots)
        {
            model.Parameters.Add(new SeoAuditRuleParameterEditModel
            {
                SortOrder = model.Parameters.Count + 1,
                IsActive = true
            });
        }

        return model;
    }
}
