using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminEmailTemplatesController(
    IEmailTemplateService templateService) : Controller
{
    [HttpGet("/admin/email-templates")]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        var rows = await templateService.ListAsync(ct);
        return View(new EmailTemplateListViewModel
        {
            Rows = rows
        });
    }

    [HttpGet("/admin/email-templates/create")]
    public IActionResult Create()
    {
        return View(new EmailTemplateEditViewModel
        {
            Mode = "create",
            Template = new EmailTemplateEditModel
            {
                IsEnabled = true
            }
        });
    }

    [HttpPost("/admin/email-templates/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreatePost([FromForm] EmailTemplateEditModel model, CancellationToken ct)
    {
        var result = await templateService.CreateAsync(model, ct);
        if (!result.Success || !result.Id.HasValue)
        {
            return View("Create", new EmailTemplateEditViewModel
            {
                Mode = "create",
                Message = result.Message,
                Template = model,
                AvailableTokens = templateService.GetAvailableTokens(model.Key)
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id = result.Id.Value });
    }

    [HttpGet("/admin/email-templates/{id:int}/edit")]
    public async Task<IActionResult> Edit(int id, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var template = await templateService.GetByIdAsync(id, ct);
        if (template is null)
            return NotFound();

        return View(new EmailTemplateEditViewModel
        {
            Mode = "edit",
            Template = new EmailTemplateEditModel
            {
                Id = template.Id,
                Key = template.Key,
                Name = template.Name,
                FromName = template.FromName,
                FromEmail = template.FromEmail,
                SubjectTemplate = template.SubjectTemplate,
                BodyHtmlTemplate = template.BodyHtmlTemplate,
                IsSensitive = template.IsSensitive,
                IsEnabled = template.IsEnabled
            },
            AvailableTokens = templateService.GetAvailableTokens(template.Key)
        });
    }

    [HttpPost("/admin/email-templates/{id:int}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost(int id, [FromForm] EmailTemplateEditModel model, CancellationToken ct)
    {
        model.Id = id;
        var result = await templateService.UpdateAsync(model, ct);
        if (!result.Success)
        {
            return View("Edit", new EmailTemplateEditViewModel
            {
                Mode = "edit",
                Message = result.Message,
                Template = model,
                AvailableTokens = templateService.GetAvailableTokens(model.Key)
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Edit), new { id });
    }
}
