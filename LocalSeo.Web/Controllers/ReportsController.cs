using System.Security.Claims;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize]
public sealed class ReportsController(IReportsService reportsService) : Controller
{
    [HttpGet("/reports/{placeId}/{runId:long}/first-contact")]
    public async Task<IActionResult> FirstContact(string placeId, long runId, [FromQuery] string? variant, [FromQuery] int? print, CancellationToken ct)
    {
        if (!TryParseVariant(variant, out var parsedVariant))
            return BadRequest("Invalid report variant.");

        ViewBag.PrintMode = print.GetValueOrDefault() == 1;

        var model = await reportsService.BuildFirstContactReportAsync(placeId, runId, parsedVariant, ct);
        if (model is null)
        {
            var availability = await reportsService.GetFirstContactAvailabilityAsync(placeId, runId, ct);
            return View("FirstContactUnavailable", availability);
        }

        return View("FirstContact", model);
    }

    [HttpPost("/reports/{placeId}/{runId:long}/first-contact/pdf")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> FirstContactPdf(string placeId, long runId, [FromQuery] string? variant, CancellationToken ct)
    {
        if (!TryParseVariant(variant, out var parsedVariant))
            return BadRequest(new { success = false, message = "Invalid report variant." });

        var reportUrl = Url.ActionLink(
            action: nameof(FirstContact),
            controller: "Reports",
            values: new
            {
                placeId,
                runId,
                variant = VariantToQuery(parsedVariant),
                print = 1
            },
            protocol: Request.Scheme);

        if (string.IsNullOrWhiteSpace(reportUrl))
            return StatusCode(StatusCodes.Status500InternalServerError, new { success = false, message = "Could not build report URL." });

        var cookies = Request.Cookies.Select(x => new ReportCookie
        {
            Name = x.Key,
            Value = x.Value,
            Domain = Request.Host.Host,
            Path = "/",
            Secure = Request.IsHttps,
            HttpOnly = false,
            SameSite = "Lax"
        }).ToList();

        int? createdByUserId = null;
        var userIdRaw = User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (int.TryParse(userIdRaw, out var parsedUserId) && parsedUserId > 0)
            createdByUserId = parsedUserId;

        var result = await reportsService.GenerateFirstContactPdfAsync(new FirstContactPdfGenerationRequest
        {
            PlaceId = placeId,
            RunId = runId,
            Variant = parsedVariant,
            ReportUrl = reportUrl,
            Cookies = cookies,
            CreatedByUserId = createdByUserId
        }, ct);

        if (!result.Success)
            return BadRequest(new { success = false, message = result.Message });

        return Json(new
        {
            success = true,
            message = result.Message,
            downloadUrl = result.DownloadUrl,
            reportId = result.ReportId
        });
    }

    private static bool TryParseVariant(string? value, out FirstContactReportVariant variant)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            variant = FirstContactReportVariant.ClientFacing;
            return true;
        }
        if (string.Equals(normalized, "client", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "clientfacing", StringComparison.OrdinalIgnoreCase)
            || string.Equals(normalized, "client-facing", StringComparison.OrdinalIgnoreCase))
        {
            variant = FirstContactReportVariant.ClientFacing;
            return true;
        }

        if (string.Equals(normalized, "internal", StringComparison.OrdinalIgnoreCase))
        {
            variant = FirstContactReportVariant.Internal;
            return true;
        }

        variant = default;
        return false;
    }

    private static string VariantToQuery(FirstContactReportVariant variant)
        => variant == FirstContactReportVariant.ClientFacing ? "client" : "internal";
}
