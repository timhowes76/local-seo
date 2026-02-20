using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminSearchVolumeController(ICategoryLocationKeywordService categoryLocationKeywordService) : Controller
{
    [HttpGet("/admin/location/{locationId:long}/categories")]
    public async Task<IActionResult> LocationCategories(long locationId, CancellationToken ct)
    {
        try
        {
            var model = await categoryLocationKeywordService.GetLocationCategoriesAsync(locationId, ct);
            return View("~/Views/Admin/LocationCategories.cshtml", model);
        }
        catch (Exception ex)
        {
            TempData["Status"] = ex.Message;
            return RedirectToAction("TownsGb", "Admin");
        }
    }

    [HttpGet("/admin/location/{locationId:long}/category/{categoryId}/keyphrases")]
    public async Task<IActionResult> Keyphrases(long locationId, string categoryId, CancellationToken ct)
    {
        try
        {
            var model = await categoryLocationKeywordService.GetKeyphrasesAsync(locationId, DecodeCategoryId(categoryId), ct);
            return View("~/Views/Admin/CategoryKeyphrases.cshtml", model);
        }
        catch (Exception ex)
        {
            TempData["Status"] = ex.Message;
            return RedirectToAction(nameof(LocationCategories), new { locationId });
        }
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/add")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> AddKeyword(long locationId, string categoryId, [FromForm] CategoryLocationKeywordCreateModel model, CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.AddKeywordAndRefreshAsync(locationId, DecodeCategoryId(categoryId), model, ct);
            TempData["Status"] = $"Keyword added. Refreshed: {summary.RefreshedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Add keyword failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/refresh-eligible")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshEligible(long locationId, string categoryId, CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.RefreshEligibleKeywordsAsync(locationId, DecodeCategoryId(categoryId), ct);
            TempData["Status"] = $"Refresh completed. Requested: {summary.RequestedCount}, Refreshed: {summary.RefreshedCount}, Skipped: {summary.SkippedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Refresh failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/{keywordId:int}/refresh")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> RefreshKeyword(long locationId, string categoryId, int keywordId, CancellationToken ct)
    {
        try
        {
            var summary = await categoryLocationKeywordService.RefreshKeywordAsync(locationId, DecodeCategoryId(categoryId), keywordId, ct);
            TempData["Status"] = $"Refresh completed. Refreshed: {summary.RefreshedCount}, Errors: {summary.ErrorCount}.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Refresh failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/{keywordId:int}/set-main")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetMainTerm(long locationId, string categoryId, int keywordId, CancellationToken ct)
    {
        try
        {
            var changed = await categoryLocationKeywordService.SetMainTermAsync(locationId, DecodeCategoryId(categoryId), keywordId, ct);
            TempData["Status"] = changed ? "Main term updated." : "Keyword was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Set Main Term failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/{keywordId:int}/set-type")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> SetKeywordType(long locationId, string categoryId, int keywordId, [FromForm] int keywordType, CancellationToken ct)
    {
        try
        {
            var changed = await categoryLocationKeywordService.SetKeywordTypeAsync(locationId, DecodeCategoryId(categoryId), keywordId, keywordType, ct);
            TempData["Status"] = changed ? "Keyword type updated." : "Keyword was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Set keyword type failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    [HttpPost("/admin/location/{locationId:long}/category/{categoryId}/keyphrases/{keywordId:int}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteKeyword(long locationId, string categoryId, int keywordId, CancellationToken ct)
    {
        try
        {
            var changed = await categoryLocationKeywordService.DeleteKeywordAsync(locationId, DecodeCategoryId(categoryId), keywordId, ct);
            TempData["Status"] = changed ? "Keyword deleted." : "Keyword was not found.";
        }
        catch (Exception ex)
        {
            TempData["Status"] = $"Delete failed: {ex.Message}";
        }
        return Redirect(BuildKeyphrasesUrl(locationId, categoryId));
    }

    private static string DecodeCategoryId(string categoryId)
    {
        var value = (categoryId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        try
        {
            var base64 = value.Replace('-', '+').Replace('_', '/');
            while (base64.Length % 4 != 0)
                base64 += "=";
            var bytes = Convert.FromBase64String(base64);
            var decoded = System.Text.Encoding.UTF8.GetString(bytes);
            if (!string.IsNullOrWhiteSpace(decoded))
                return decoded.Trim();
        }
        catch
        {
            // Fall back to URL decoding for legacy links.
        }

        return Uri.UnescapeDataString(value);
    }

    private static string BuildKeyphrasesUrl(long locationId, string categoryId)
    {
        var normalized = DecodeCategoryId(categoryId);
        var encodedCategoryId = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(normalized))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        return $"/admin/location/{locationId}/category/{encodedCategoryId}/keyphrases";
    }
}
