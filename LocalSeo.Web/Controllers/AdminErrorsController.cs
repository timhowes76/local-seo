using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminErrorsController(
    IAppErrorRepository appErrorRepository,
    IWebHostEnvironment environment) : Controller
{
    private const int DefaultPageSize = 50;

    [HttpGet("/admin/errors")]
    public async Task<IActionResult> Index([FromQuery] int page = 1, [FromQuery] int pageSize = DefaultPageSize, CancellationToken ct = default)
    {
        var normalizedPage = Math.Max(1, page);
        var normalizedPageSize = NormalizePageSize(pageSize);

        var result = await appErrorRepository.GetPagedAsync(normalizedPage, normalizedPageSize, ct);
        var totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedPageSize));

        if (normalizedPage > totalPages)
        {
            normalizedPage = totalPages;
            result = await appErrorRepository.GetPagedAsync(normalizedPage, normalizedPageSize, ct);
            totalPages = Math.Max(1, (int)Math.Ceiling(result.TotalCount / (double)normalizedPageSize));
        }

        return View(new AppErrorListViewModel
        {
            Rows = result.Items,
            PageNumber = normalizedPage,
            PageSize = normalizedPageSize,
            TotalCount = result.TotalCount,
            TotalPages = totalPages,
            IsDevelopment = environment.IsDevelopment()
        });
    }

    [HttpGet("/admin/errors/{id:long}")]
    public async Task<IActionResult> Details(long id, CancellationToken ct = default)
    {
        if (id <= 0)
            return NotFound();

        var row = await appErrorRepository.GetByIdAsync(id, ct);
        if (row is null)
            return NotFound();

        return View(new AppErrorDetailViewModel
        {
            Row = row,
            IsDevelopment = environment.IsDevelopment()
        });
    }

    [HttpPost("/admin/errors/delete-all")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteAll(CancellationToken ct)
    {
        await appErrorRepository.DeleteAllAsync(ct);
        TempData["Status"] = "All 500 error logs deleted.";
        return RedirectToAction(nameof(Index));
    }

    [HttpGet("/admin/errors/test/throw")]
    public IActionResult TestThrow()
    {
        if (!environment.IsDevelopment())
            return NotFound();

        throw new InvalidOperationException("Development test exception for AppError logging.");
    }

    [HttpGet("/admin/errors/test/status-500")]
    public IActionResult TestStatus500()
    {
        if (!environment.IsDevelopment())
            return NotFound();

        return StatusCode(500, "Development test endpoint returned StatusCode(500) without throwing.");
    }

    private static int NormalizePageSize(int value)
    {
        return value switch
        {
            25 => 25,
            50 => 50,
            100 => 100,
            500 => 500,
            1000 => 1000,
            _ => DefaultPageSize
        };
    }
}
