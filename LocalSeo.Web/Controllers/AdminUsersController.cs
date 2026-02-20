using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminUsersController(IUserRepository userRepository) : Controller
{
    [HttpGet("/admin/users")]
    public async Task<IActionResult> Index([FromQuery] string? status, CancellationToken ct)
    {
        var filter = ParseFilter(status);
        var rows = await userRepository.ListByStatusAsync(filter, ct);
        return View(new AdminUsersListViewModel
        {
            Filter = ToFilterKey(filter),
            Rows = rows
        });
    }

    [HttpGet("/admin/users/{id:int}")]
    public async Task<IActionResult> Details(int id, [FromQuery] string? status, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var row = await userRepository.GetByIdAsync(id, ct);
        if (row is null)
            return NotFound();

        return View(new AdminUserDetailsViewModel
        {
            User = row,
            Filter = ToFilterKey(ParseFilter(status))
        });
    }

    private static UserStatusFilter ParseFilter(string? status)
    {
        if (string.Equals(status, "inactive", StringComparison.OrdinalIgnoreCase))
            return UserStatusFilter.Inactive;
        if (string.Equals(status, "all", StringComparison.OrdinalIgnoreCase))
            return UserStatusFilter.All;
        return UserStatusFilter.Active;
    }

    private static string ToFilterKey(UserStatusFilter filter)
    {
        return filter switch
        {
            UserStatusFilter.Inactive => "inactive",
            UserStatusFilter.All => "all",
            _ => "active"
        };
    }
}
