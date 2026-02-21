using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class AdminUsersController(
    IUserRepository userRepository,
    IUserInviteRepository userInviteRepository,
    IInviteService inviteService,
    IEmailAddressNormalizer emailAddressNormalizer) : Controller
{
    [HttpGet("/admin/users")]
    public async Task<IActionResult> Index([FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        var filter = ParseFilter(status);
        var searchTerm = NormalizeSearchTerm(q);
        var rows = await userRepository.ListByStatusAsync(filter, searchTerm, ct);
        return View(new AdminUsersListViewModel
        {
            Filter = ToFilterKey(filter),
            SearchTerm = searchTerm,
            CurrentUserId = GetCurrentUserId(),
            Rows = rows
        });
    }

    [HttpGet("/admin/users/create")]
    public IActionResult Create([FromQuery] string? status, [FromQuery] string? q)
    {
        var filter = ParseFilter(status);
        var searchTerm = NormalizeSearchTerm(q);
        return View(new AdminCreateUserViewModel
        {
            Filter = ToFilterKey(filter),
            SearchTerm = searchTerm,
            CreateUser = new AdminCreateUserRequestModel()
        });
    }

    [HttpPost("/admin/users")]
    [HttpPost("/admin/users/create")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> CreatePost([FromForm] AdminCreateUserRequestModel model, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        var filter = ToFilterKey(ParseFilter(status));
        var searchTerm = NormalizeSearchTerm(q);
        var result = await inviteService.CreateUserAndInviteAsync(
            model.FirstName,
            model.LastName,
            model.EmailAddress,
            GetCurrentUserId(),
            $"{Request.Scheme}://{Request.Host.Value}",
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            ct);

        if (!result.Success)
        {
            ViewBag.Message = result.Message;
            return View("Create", new AdminCreateUserViewModel
            {
                Filter = filter,
                SearchTerm = searchTerm,
                CreateUser = model
            });
        }

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Index), new { status = filter, q = searchTerm });
    }

    [HttpPost("/admin/users/{id:int}/resend-invite")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ResendInvite(int id, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        var searchTerm = NormalizeSearchTerm(q);
        var result = await inviteService.ResendInviteAsync(
            id,
            GetCurrentUserId(),
            $"{Request.Scheme}://{Request.Host.Value}",
            HttpContext.Connection.RemoteIpAddress?.ToString(),
            ct);

        TempData["Status"] = result.Message;
        return RedirectToAction(nameof(Index), new { status = ToFilterKey(ParseFilter(status)), q = searchTerm });
    }

    [HttpGet("/admin/users/{id:int}/edit")]
    public async Task<IActionResult> Edit(int id, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var searchTerm = NormalizeSearchTerm(q);
        var user = await userRepository.GetByIdAsync(id, ct);
        if (user is null)
            return NotFound();

        return View(new AdminEditUserViewModel
        {
            Filter = ToFilterKey(ParseFilter(status)),
            SearchTerm = searchTerm,
            User = new AdminEditUserRequestModel
            {
                Id = user.Id,
                FirstName = user.FirstName,
                LastName = user.LastName,
                EmailAddress = user.EmailAddress,
                IsAdmin = user.IsAdmin,
                InviteStatus = user.InviteStatus
            }
        });
    }

    [HttpPost("/admin/users/{id:int}/edit")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> EditPost(int id, [FromForm] AdminEditUserRequestModel model, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        var filter = ToFilterKey(ParseFilter(status));
        var searchTerm = NormalizeSearchTerm(q);
        model.Id = id;

        var firstName = (model.FirstName ?? string.Empty).Trim();
        var lastName = (model.LastName ?? string.Empty).Trim();
        var emailAddress = (model.EmailAddress ?? string.Empty).Trim();
        if (firstName.Length == 0 || lastName.Length == 0 || emailAddress.Length == 0)
        {
            ViewBag.Message = "First name, last name, and email are required.";
            return View("Edit", new AdminEditUserViewModel { Filter = filter, SearchTerm = searchTerm, User = model });
        }

        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        if (normalizedEmail.Length == 0)
        {
            ViewBag.Message = "A valid email address is required.";
            return View("Edit", new AdminEditUserViewModel { Filter = filter, SearchTerm = searchTerm, User = model });
        }

        var emailOwner = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        if (emailOwner is not null && emailOwner.Id != id)
        {
            ViewBag.Message = "A user with that email already exists.";
            return View("Edit", new AdminEditUserViewModel { Filter = filter, SearchTerm = searchTerm, User = model });
        }

        try
        {
            var updated = await userRepository.UpdateUserAsync(
                id,
                firstName,
                lastName,
                emailAddress,
                normalizedEmail,
                model.IsAdmin,
                model.InviteStatus,
                ct);

            if (!updated)
            {
                TempData["Status"] = "User not found or could not be updated.";
                return RedirectToAction(nameof(Index), new { status = filter, q = searchTerm });
            }

            TempData["Status"] = "User updated.";
            return RedirectToAction(nameof(Details), new { id, status = filter, q = searchTerm });
        }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number is 2601 or 2627)
        {
            ViewBag.Message = "A user with that email already exists.";
            return View("Edit", new AdminEditUserViewModel { Filter = filter, SearchTerm = searchTerm, User = model });
        }
    }

    [HttpPost("/admin/users/{id:int}/delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Delete(int id, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        var searchTerm = NormalizeSearchTerm(q);
        var currentUserId = GetCurrentUserId();
        if (currentUserId.HasValue && currentUserId.Value == id)
        {
            TempData["Status"] = "You cannot delete your own account while signed in.";
            return RedirectToAction(nameof(Index), new { status = ToFilterKey(ParseFilter(status)), q = searchTerm });
        }

        var deleted = await userRepository.DeleteUserAsync(id, ct);
        TempData["Status"] = deleted
            ? "User deleted."
            : "User not found or could not be deleted.";
        return RedirectToAction(nameof(Index), new { status = ToFilterKey(ParseFilter(status)), q = searchTerm });
    }

    [HttpGet("/admin/users/{id:int}")]
    public async Task<IActionResult> Details(int id, [FromQuery] string? status, [FromQuery] string? q, CancellationToken ct)
    {
        if (id <= 0)
            return NotFound();

        var searchTerm = NormalizeSearchTerm(q);
        var row = await userRepository.GetByIdAsync(id, ct);
        if (row is null)
            return NotFound();
        var latestInvite = await userInviteRepository.GetLatestInviteByUserIdAsync(id, ct);
        var nowUtc = DateTime.UtcNow;

        return View(new AdminUserDetailsViewModel
        {
            User = row,
            Filter = ToFilterKey(ParseFilter(status)),
            SearchTerm = searchTerm,
            CurrentUserId = GetCurrentUserId(),
            LastInviteCreatedAtUtc = latestInvite?.CreatedAtUtc,
            HasActiveInvite = latestInvite is not null
                && latestInvite.Status == UserInviteStatus.Active
                && !latestInvite.UsedAtUtc.HasValue
                && latestInvite.ExpiresAtUtc >= nowUtc
        });
    }

    private static UserStatusFilter ParseFilter(string? status)
    {
        if (string.Equals(status, "pending", StringComparison.OrdinalIgnoreCase))
            return UserStatusFilter.Pending;
        if (string.Equals(status, "disabled", StringComparison.OrdinalIgnoreCase)
            || string.Equals(status, "inactive", StringComparison.OrdinalIgnoreCase))
            return UserStatusFilter.Disabled;
        if (string.Equals(status, "all", StringComparison.OrdinalIgnoreCase))
            return UserStatusFilter.All;
        return UserStatusFilter.Active;
    }

    private static string ToFilterKey(UserStatusFilter filter)
    {
        return filter switch
        {
            UserStatusFilter.Pending => "pending",
            UserStatusFilter.Disabled => "disabled",
            UserStatusFilter.All => "all",
            _ => "active"
        };
    }

    private static string NormalizeSearchTerm(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        return trimmed.Length <= 200 ? trimmed : trimmed[..200];
    }

    private int? GetCurrentUserId()
    {
        var raw = User.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(raw, out var id) && id > 0 ? id : null;
    }
}
