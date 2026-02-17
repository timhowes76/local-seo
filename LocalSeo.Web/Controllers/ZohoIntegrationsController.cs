using System.Security.Claims;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
[ApiController]
public sealed class ZohoIntegrationsController(
    IZohoOAuthService zohoOAuthService,
    IZohoCrmClient zohoCrmClient,
    ILogger<ZohoIntegrationsController> logger) : ControllerBase
{
    [HttpGet("/integrations/zoho/connect")]
    public IActionResult Connect()
    {
        try
        {
            var connectUrl = zohoOAuthService.BuildConnectUrl(GetCurrentUserIdentityKey());
            return Redirect(connectUrl);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Zoho connect redirect failed.");
            return BadRequest(new { success = false, message = ex.Message });
        }
    }

    [HttpGet("/integrations/zoho/callback")]
    [HttpGet("/zoho/oauth/callback")]
    public async Task<IActionResult> Callback(
        [FromQuery] string? code,
        [FromQuery] string? state,
        [FromQuery] string? location,
        [FromQuery] string? error,
        [FromQuery(Name = "error_description")] string? errorDescription,
        CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(error))
        {
            var message = string.IsNullOrWhiteSpace(errorDescription)
                ? $"Zoho authorization failed: {error}."
                : $"Zoho authorization failed: {error} ({errorDescription}).";
            return BadRequest(new { success = false, message });
        }

        try
        {
            var result = await zohoOAuthService.CompleteConnectionAsync(
                code ?? string.Empty,
                state ?? string.Empty,
                GetCurrentUserIdentityKey(),
                location,
                ct);

            return Ok(result);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Zoho callback processing failed.");
            return BadRequest(new { success = false, message = ex.Message });
        }
    }

    [HttpGet("/integrations/zoho/ping")]
    public async Task<IActionResult> Ping(CancellationToken ct)
    {
        try
        {
            using var response = await zohoCrmClient.PingAsync(ct);
            return Ok(new
            {
                success = true,
                message = "Zoho CRM API reachable.",
                response = response.RootElement.Clone()
            });
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Zoho ping failed.");
            return StatusCode(StatusCodes.Status502BadGateway, new { success = false, message = ex.Message });
        }
    }

    private string GetCurrentUserIdentityKey()
    {
        var email = User.FindFirstValue(ClaimTypes.Email);
        if (!string.IsNullOrWhiteSpace(email))
            return email;
        if (!string.IsNullOrWhiteSpace(User.Identity?.Name))
            return User.Identity.Name;
        return "unknown";
    }
}
