using LocalSeo.Web.Models;
using LocalSeo.Web.Services.TransactionalEmails;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "AdminOnly")]
public sealed class TransactionalEmailDemoController(IEmailSender emailSender) : Controller
{
    [HttpPost("/admin/dev/email/password-reset")]
    public async Task<IActionResult> SendPasswordReset(
        [FromQuery] string toEmail,
        [FromQuery] string? toName,
        [FromQuery] string? recipientName,
        [FromQuery] string? resetUrl,
        CancellationToken ct)
    {
        var model = new PasswordResetEmailModel
        {
            RecipientName = string.IsNullOrWhiteSpace(recipientName) ? (toName ?? string.Empty) : recipientName.Trim(),
            ResetUrl = string.IsNullOrWhiteSpace(resetUrl)
                ? "https://example.local/reset-password?token=sample-token"
                : resetUrl.Trim()
        };

        await emailSender.SendAsync(
            toEmail,
            toName ?? string.Empty,
            templateKey: "PasswordReset",
            model,
            ct);

        return Ok(new { ok = true });
    }
}
