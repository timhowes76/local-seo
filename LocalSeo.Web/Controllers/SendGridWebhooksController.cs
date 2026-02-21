using System.Text;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[AllowAnonymous]
public sealed class SendGridWebhooksController(
    ISendGridWebhookSignatureValidator signatureValidator,
    ISendGridWebhookIngestionService webhookIngestionService,
    ILogger<SendGridWebhooksController> logger) : ControllerBase
{
    [IgnoreAntiforgeryToken]
    [HttpPost("/api/webhooks/sendgrid/events")]
    public async Task<IActionResult> Events(CancellationToken ct)
    {
        string payloadJson;
        using (var reader = new StreamReader(Request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true))
        {
            payloadJson = await reader.ReadToEndAsync(ct);
        }

        var payloadBytes = Encoding.UTF8.GetBytes(payloadJson);
        var signature = Request.Headers["X-Twilio-Email-Event-Webhook-Signature"].ToString();
        var timestamp = Request.Headers["X-Twilio-Email-Event-Webhook-Timestamp"].ToString();
        if (!signatureValidator.IsValid(timestamp, signature, payloadBytes))
            return Unauthorized();

        var result = await webhookIngestionService.IngestAsync(payloadJson, ct);
        if (!result.Success)
        {
            logger.LogWarning("SendGrid webhook ingestion failed. Message={Message}", result.Message);
            return BadRequest(new { ok = false, message = "Invalid SendGrid webhook payload." });
        }

        return Ok(new { ok = true, received = result.ReceivedCount, inserted = result.InsertedCount });
    }
}
