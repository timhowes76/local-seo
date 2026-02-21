using System.Collections.Generic;
using System.IO;
using System.Text;
using LocalSeo.Web.Controllers;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

namespace LocalSeo.Web.Tests;

public class EmailAndWebhookTests
{
    [Fact]
    public void EmailTemplateRenderer_ReplacesTokens_CaseInsensitive()
    {
        var renderer = new EmailTemplateRenderer();
        var tokens = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["code"] = "654321",
            ["expiryminutes"] = "10"
        };

        var result = renderer.Render("Code [%Code%], valid [%EXPIRYMINUTES%] mins.", tokens);

        Assert.Equal("Code 654321, valid 10 mins.", result.RenderedText);
        Assert.Empty(result.UnknownTokens);
    }

    [Fact]
    public void EmailRedactionService_RedactsTwoFactorCode()
    {
        var redactor = new EmailRedactionService();
        var tokens = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Code"] = "123456"
        };

        var result = redactor.RedactForStorage(
            "TwoFactorCode",
            isSensitive: true,
            renderedSubject: "Your code is 123456",
            renderedBodyHtml: "<p>Use 123456 to sign in.</p>",
            tokens: tokens);

        Assert.True(result.RedactionApplied);
        Assert.DoesNotContain("123456", result.SubjectRedacted, StringComparison.Ordinal);
        Assert.DoesNotContain("123456", result.BodyHtmlRedacted, StringComparison.Ordinal);
    }

    [Fact]
    public void EmailRedactionService_RedactsOneTimeLinks()
    {
        var redactor = new EmailRedactionService();
        var url = "https://example.local/reset?token=super-secret";
        var tokens = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["ResetUrl"] = url
        };

        var result = redactor.RedactForStorage(
            "PasswordReset",
            isSensitive: true,
            renderedSubject: "Reset your password",
            renderedBodyHtml: $"<a href=\"{url}\">Reset password</a>",
            tokens: tokens);

        Assert.True(result.RedactionApplied);
        Assert.DoesNotContain(url, result.BodyHtmlRedacted, StringComparison.Ordinal);
        Assert.Contains("(one-time link redacted)", result.BodyHtmlRedacted, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SendGridWebhookController_InvalidSignature_ReturnsUnauthorized()
    {
        var validator = new RejectingSignatureValidator();
        var ingestion = new RecordingWebhookIngestionService();
        var controller = new SendGridWebhooksController(
            validator,
            ingestion,
            NullLogger<SendGridWebhooksController>.Instance);

        var context = new DefaultHttpContext();
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("[{\"event\":\"processed\"}]"));
        context.Request.Headers["X-Twilio-Email-Event-Webhook-Signature"] = "invalid";
        context.Request.Headers["X-Twilio-Email-Event-Webhook-Timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        controller.ControllerContext = new ControllerContext
        {
            HttpContext = context
        };

        var result = await controller.Events(CancellationToken.None);

        Assert.IsType<UnauthorizedResult>(result);
        Assert.Equal(0, ingestion.CallCount);
    }

    [Fact]
    public async Task SendGridWebhookIngestion_DuplicateEvent_IsIdempotent()
    {
        var logRepo = new FakeEmailLogRepository();
        var providerRepo = new FakeProviderEventRepository();
        var service = new SendGridWebhookIngestionService(
            logRepo,
            providerRepo,
            TimeProvider.System,
            NullLogger<SendGridWebhookIngestionService>.Instance);

        const string payload = "[{\"event\":\"delivered\",\"timestamp\":1730000000,\"sg_message_id\":\"msg-123\"}]";

        var first = await service.IngestAsync(payload, CancellationToken.None);
        var second = await service.IngestAsync(payload, CancellationToken.None);

        Assert.True(first.Success);
        Assert.True(second.Success);
        Assert.Equal(1, first.InsertedCount);
        Assert.Equal(0, second.InsertedCount);
        Assert.Equal(1, providerRepo.StoredCount);
    }

    private sealed class RejectingSignatureValidator : ISendGridWebhookSignatureValidator
    {
        public bool IsValid(string? timestampHeader, string? signatureHeader, ReadOnlySpan<byte> payloadUtf8) => false;
    }

    private sealed class RecordingWebhookIngestionService : ISendGridWebhookIngestionService
    {
        public int CallCount { get; private set; }

        public Task<SendGridWebhookIngestionResult> IngestAsync(string payloadJson, CancellationToken ct)
        {
            CallCount++;
            return Task.FromResult(new SendGridWebhookIngestionResult(true, 1, 1, null));
        }
    }

    private sealed class FakeEmailLogRepository : IEmailLogRepository
    {
        public Task<long> CreateQueuedAsync(EmailLogCreateRequest request, CancellationToken ct) => Task.FromResult(1L);

        public Task MarkSentAsync(long id, string? providerMessageId, DateTime nowUtc, CancellationToken ct) => Task.CompletedTask;

        public Task MarkFailedAsync(long id, string error, DateTime nowUtc, CancellationToken ct) => Task.CompletedTask;

        public Task<PagedResult<EmailLogListRow>> SearchAsync(EmailLogQuery query, CancellationToken ct) => Task.FromResult(new PagedResult<EmailLogListRow>([], 0));

        public Task<EmailLogDetailsRow?> GetDetailsAsync(long id, CancellationToken ct) => Task.FromResult<EmailLogDetailsRow?>(null);

        public Task<IReadOnlyList<EmailProviderEventRow>> ListEventsAsync(long emailLogId, CancellationToken ct) => Task.FromResult<IReadOnlyList<EmailProviderEventRow>>([]);

        public Task<long?> FindByProviderMessageIdAsync(string providerMessageId, CancellationToken ct)
            => Task.FromResult<long?>(providerMessageId == "msg-123" ? 42L : null);

        public Task UpdateLastProviderEventAsync(long emailLogId, string eventType, DateTime eventUtc, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeProviderEventRepository : IEmailProviderEventRepository
    {
        private readonly HashSet<string> keys = new(StringComparer.Ordinal);

        public int StoredCount => keys.Count;

        public Task<bool> InsertIfNotExistsAsync(EmailProviderEventCreateRequest request, CancellationToken ct)
        {
            var key = $"{request.Provider}|{request.ProviderMessageId}|{request.EventType}|{request.EventUtc:O}";
            return Task.FromResult(keys.Add(key));
        }
    }
}
