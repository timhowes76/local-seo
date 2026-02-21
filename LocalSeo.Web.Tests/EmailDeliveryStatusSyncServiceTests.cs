using System.Net;
using System.Net.Http;
using System.Collections.Generic;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace LocalSeo.Web.Tests;

public class EmailDeliveryStatusSyncServiceTests
{
    [Fact]
    public async Task RefreshPendingAsync_ListRow_Delivered_UpdatesLastProviderEvent()
    {
        var repository = new Mock<IEmailLogRepository>();
        repository
            .Setup(x => x.UpdateLastProviderEventAsync(42, "delivered", It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        string? requestPath = null;
        var handler = new StubHttpMessageHandler((request, _) =>
        {
            requestPath = request.RequestUri?.AbsolutePath;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"status\":\"delivered\",\"events\":[{\"event\":\"delivered\",\"timestamp\":1730000000}]}")
            });
        });

        var httpFactory = new Mock<IHttpClientFactory>();
        httpFactory
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient(handler));

        var service = CreateService(httpFactory.Object, repository.Object);
        var rows = new List<EmailLogListRow>
        {
            new()
            {
                Id = 42,
                CreatedUtc = DateTime.UtcNow,
                TemplateKey = "TwoFactorCode",
                ToEmail = "user@example.com",
                Status = "Sent",
                LastProviderEvent = null,
                SendGridMessageId = "msg-42"
            }
        };

        var refreshed = await service.RefreshPendingAsync(rows, CancellationToken.None);

        Assert.Equal(1, refreshed);
        Assert.Equal("/v3/logs/msg-42", requestPath);
        repository.Verify();
    }

    [Fact]
    public async Task RefreshPendingAsync_ListRow_FinalEvent_DoesNotCallSendGrid()
    {
        var repository = new Mock<IEmailLogRepository>();
        var httpFactory = new Mock<IHttpClientFactory>(MockBehavior.Strict);
        var service = CreateService(httpFactory.Object, repository.Object);

        var rows = new List<EmailLogListRow>
        {
            new()
            {
                Id = 100,
                CreatedUtc = DateTime.UtcNow,
                TemplateKey = "TwoFactorCode",
                ToEmail = "user@example.com",
                Status = "Sent",
                LastProviderEvent = "delivered",
                SendGridMessageId = "msg-100"
            }
        };

        var refreshed = await service.RefreshPendingAsync(rows, CancellationToken.None);

        Assert.Equal(0, refreshed);
        repository.Verify(x => x.UpdateLastProviderEventAsync(It.IsAny<long>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task RefreshPendingAsync_ListRow_Logs404_FallsBackToMessagesQuery()
    {
        var repository = new Mock<IEmailLogRepository>();
        repository
            .Setup(x => x.UpdateLastProviderEventAsync(55, "delivered", It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var requestUrls = new List<Uri>();
        var handler = new StubHttpMessageHandler((request, _) =>
        {
            requestUrls.Add(request.RequestUri!);
            if (request.RequestUri!.AbsolutePath.StartsWith("/v3/logs/", StringComparison.Ordinal))
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));

            if (request.RequestUri.AbsolutePath.Equals("/v3/messages", StringComparison.Ordinal))
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{\"messages\":[{\"msg_id\":\"9hxM5NiZShqPCNkd3F9xIA.some-suffix\",\"to_email\":\"user@example.com\",\"status\":\"delivered\",\"last_event_time\":\"2026-02-21T15:00:00Z\"}]}")
                });
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
        });

        var httpFactory = new Mock<IHttpClientFactory>();
        httpFactory
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient(handler));

        var service = CreateService(httpFactory.Object, repository.Object);
        var rows = new List<EmailLogListRow>
        {
            new()
            {
                Id = 55,
                CreatedUtc = DateTime.UtcNow,
                TemplateKey = "TwoFactorCode",
                ToEmail = "user@example.com",
                Status = "Sent",
                LastProviderEvent = null,
                SendGridMessageId = "9hxM5NiZShqPCNkd3F9xIA"
            }
        };

        var refreshed = await service.RefreshPendingAsync(rows, CancellationToken.None);

        Assert.Equal(1, refreshed);
        Assert.Equal(2, requestUrls.Count);
        Assert.Equal("/v3/logs/9hxM5NiZShqPCNkd3F9xIA", requestUrls[0].AbsolutePath);
        Assert.Equal("/v3/messages", requestUrls[1].AbsolutePath);
        Assert.Contains("msg_id%20LIKE%20%279hxM5NiZShqPCNkd3F9xIA%25%27", requestUrls[1].Query, StringComparison.Ordinal);
        repository.Verify();
    }

    [Fact]
    public async Task RefreshPendingAsync_DetailsRow_Bounced_MapsToBounce()
    {
        var repository = new Mock<IEmailLogRepository>();
        repository
            .Setup(x => x.UpdateLastProviderEventAsync(77, "bounce", It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var handler = new StubHttpMessageHandler((request, _) =>
        {
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"status\":\"bounced\",\"events\":[{\"event\":\"bounce\",\"timestamp\":1730000000}]}")
            });
        });

        var httpFactory = new Mock<IHttpClientFactory>();
        httpFactory
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient(handler));

        var service = CreateService(httpFactory.Object, repository.Object);
        var row = new EmailLogDetailsRow
        {
            Id = 77,
            CreatedUtc = DateTime.UtcNow,
            TemplateKey = "TwoFactorCode",
            ToEmail = "user@example.com",
            FromEmail = "noreply@example.com",
            SubjectRendered = "subject",
            BodyHtmlRendered = "<p>body</p>",
            IsSensitive = false,
            RedactionApplied = false,
            Status = "Sent",
            SendGridMessageId = "msg-77",
            LastProviderEvent = null
        };

        var refreshed = await service.RefreshPendingAsync(row, CancellationToken.None);

        Assert.True(refreshed);
        repository.Verify();
    }

    private static SendGridEmailDeliveryStatusSyncService CreateService(IHttpClientFactory httpFactory, IEmailLogRepository repository)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new SendGridOptions
        {
            ApiKey = "test-key"
        });

        return new SendGridEmailDeliveryStatusSyncService(
            httpFactory,
            options,
            repository,
            TimeProvider.System,
            NullLogger<SendGridEmailDeliveryStatusSyncService>.Instance);
    }

    private sealed class StubHttpMessageHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> callback) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => callback(request, cancellationToken);
    }
}
