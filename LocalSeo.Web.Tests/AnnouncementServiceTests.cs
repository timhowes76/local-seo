using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace LocalSeo.Web.Tests;

public sealed class AnnouncementServiceTests
{
    [Fact]
    public async Task CreateAsync_PreservesMultiLineEditorHtml()
    {
        var repository = new Mock<IAnnouncementRepository>();
        string? persistedBodyHtml = null;

        repository
            .Setup(x => x.CreateAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int?>(),
                It.IsAny<DateTime>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, int?, DateTime, CancellationToken>((_, bodyHtml, _, _, _) => persistedBodyHtml = bodyHtml)
            .ReturnsAsync(42L);

        var service = new AnnouncementService(
            repository.Object,
            new AnnouncementHtmlSanitizer(),
            TimeProvider.System,
            NullLogger<AnnouncementService>.Instance);

        var result = await service.CreateAsync(
            new AnnouncementEditModel
            {
                Title = "Service update",
                BodyHtml = "Line 1<div>Line 2</div><div>Line 3</div>"
            },
            actorUserId: 7,
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.Equal(42L, result.AnnouncementId);
        Assert.NotNull(persistedBodyHtml);
        Assert.Contains("Line 1", persistedBodyHtml, StringComparison.Ordinal);
        Assert.Contains("Line 2", persistedBodyHtml, StringComparison.Ordinal);
        Assert.Contains("Line 3", persistedBodyHtml, StringComparison.Ordinal);
    }
}
