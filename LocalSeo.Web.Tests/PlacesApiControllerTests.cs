using LocalSeo.Web.Controllers;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace LocalSeo.Web.Tests;

public class PlacesApiControllerTests
{
    [Fact]
    public async Task GetReviewVelocity_ReturnsNotFound_WhenMissing()
    {
        var svc = new Mock<IReviewVelocityService>();
        svc.Setup(x => x.GetPlaceReviewVelocityAsync("p1", It.IsAny<CancellationToken>()))
            .ReturnsAsync((PlaceReviewVelocityDetailsDto?)null);

        var controller = new PlacesApiController(svc.Object);
        var result = await controller.GetReviewVelocity("p1", CancellationToken.None);

        Assert.IsType<NotFoundResult>(result);
    }
}
