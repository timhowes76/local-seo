using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public class ReviewVelocityLogicTests
{
    [Theory]
    [InlineData(-100, 0)]
    [InlineData(0, 50)]
    [InlineData(100, 75)]
    [InlineData(200, 100)]
    [InlineData(300, 100)]
    public void MapGrowthScore_MapsExpectedCurve(decimal trend, int expected)
    {
        var score = ReviewVelocityLogic.MapGrowthScore(trend);
        Assert.Equal(expected, score);
    }
}
