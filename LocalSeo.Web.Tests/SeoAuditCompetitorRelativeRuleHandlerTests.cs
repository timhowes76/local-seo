using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class SeoAuditCompetitorRelativeRuleHandlerTests
{
    private readonly CompetitorRelativeSeoAuditRuleHandler handler = new();

    [Fact]
    public void Evaluate_TownCentreDistanceRelative_ReturnsPass_WhenPlaceIsNearest()
    {
        var result = handler.Evaluate(
            CreateRule(),
            CreateContext(
                "target",
                0.009m,
                0m,
                [
                    new PlaceAuditPeer("target", 0.009m, 0m),
                    new PlaceAuditPeer("peer-2", 0.018m, 0m),
                    new PlaceAuditPeer("peer-3", 0.027m, 0m)
                ]));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
        Assert.Contains("1st nearest of 3", result.GapValue, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_TownCentreDistanceRelative_ReturnsJointNearest_WhenDistanceMatchesExactly()
    {
        var result = handler.Evaluate(
            CreateRule(),
            CreateContext(
                "target",
                0.009m,
                0m,
                [
                    new PlaceAuditPeer("target", 0.009m, 0m),
                    new PlaceAuditPeer("peer-2", 0.009m, 0m),
                    new PlaceAuditPeer("peer-3", 0.027m, 0m)
                ]));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Contains("Joint 1st nearest of 3", result.GapValue, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_TownCentreDistanceRelative_ReturnsWarning_ForMiddleCohort()
    {
        var result = handler.Evaluate(
            CreateRule(),
            CreateContext(
                "target",
                0.018m,
                0m,
                [
                    new PlaceAuditPeer("peer-1", 0.009m, 0m),
                    new PlaceAuditPeer("target", 0.018m, 0m),
                    new PlaceAuditPeer("peer-3", 0.027m, 0m)
                ]));

        Assert.Equal(SeoAuditStatuses.Warning, result.Status);
        Assert.InRange(result.ScoreImpactApplied, 3, 4);
        Assert.Contains("2nd nearest of 3", result.GapValue, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_TownCentreDistanceRelative_ReturnsNotApplicable_WhenCoordinatesMissing()
    {
        var result = handler.Evaluate(
            CreateRule(),
            CreateContext(
                "target",
                null,
                null,
                [
                    new PlaceAuditPeer("target", null, null),
                    new PlaceAuditPeer("peer-2", 0.018m, 0m)
                ]));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    private static SeoAuditRuleDefinition CreateRule()
    {
        return new SeoAuditRuleDefinition(
            99,
            SeoAuditRuleKeys.TownCentreDistanceRelative,
            "Distance from town centre",
            string.Empty,
            "Location Context",
            SeoAuditRuleModes.CompetitorRelative,
            SeoAuditRuleTypes.TownCentreDistanceRank,
            SeoAuditEntityTypes.GbpProfile,
            SeoAuditSeverityLevels.Info,
            3,
            5,
            240,
            true,
            true,
            "Why",
            string.Empty,
            DateTime.UtcNow,
            DateTime.UtcNow,
            []);
    }

    private static PlaceAuditContext CreateContext(
        string placeId,
        decimal? placeLat,
        decimal? placeLng,
        IReadOnlyList<PlaceAuditPeer> comparablePlaces)
    {
        return new PlaceAuditContext(
            placeId,
            "Business Name",
            null,
            "Description",
            null,
            "https://example.com",
            WebsiteType.RealWebsite,
            1,
            0,
            "[]",
            "[]",
            placeLat,
            placeLng,
            0m,
            0m,
            null,
            null,
            null,
            4.5m,
            10,
            123,
            0,
            0,
            0,
            0,
            [],
            [],
            [],
            [],
            comparablePlaces);
    }
}
