using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class ReportScoringTests
{
    [Fact]
    public void ScoreRelative_ReturnsStrong_ForHighestValue()
    {
        var values = new Dictionary<string, decimal?>
        {
            ["you"] = 90m,
            ["a"] = 50m,
            ["b"] = 40m,
            ["c"] = 30m
        };

        var result = ReportScoring.ScoreRelative(values, "you");

        Assert.Equal(StrengthLevel.Strong, result);
    }

    [Fact]
    public void ScoreRelative_ReturnsWeak_ForLowestValue()
    {
        var values = new Dictionary<string, decimal?>
        {
            ["you"] = 10m,
            ["a"] = 20m,
            ["b"] = 30m,
            ["c"] = 40m
        };

        var result = ReportScoring.ScoreRelative(values, "you");

        Assert.Equal(StrengthLevel.Weak, result);
    }

    [Fact]
    public void ScoreRelative_UsesLowerIsBetter_WhenRequested()
    {
        var values = new Dictionary<string, decimal?>
        {
            ["you"] = 3m,
            ["a"] = 10m,
            ["b"] = 12m,
            ["c"] = 15m
        };

        var result = ReportScoring.ScoreRelative(values, "you", lowerIsBetter: true);

        Assert.Equal(StrengthLevel.Strong, result);
    }

    [Fact]
    public void ScoreRelative_ReturnsUnknown_WhenTargetMissing()
    {
        var values = new Dictionary<string, decimal?>
        {
            ["a"] = 1m
        };

        var result = ReportScoring.ScoreRelative(values, "you");

        Assert.Equal(StrengthLevel.Unknown, result);
    }
}
