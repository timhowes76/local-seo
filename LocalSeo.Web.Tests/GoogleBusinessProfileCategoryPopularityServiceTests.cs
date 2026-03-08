using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class GoogleBusinessProfileCategoryPopularityServiceTests
{
    private readonly GoogleBusinessProfileCategoryPopularityService service = new();

    [Fact]
    public void MatchCandidates_MatchesExactNames_CaseInsensitively()
    {
        var result = service.MatchCandidates(
        [
            new GoogleBusinessProfileCategoryMatchCandidate("1", " plumber "),
            new GoogleBusinessProfileCategoryMatchCandidate("2", "Electrician")
        ]);

        Assert.Contains("Plumber", result.MatchedSourceNames);
        Assert.Contains("Electrician", result.MatchedSourceNames);
        Assert.Contains("1", result.MatchedCategoryIds);
        Assert.Contains("2", result.MatchedCategoryIds);
    }

    [Fact]
    public void MatchCandidates_MatchesNormalizedVariants()
    {
        var result = service.MatchCandidates(
        [
            new GoogleBusinessProfileCategoryMatchCandidate("1", "Doctor-General Practitioner"),
            new GoogleBusinessProfileCategoryMatchCandidate("2", "Fish & Chips Takeaway")
        ]);

        Assert.Contains("Doctor / General Practitioner", result.MatchedSourceNames);
        Assert.Contains("Fish and Chips Takeaway", result.MatchedSourceNames);
        Assert.Contains("1", result.MatchedCategoryIds);
        Assert.Contains("2", result.MatchedCategoryIds);
    }

    [Fact]
    public void MatchCandidates_UsesAliasMap_ForObviousVariants()
    {
        var result = service.MatchCandidates(
        [
            new GoogleBusinessProfileCategoryMatchCandidate("1", "GP"),
            new GoogleBusinessProfileCategoryMatchCandidate("2", "MOT Testing Service")
        ]);

        Assert.Contains("Doctor / General Practitioner", result.MatchedSourceNames);
        Assert.Contains("MOT Test Centre", result.MatchedSourceNames);
        Assert.Contains("1", result.MatchedCategoryIds);
        Assert.Contains("2", result.MatchedCategoryIds);
    }

    [Fact]
    public void MatchCandidates_ReportsUnmatchedSourceNames()
    {
        var result = service.MatchCandidates(
        [
            new GoogleBusinessProfileCategoryMatchCandidate("1", "Plumber")
        ]);

        Assert.Contains("Plumber", result.MatchedSourceNames);
        Assert.Contains("Electrician", result.UnmatchedSourceNames);
    }
}
