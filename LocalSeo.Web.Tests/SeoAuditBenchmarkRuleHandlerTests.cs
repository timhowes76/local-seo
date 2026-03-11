using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class SeoAuditBenchmarkRuleHandlerTests
{
    private readonly BenchmarkSeoAuditRuleHandler handler = new();

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsPass_WhenAllTermsMatch()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("Yeovil Web Designer Ltd", null, "1 High Street, Yeovil, Somerset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
        Assert.Contains("good keyword match", result.SummaryText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsWarning_WhenPartialTermsMatch()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("Acme Web Studio", null, "1 High Street, Yeovil, Somerset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Warning, result.Status);
        Assert.Equal(6, result.ScoreImpactApplied);
        Assert.Contains("Service terms: web", result.ActualValue, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsPass_WhenCloseServiceVariantMatches()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("CK Website Design", null, "1 High Street, Yeovil, Somerset", "website_designer", "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
    }

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsFail_WhenNoTermsMatch()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("Acme Digital Ltd", null, "1 High Street, Yeovil, Somerset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(9, result.ScoreImpactApplied);
        Assert.Contains("does not match", result.SummaryText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsFail_WhenOnlyGenericCreativeTermAppears()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("CK Creative", null, "1 High Street, Yeovil, Somerset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(9, result.ScoreImpactApplied);
    }

    [Fact]
    public void Evaluate_KeywordsInBusinessTitle_ReturnsNotApplicable_WhenRunContextMissing()
    {
        var result = handler.Evaluate(
            CreateBusinessTitleRule(),
            CreateContext("Acme Digital Ltd", null, "1 High Street, Yeovil, Somerset", null, null, "Yeovil"));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    [Fact]
    public void Evaluate_PrimaryCategoryMatchesRun_ReturnsPass_WhenCategoryMatchesExactly()
    {
        var result = handler.Evaluate(
            CreatePrimaryCategoryRule(),
            CreateContext("Business Name", "Web Designer", "1 High Street, Yeovil, Somerset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
        Assert.Equal("Exact match", result.GapValue);
    }

    [Fact]
    public void Evaluate_PrimaryCategoryMatchesRun_ReturnsPass_WhenCategoryMatchesCanonicalCategoryIdLabel()
    {
        var result = handler.Evaluate(
            CreatePrimaryCategoryRule(),
            CreateContext("Business Name", "Website Designer", "1 High Street, Yeovil, Somerset", "website_designer", "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
    }

    [Fact]
    public void Evaluate_PrimaryCategoryMatchesRun_ReturnsFail_WhenCategoryDoesNotMatch()
    {
        var result = handler.Evaluate(
            CreatePrimaryCategoryRule(),
            CreateContext("Business Name", "Marketing Agency", "1 High Street, Yeovil, Somerset", "website_designer", "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(10, result.ScoreImpactApplied);
        Assert.Equal("Mismatch", result.GapValue);
    }

    [Fact]
    public void Evaluate_PrimaryCategoryMatchesRun_ReturnsNotApplicable_WhenPrimaryCategoryMissing()
    {
        var result = handler.Evaluate(
            CreatePrimaryCategoryRule(),
            CreateContext("Business Name", null, "1 High Street, Yeovil, Somerset", "website_designer", "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    [Fact]
    public void Evaluate_PrimaryCategoryMatchesRun_ReturnsNotApplicable_WhenRunCategoryMissing()
    {
        var result = handler.Evaluate(
            CreatePrimaryCategoryRule(),
            CreateContext("Business Name", "Web Designer", "1 High Street, Yeovil, Somerset", null, null, "Yeovil"));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsPass_WhenTownAppearsInAddress()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "1 High Street, Yeovil BA20 1AA, UK", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
        Assert.Equal("Primary locality matches search town", result.GapValue);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsPass_WhenBuildingAndStreetPrecedeTown()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "The Abbey, Preston Rd, Yeovil BA20 2EN, UK", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Pass, result.Status);
        Assert.Equal(0, result.ScoreImpactApplied);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsFail_WhenTownDoesNotAppearInAddress()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "1 Market Square, Sherborne, Dorset", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(8, result.ScoreImpactApplied);
        Assert.Equal("Town not found in address", result.GapValue);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsFail_WhenPostalTownAppearsButLocalityIsDifferent()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "8 Yeovil Rd, Tintinhull, Yeovil BA22 8QL, UK", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(8, result.ScoreImpactApplied);
        Assert.Contains("Tintinhull", result.GapValue, StringComparison.Ordinal);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsFail_WhenAnotherLocalityAppearsAfterPostalTown()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "East Stoke, Yeovil, Stoke-sub-Hamdon TA14 6UQ, UK", null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.Fail, result.Status);
        Assert.Equal(8, result.ScoreImpactApplied);
        Assert.Contains("Different locality", result.GapValue, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsNotApplicable_WhenAddressMissing()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", null, null, "Web Designer", "Yeovil"));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    [Fact]
    public void Evaluate_PhysicalAddressMatchesSearchTown_ReturnsNotApplicable_WhenTownMissing()
    {
        var result = handler.Evaluate(
            CreatePhysicalAddressRule(),
            CreateContext("Business Name", "Web Designer", "1 High Street, Yeovil, Somerset", null, "Web Designer", null));

        Assert.Equal(SeoAuditStatuses.NotApplicable, result.Status);
    }

    private static SeoAuditRuleDefinition CreateBusinessTitleRule()
    {
        return new SeoAuditRuleDefinition(
            100,
            SeoAuditRuleKeys.KeywordsInBusinessTitle,
            "Keywords in business title",
            string.Empty,
            "Location Context",
            SeoAuditRuleModes.Benchmark,
            SeoAuditRuleTypes.BusinessTitleKeywordMatch,
            SeoAuditEntityTypes.GbpProfile,
            SeoAuditSeverityLevels.Info,
            6,
            9,
            250,
            true,
            true,
            "Why",
            string.Empty,
            DateTime.UtcNow,
            DateTime.UtcNow,
            []);
    }

    private static SeoAuditRuleDefinition CreatePrimaryCategoryRule()
    {
        return new SeoAuditRuleDefinition(
            101,
            SeoAuditRuleKeys.PrimaryCategoryMatchesRun,
            "Primary category matches run",
            string.Empty,
            "Categories",
            SeoAuditRuleModes.Benchmark,
            SeoAuditRuleTypes.PrimaryCategoryMatch,
            SeoAuditEntityTypes.GbpCategories,
            SeoAuditSeverityLevels.Critical,
            0,
            10,
            260,
            true,
            true,
            "Why",
            "Action",
            DateTime.UtcNow,
            DateTime.UtcNow,
            []);
    }

    private static SeoAuditRuleDefinition CreatePhysicalAddressRule()
    {
        return new SeoAuditRuleDefinition(
            102,
            SeoAuditRuleKeys.PhysicalAddressMatchesSearchTown,
            "Physical address in search town",
            string.Empty,
            "Location Context",
            SeoAuditRuleModes.Benchmark,
            SeoAuditRuleTypes.PhysicalAddressInSearchTown,
            SeoAuditEntityTypes.GbpProfile,
            SeoAuditSeverityLevels.Info,
            0,
            8,
            270,
            true,
            true,
            "Why",
            string.Empty,
            DateTime.UtcNow,
            DateTime.UtcNow,
            []);
    }

    private static PlaceAuditContext CreateContext(string? displayName, string? primaryCategory, string? formattedAddress, string? sourceCategoryId, string? sourceKeyword, string? sourceTownName)
    {
        return new PlaceAuditContext(
            "place-1",
            displayName,
            primaryCategory,
            "Description",
            formattedAddress,
            "https://example.com",
            WebsiteType.RealWebsite,
            1,
            0,
            "[]",
            "[]",
            null,
            null,
            null,
            null,
            sourceCategoryId,
            sourceKeyword,
            sourceTownName,
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
            []);
    }
}
