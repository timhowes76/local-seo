using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class WebsiteClassifierTests
{
    private readonly WebsiteClassifier classifier = new();

    [Theory]
    [InlineData("https://facebook.com/test")]
    [InlineData("https://m.facebook.com/profile.php/?id=123")]
    [InlineData("http://www.instagram.com/mybiz")]
    [InlineData("https://linkedin.com/company/test")]
    [InlineData("https://x.com/test")]
    [InlineData("https://linktr.ee/test")]
    [InlineData("facebook.com/test")]
    [InlineData("www.youtube.com/@test")]
    public void Classify_ReturnsSocialProfile_ForKnownSocialUrls(string? value)
    {
        Assert.Equal(WebsiteType.SocialProfile, classifier.Classify(value));
    }

    [Theory]
    [InlineData("https://www.kontrolit.net")]
    [InlineData("https://example.co.uk")]
    [InlineData("http://shop.example.com")]
    [InlineData("www.mysite.com")]
    public void Classify_ReturnsRealWebsite_ForBusinessSites(string? value)
    {
        Assert.Equal(WebsiteType.RealWebsite, classifier.Classify(value));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Classify_ReturnsNone_ForBlankValues(string? value)
    {
        Assert.Equal(WebsiteType.None, classifier.Classify(value));
    }

    [Theory]
    [InlineData("https://notfacebook.com")]
    [InlineData("https://myinstagramagency.co.uk")]
    public void Classify_DoesNotFalsePositive_OnLookalikeDomains(string? value)
    {
        Assert.Equal(WebsiteType.RealWebsite, classifier.Classify(value));
    }
}
