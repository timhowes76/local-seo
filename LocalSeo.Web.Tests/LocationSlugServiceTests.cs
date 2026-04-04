using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public sealed class LocationSlugServiceTests
{
    private readonly LocationSlugService service = new();

    [Theory]
    [InlineData("Yeovil", "yeovil")]
    [InlineData("Weston super Mare", "weston-super-mare")]
    [InlineData("Bath & North East Somerset", "bath-and-north-east-somerset")]
    [InlineData("King's Lynn", "kings-lynn")]
    [InlineData("Caf\u00E9 Town", "cafe-town")]
    [InlineData("A/B Test Town", "a-b-test-town")]
    public void GenerateSlug_ReturnsExpectedSlug(string input, string expected)
    {
        var result = service.GenerateSlug(input);

        Assert.True(result.Success);
        Assert.Equal(expected, result.Value);
        Assert.Null(result.ErrorMessage);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(".")]
    [InlineData("..")]
    public void GenerateSlug_RejectsInvalidInput(string input)
    {
        var result = service.GenerateSlug(input);

        Assert.False(result.Success);
        Assert.Null(result.Value);
        Assert.False(string.IsNullOrWhiteSpace(result.ErrorMessage));
    }

    [Fact]
    public void NormalizeOptionalSlug_SanitizesManualSlug()
    {
        var result = service.NormalizeOptionalSlug("  Bath & North East Somerset  ");

        Assert.True(result.Success);
        Assert.Equal("bath-and-north-east-somerset", result.Value);
    }

    [Fact]
    public void NormalizeOptionalSlug_AllowsBlankValue()
    {
        var result = service.NormalizeOptionalSlug("   ");

        Assert.True(result.Success);
        Assert.Null(result.Value);
    }

    [Fact]
    public void NormalizeOptionalSlug_RejectsSlugThatCleansToEmpty()
    {
        var result = service.NormalizeOptionalSlug("///");

        Assert.False(result.Success);
        Assert.Null(result.Value);
        Assert.False(string.IsNullOrWhiteSpace(result.ErrorMessage));
    }
}
