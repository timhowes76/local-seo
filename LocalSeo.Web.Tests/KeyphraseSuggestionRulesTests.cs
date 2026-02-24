using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public class KeyphraseSuggestionRulesTests
{
    [Fact]
    public void NormalizeAndFilterSuggestions_DedupesByNormalizedKeyword()
    {
        var rows = new List<KeyphraseSuggestionItem>
        {
            new() { Keyword = "Best plumber Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.9m },
            new() { Keyword = "best-plumber   Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.8m },
            new() { Keyword = "Emergency plumber Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.7m }
        };

        var result = KeyphraseSuggestionRules.NormalizeAndFilterSuggestions(rows, "Yeovil", "Plumber Yeovil", 20);

        Assert.Equal(2, result.Count);
        Assert.Contains(result, x => x.Keyword == "Best plumber Yeovil");
        Assert.Contains(result, x => x.Keyword == "Emergency plumber Yeovil");
    }

    [Fact]
    public void NormalizeAndFilterSuggestions_RequiresLocationToken()
    {
        var rows = new List<KeyphraseSuggestionItem>
        {
            new() { Keyword = "best plumber", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.9m },
            new() { Keyword = "best plumber Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.8m }
        };

        var result = KeyphraseSuggestionRules.NormalizeAndFilterSuggestions(rows, "Yeovil", "Plumber Yeovil", 20);

        Assert.Single(result);
        Assert.Equal("best plumber Yeovil", result[0].Keyword);
    }

    [Fact]
    public void NormalizeAndFilterSuggestions_ExcludesMainKeyword()
    {
        var rows = new List<KeyphraseSuggestionItem>
        {
            new() { Keyword = "Plumber Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.9m },
            new() { Keyword = "Affordable plumber Yeovil", KeywordType = CategoryLocationKeywordTypes.Modifier, Confidence = 0.8m }
        };

        var result = KeyphraseSuggestionRules.NormalizeAndFilterSuggestions(rows, "Yeovil", "Plumber Yeovil", 20);

        Assert.Single(result);
        Assert.Equal("Affordable plumber Yeovil", result[0].Keyword);
    }
}
