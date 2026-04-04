using System.Globalization;
using System.Text;

namespace LocalSeo.Web.Services;

public interface ILocationSlugService
{
    SlugNormalizationResult GenerateSlug(string? value);
    SlugNormalizationResult NormalizeOptionalSlug(string? value);
}

public readonly record struct SlugNormalizationResult(
    bool Success,
    string? Value,
    string? ErrorMessage);

public sealed class LocationSlugService : ILocationSlugService
{
    public SlugNormalizationResult GenerateSlug(string? value)
    {
        var normalized = NormalizeCore(value);
        if (string.IsNullOrWhiteSpace(normalized.Input))
            return Invalid("Name is required to generate a slug.");

        if (normalized.IsDotSegment)
            return Invalid("Slug cannot be '.' or '..'.");

        if (string.IsNullOrWhiteSpace(normalized.Slug))
            return Invalid("Slug must contain at least one letter or number.");

        return Valid(normalized.Slug);
    }

    public SlugNormalizationResult NormalizeOptionalSlug(string? value)
    {
        var normalized = NormalizeCore(value);
        if (string.IsNullOrWhiteSpace(normalized.Input))
            return Valid(null);

        if (normalized.IsDotSegment)
            return Invalid("Slug cannot be '.' or '..'.");

        if (string.IsNullOrWhiteSpace(normalized.Slug))
            return Invalid("Slug must contain at least one letter or number.");

        return Valid(normalized.Slug);
    }

    private static (string Input, string Slug, bool IsDotSegment) NormalizeCore(string? value)
    {
        var input = (value ?? string.Empty).Trim();
        if (input.Length == 0)
            return (string.Empty, string.Empty, false);

        var slug = BuildSlug(input);
        var isDotSegment = slug is "." or ".." || input is "." or "..";
        return (input, slug, isDotSegment);
    }

    private static string BuildSlug(string value)
    {
        var source = value
            .Replace("&", " and ", StringComparison.Ordinal)
            .Replace("'", string.Empty, StringComparison.Ordinal)
            .Replace("\"", string.Empty, StringComparison.Ordinal)
            .Replace("`", string.Empty, StringComparison.Ordinal)
            .Replace("\u2019", string.Empty, StringComparison.Ordinal)
            .Replace("\u2018", string.Empty, StringComparison.Ordinal)
            .Replace("\u201A", string.Empty, StringComparison.Ordinal)
            .Replace("\u201B", string.Empty, StringComparison.Ordinal)
            .Replace("\u201C", string.Empty, StringComparison.Ordinal)
            .Replace("\u201D", string.Empty, StringComparison.Ordinal)
            .Replace("\u201E", string.Empty, StringComparison.Ordinal)
            .Replace("\u201F", string.Empty, StringComparison.Ordinal)
            .Replace("\u00DF", "ss", StringComparison.Ordinal)
            .Replace("\u00C6", "AE", StringComparison.Ordinal)
            .Replace("\u00E6", "ae", StringComparison.Ordinal)
            .Replace("\u0152", "OE", StringComparison.Ordinal)
            .Replace("\u0153", "oe", StringComparison.Ordinal)
            .Replace("\u00D8", "O", StringComparison.Ordinal)
            .Replace("\u00F8", "o", StringComparison.Ordinal)
            .Replace("\u00D0", "D", StringComparison.Ordinal)
            .Replace("\u00F0", "d", StringComparison.Ordinal)
            .Replace("\u00DE", "TH", StringComparison.Ordinal)
            .Replace("\u00FE", "th", StringComparison.Ordinal)
            .Replace("\u0141", "L", StringComparison.Ordinal)
            .Replace("\u0142", "l", StringComparison.Ordinal)
            .Normalize(NormalizationForm.FormD);

        var builder = new StringBuilder(source.Length);
        var previousWasHyphen = false;

        foreach (var character in source)
        {
            var category = CharUnicodeInfo.GetUnicodeCategory(character);
            if (category == UnicodeCategory.NonSpacingMark)
                continue;

            var lower = char.ToLowerInvariant(character);
            if ((lower >= 'a' && lower <= 'z') || (lower >= '0' && lower <= '9'))
            {
                builder.Append(lower);
                previousWasHyphen = false;
                continue;
            }

            if (builder.Length == 0 || previousWasHyphen)
                continue;

            if (char.IsWhiteSpace(lower)
                || char.IsPunctuation(lower)
                || char.IsSeparator(lower)
                || char.IsSymbol(lower)
                || lower == '_')
            {
                builder.Append('-');
                previousWasHyphen = true;
            }
        }

        return builder
            .ToString()
            .Trim('-');
    }

    private static SlugNormalizationResult Valid(string? value) => new(true, value, null);

    private static SlugNormalizationResult Invalid(string errorMessage) => new(false, null, errorMessage);
}
