using System.Security.Cryptography;
using System.Text;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record AvatarViewModel(string? ImageUrl, string Initials, bool UsesGravatar);

public interface IAvatarResolver
{
    AvatarViewModel Resolve(UserRecord? user, int size = 64);
    string NormalizeEmailForGravatar(string? emailAddress);
    string ComputeGravatarHash(string? emailAddress);
}

public sealed class AvatarResolver : IAvatarResolver
{
    public AvatarViewModel Resolve(UserRecord? user, int size = 64)
    {
        var boundedSize = Math.Clamp(size, 16, 512);
        var initials = BuildInitials(user?.FirstName, user?.LastName, user?.EmailAddress);

        if (user is not null && user.UseGravatar)
        {
            var hash = ComputeGravatarHash(user.EmailAddress);
            var gravatarUrl = $"https://www.gravatar.com/avatar/{hash}?d=identicon&s={boundedSize}";
            return new AvatarViewModel(gravatarUrl, initials, true);
        }

        return new AvatarViewModel(null, initials, false);
    }

    public string NormalizeEmailForGravatar(string? emailAddress)
    {
        return (emailAddress ?? string.Empty).Trim().ToLowerInvariant();
    }

    public string ComputeGravatarHash(string? emailAddress)
    {
        var normalized = NormalizeEmailForGravatar(emailAddress);
        var bytes = Encoding.UTF8.GetBytes(normalized);
        var hashBytes = MD5.HashData(bytes);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }

    private static string BuildInitials(string? firstName, string? lastName, string? emailAddress)
    {
        var first = ExtractInitial(firstName);
        var last = ExtractInitial(lastName);
        if (first.Length == 1 && last.Length == 1)
            return $"{first}{last}";
        if (first.Length == 1)
            return first;
        if (last.Length == 1)
            return last;

        var email = (emailAddress ?? string.Empty).Trim();
        if (email.Length > 0)
            return ExtractInitial(email);

        return "U";
    }

    private static string ExtractInitial(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return string.Empty;
        return char.ToUpperInvariant(trimmed[0]).ToString();
    }
}
