using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IWebsiteClassifier
{
    WebsiteType Classify(string? websiteUrl);
}

public sealed class WebsiteClassifier : IWebsiteClassifier
{
    private static readonly string[] SocialDomains =
    [
        "facebook.com",
        "m.facebook.com",
        "fb.com",
        "instagram.com",
        "linkedin.com",
        "lnk.bio",
        "linktr.ee",
        "x.com",
        "twitter.com",
        "tiktok.com",
        "youtube.com",
        "youtu.be",
        "pinterest.com",
        "pin.it",
        "snapchat.com",
        "threads.net",
        "whatsapp.com",
        "wa.me",
        "telegram.me",
        "t.me"
    ];

    public WebsiteType Classify(string? websiteUrl)
    {
        var normalized = (websiteUrl ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return WebsiteType.None;

        var host = TryExtractHost(normalized);
        if (host is null)
            return WebsiteType.RealWebsite;

        return IsSocialHost(host)
            ? WebsiteType.SocialProfile
            : WebsiteType.RealWebsite;
    }

    private static bool IsSocialHost(string host)
    {
        foreach (var socialDomain in SocialDomains)
        {
            if (host.Equals(socialDomain, StringComparison.OrdinalIgnoreCase)
                || host.EndsWith("." + socialDomain, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string? TryExtractHost(string value)
    {
        if (TryParseAbsoluteHttpUri(value, out var uri) && !string.IsNullOrWhiteSpace(uri.Host))
            return NormalizeHost(uri.Host);

        var hostCandidate = value;
        if (hostCandidate.StartsWith("//", StringComparison.Ordinal))
            hostCandidate = hostCandidate[2..];

        var schemeIndex = hostCandidate.IndexOf("://", StringComparison.Ordinal);
        if (schemeIndex >= 0 && schemeIndex + 3 < hostCandidate.Length)
            hostCandidate = hostCandidate[(schemeIndex + 3)..];

        var slashIndex = hostCandidate.IndexOfAny(['/', '?', '#']);
        if (slashIndex >= 0)
            hostCandidate = hostCandidate[..slashIndex];

        var atIndex = hostCandidate.LastIndexOf('@');
        if (atIndex >= 0 && atIndex + 1 < hostCandidate.Length)
            hostCandidate = hostCandidate[(atIndex + 1)..];

        var colonIndex = hostCandidate.IndexOf(':');
        if (colonIndex >= 0)
            hostCandidate = hostCandidate[..colonIndex];

        hostCandidate = NormalizeHost(hostCandidate);
        return hostCandidate?.Length > 0 ? hostCandidate : null;
    }

    private static bool TryParseAbsoluteHttpUri(string value, out Uri uri)
    {
        uri = null!;

        if (Uri.TryCreate(value, UriKind.Absolute, out var parsedUri) && IsHttpScheme(parsedUri))
        {
            uri = parsedUri;
            return true;
        }

        if (value.Contains("://", StringComparison.Ordinal))
            return false;

        if (Uri.TryCreate("https://" + value, UriKind.Absolute, out parsedUri) && IsHttpScheme(parsedUri))
        {
            uri = parsedUri;
            return true;
        }

        return false;
    }

    private static bool IsHttpScheme(Uri uri)
    {
        return string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            || string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);
    }

    private static string? NormalizeHost(string? host)
    {
        var normalized = (host ?? string.Empty).Trim().Trim('.').ToLowerInvariant();
        return normalized.Length == 0 ? null : normalized;
    }
}
