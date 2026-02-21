using System.Text.RegularExpressions;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record EmailRenderResult(string RenderedText, IReadOnlyList<string> UnknownTokens);

public interface IEmailTemplateRenderer
{
    EmailRenderResult Render(string templateText, IReadOnlyDictionary<string, string> tokens);
}

public sealed class EmailTemplateRenderer : IEmailTemplateRenderer
{
    private static readonly Regex TokenRegex = new(@"\[\%(?<name>[^\]%]+)\%\]", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public EmailRenderResult Render(string templateText, IReadOnlyDictionary<string, string> tokens)
    {
        var source = templateText ?? string.Empty;
        var normalizedTokens = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in tokens)
        {
            var key = (pair.Key ?? string.Empty).Trim();
            if (key.Length == 0)
                continue;
            normalizedTokens[key] = pair.Value ?? string.Empty;
        }

        var unknown = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var rendered = TokenRegex.Replace(source, match =>
        {
            var tokenName = (match.Groups["name"].Value ?? string.Empty).Trim();
            if (tokenName.Length == 0)
                return match.Value;

            if (normalizedTokens.TryGetValue(tokenName, out var value))
                return value;

            unknown.Add(tokenName);
            return match.Value;
        });

        return new EmailRenderResult(rendered, unknown.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList());
    }
}

public sealed record EmailRedactionResult(string SubjectRedacted, string BodyHtmlRedacted, bool RedactionApplied);

public interface IEmailRedactionService
{
    EmailRedactionResult RedactForStorage(string templateKey, bool isSensitive, string renderedSubject, string renderedBodyHtml, IReadOnlyDictionary<string, string> tokens);
}

public sealed class EmailRedactionService : IEmailRedactionService
{
    private const string CodeMask = "******";
    private const string OneTimeLinkMask = "(one-time link redacted)";

    private static readonly Regex DigitsFallbackRegex = new(@"(?<!\d)\d{6,8}(?!\d)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex OneTimeLinkFallbackRegex = new(@"https?:\/\/[^\s""'<>]*(token=|code=|state=|sig=)[^\s""'<>]*", RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    public EmailRedactionResult RedactForStorage(string templateKey, bool isSensitive, string renderedSubject, string renderedBodyHtml, IReadOnlyDictionary<string, string> tokens)
    {
        var subject = renderedSubject ?? string.Empty;
        var body = renderedBodyHtml ?? string.Empty;
        if (!isSensitive)
            return new EmailRedactionResult(subject, body, false);

        var redactionApplied = false;
        var normalizedTokens = new Dictionary<string, string>(tokens, StringComparer.OrdinalIgnoreCase);
        var key = (templateKey ?? string.Empty).Trim();

        if (key.Equals("TwoFactorCode", StringComparison.OrdinalIgnoreCase)
            || key.Equals("InviteOtp", StringComparison.OrdinalIgnoreCase)
            || key.Equals("ChangePasswordOtp", StringComparison.OrdinalIgnoreCase))
        {
            if (normalizedTokens.TryGetValue("Code", out var code) && !string.IsNullOrWhiteSpace(code))
            {
                var beforeSubject = subject;
                var beforeBody = body;
                subject = ReplaceInvariant(subject, code, CodeMask);
                body = ReplaceInvariant(body, code, CodeMask);
                redactionApplied = redactionApplied
                                   || !string.Equals(beforeSubject, subject, StringComparison.Ordinal)
                                   || !string.Equals(beforeBody, body, StringComparison.Ordinal);
            }

            var fallbackSubject = DigitsFallbackRegex.Replace(subject, CodeMask);
            var fallbackBody = DigitsFallbackRegex.Replace(body, CodeMask);
            if (!string.Equals(subject, fallbackSubject, StringComparison.Ordinal) || !string.Equals(body, fallbackBody, StringComparison.Ordinal))
                redactionApplied = true;
            subject = fallbackSubject;
            body = fallbackBody;
        }
        else if (key.Equals("PasswordReset", StringComparison.OrdinalIgnoreCase)
                 || key.Equals("NewUserInvite", StringComparison.OrdinalIgnoreCase))
        {
            var urlTokenKeys = new[] { "ResetUrl", "InviteUrl", "Link" };
            foreach (var tokenKey in urlTokenKeys)
            {
                if (!normalizedTokens.TryGetValue(tokenKey, out var tokenValue))
                    continue;
                if (string.IsNullOrWhiteSpace(tokenValue))
                    continue;

                var beforeSubject = subject;
                var beforeBody = body;
                subject = ReplaceInvariant(subject, tokenValue, OneTimeLinkMask);
                body = ReplaceInvariant(body, tokenValue, OneTimeLinkMask);
                redactionApplied = redactionApplied
                                   || !string.Equals(beforeSubject, subject, StringComparison.Ordinal)
                                   || !string.Equals(beforeBody, body, StringComparison.Ordinal);
            }

            var fallbackSubject = OneTimeLinkFallbackRegex.Replace(subject, OneTimeLinkMask);
            var fallbackBody = OneTimeLinkFallbackRegex.Replace(body, OneTimeLinkMask);
            if (!string.Equals(subject, fallbackSubject, StringComparison.Ordinal) || !string.Equals(body, fallbackBody, StringComparison.Ordinal))
                redactionApplied = true;
            subject = fallbackSubject;
            body = fallbackBody;
        }

        return new EmailRedactionResult(subject, body, redactionApplied);
    }

    private static string ReplaceInvariant(string source, string value, string replacement)
    {
        if (string.IsNullOrEmpty(source) || string.IsNullOrEmpty(value))
            return source;
        return Regex.Replace(source, Regex.Escape(value), replacement, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }
}

public interface IEmailTokenFactory
{
    IReadOnlyDictionary<string, string> BuildTwoFactorCodeTokens(string code, int expiryMinutes);
    IReadOnlyDictionary<string, string> BuildPasswordResetTokens(string code, string resetUrl, int expiryMinutes);
    IReadOnlyDictionary<string, string> BuildNewUserInviteTokens(string recipientName, string inviteUrl, DateTime expiresAtUtc);
    IReadOnlyDictionary<string, string> BuildInviteOtpTokens(string code, DateTime expiresAtUtc);
    IReadOnlyDictionary<string, string> BuildChangePasswordOtpTokens(string code, DateTime expiresAtUtc);
}

public sealed class EmailTokenFactory : IEmailTokenFactory
{
    public IReadOnlyDictionary<string, string> BuildTwoFactorCodeTokens(string code, int expiryMinutes)
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Code"] = code,
            ["ExpiryMinutes"] = Math.Max(1, expiryMinutes).ToString()
        };

    public IReadOnlyDictionary<string, string> BuildPasswordResetTokens(string code, string resetUrl, int expiryMinutes)
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Code"] = code,
            ["ResetUrl"] = resetUrl,
            ["ExpiryMinutes"] = Math.Max(1, expiryMinutes).ToString()
        };

    public IReadOnlyDictionary<string, string> BuildNewUserInviteTokens(string recipientName, string inviteUrl, DateTime expiresAtUtc)
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["RecipientName"] = recipientName,
            ["InviteUrl"] = inviteUrl,
            ["ExpiresAtUtc"] = expiresAtUtc.ToString("u")
        };

    public IReadOnlyDictionary<string, string> BuildInviteOtpTokens(string code, DateTime expiresAtUtc)
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Code"] = code,
            ["ExpiresAtUtc"] = expiresAtUtc.ToString("u")
        };

    public IReadOnlyDictionary<string, string> BuildChangePasswordOtpTokens(string code, DateTime expiresAtUtc)
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Code"] = code,
            ["ExpiresAtUtc"] = expiresAtUtc.ToString("u")
        };
}
