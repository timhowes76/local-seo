using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.AspNetCore.Html;
using Microsoft.Extensions.Options;
using RazorLight;
using TransactionalEmails = LocalSeo.Web.Services.TransactionalEmails;

namespace LocalSeo.Web.Services;

public interface IEmailWrapperComposer
{
    Task<string> ComposeAsync(string subject, string bodyHtml, string? fromName, CancellationToken ct);
}

public sealed class RazorEmailWrapperComposer : IEmailWrapperComposer
{
    private readonly string basePathFull;
    private readonly string basePathPrefix;
    private readonly RazorLightEngine razorEngine;
    private readonly TransactionalEmails.ITransactionalEmailRepository repository;
    private readonly ILogger<RazorEmailWrapperComposer> logger;

    public RazorEmailWrapperComposer(
        IWebHostEnvironment environment,
        IOptions<EmailTemplatePathOptions> pathOptions,
        TransactionalEmails.ITransactionalEmailRepository repository,
        ILogger<RazorEmailWrapperComposer> logger)
    {
        this.repository = repository;
        this.logger = logger;

        var configuredBasePath = (pathOptions.Value.BasePath ?? string.Empty).Trim();
        if (configuredBasePath.Length == 0)
            configuredBasePath = "wwwroot/assets/email-template";

        var absoluteBasePath = Path.IsPathRooted(configuredBasePath)
            ? configuredBasePath
            : Path.Combine(environment.ContentRootPath, configuredBasePath);

        basePathFull = Path.GetFullPath(absoluteBasePath);
        if (!Directory.Exists(basePathFull))
            Directory.CreateDirectory(basePathFull);
        basePathPrefix = basePathFull.EndsWith(Path.DirectorySeparatorChar)
            ? basePathFull
            : $"{basePathFull}{Path.DirectorySeparatorChar}";

        razorEngine = new RazorLightEngineBuilder()
            .UseFileSystemProject(basePathFull)
            .UseMemoryCachingProvider()
            .Build();
    }

    public async Task<string> ComposeAsync(string subject, string bodyHtml, string? fromName, CancellationToken ct)
    {
        var fallbackSettings = new EmailSettingsRecord(
            EmailSettingsId: 0,
            FromEmail: "noreply@example.local",
            FromName: "Local SEO",
            GlobalSignatureHtml: string.Empty,
            WrapperViewPath: "_EmailWrapper.cshtml",
            UpdatedUtc: DateTime.UtcNow);

        EmailSettingsRecord settings;
        try
        {
            settings = await repository.GetEmailSettingsAsync(ct) ?? fallbackSettings;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load email settings for wrapper rendering. Falling back to built-in wrapper.");
            return BuildFallbackWrapper(subject, bodyHtml, string.Empty, fromName, fallbackSettings.FromName);
        }

        try
        {
            var wrapperPath = ResolveSafePath(settings.WrapperViewPath);
            var fileInfo = new FileInfo(wrapperPath.FullPath);
            if (!fileInfo.Exists)
            {
                logger.LogError("Email wrapper file not found at {WrapperPath}. Falling back to built-in wrapper.", wrapperPath.FullPath);
                return BuildFallbackWrapper(subject, bodyHtml, settings.GlobalSignatureHtml, fromName, settings.FromName);
            }

            var source = await File.ReadAllTextAsync(wrapperPath.FullPath, ct);
            var cacheKey = $"{wrapperPath.RelativePath}|{fileInfo.LastWriteTimeUtc.Ticks}";
            var model = new EmailWrapperRenderModel
            {
                Subject = subject ?? string.Empty,
                BodyHtml = new HtmlString(bodyHtml ?? string.Empty),
                SignatureHtml = new HtmlString(settings.GlobalSignatureHtml ?? string.Empty),
                BrandName = settings.FromName,
                FromName = string.IsNullOrWhiteSpace(fromName) ? settings.FromName : fromName
            };

            return await razorEngine.CompileRenderStringAsync(cacheKey, source, model);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Email wrapper rendering failed. WrapperPath={WrapperPath}. Falling back to built-in wrapper.", settings.WrapperViewPath);
            return BuildFallbackWrapper(subject, bodyHtml, settings.GlobalSignatureHtml, fromName, settings.FromName);
        }
    }

    private (string RelativePath, string FullPath) ResolveSafePath(string relativePath)
    {
        var normalized = (relativePath ?? string.Empty).Trim().Replace('\\', '/');
        normalized = normalized.TrimStart('/');
        if (normalized.Length == 0)
            throw new InvalidOperationException("Email wrapper path is missing.");
        if (Path.IsPathRooted(normalized))
            throw new InvalidOperationException($"Absolute email wrapper path is not allowed: {relativePath}");
        if (!normalized.EndsWith(".cshtml", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Email wrapper path must point to a .cshtml file: {relativePath}");

        var combined = Path.Combine(basePathFull, normalized.Replace('/', Path.DirectorySeparatorChar));
        var fullPath = Path.GetFullPath(combined);
        if (!fullPath.StartsWith(basePathPrefix, StringComparison.OrdinalIgnoreCase))
        {
            logger.LogWarning("Rejected email wrapper path outside base directory. RequestedPath={Path}", relativePath);
            throw new InvalidOperationException("Wrapper path is outside allowed email template directory.");
        }

        var relativeForCache = fullPath[basePathPrefix.Length..].Replace(Path.DirectorySeparatorChar, '/');
        return (relativeForCache, fullPath);
    }

    private static string BuildFallbackWrapper(string? subject, string? bodyHtml, string? signatureHtml, string? fromName, string? brandName)
    {
        var encodedTitle = System.Net.WebUtility.HtmlEncode(subject ?? string.Empty);
        var effectiveBrand = string.IsNullOrWhiteSpace(brandName) ? "Local SEO" : brandName;
        var encodedBrand = System.Net.WebUtility.HtmlEncode(effectiveBrand);
        var encodedFrom = System.Net.WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(fromName) ? effectiveBrand : fromName);
        var safeBody = bodyHtml ?? string.Empty;
        var safeSignature = signatureHtml ?? string.Empty;

        return $"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{encodedTitle}</title>
</head>
<body style="margin:0;padding:0;background:#eef3f8;color:#10233d;font-family:Segoe UI,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:28px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="640" cellpadding="0" cellspacing="0" border="0" style="width:640px;max-width:640px;background:#ffffff;border:1px solid #d7e2ef;border-radius:14px;">
          <tr>
            <td style="padding:24px;background:#1d4c8f;color:#ffffff;font-size:20px;font-weight:700;">{encodedBrand}</td>
          </tr>
          <tr>
            <td style="padding:24px;font-size:15px;line-height:1.6;color:#1f2f46;">{safeBody}</td>
          </tr>
          <tr>
            <td style="padding:0 24px 20px 24px;">
              <div style="border-top:1px solid #e3ebf5;padding-top:14px;color:#4b5f7a;font-size:13px;line-height:1.6;">{safeSignature}</div>
            </td>
          </tr>
          <tr>
            <td style="padding:12px 24px;background:#f6f9fc;border-top:1px solid #e3ebf5;color:#64748b;font-size:12px;">Sent by {encodedFrom}</td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
""";
    }
}
