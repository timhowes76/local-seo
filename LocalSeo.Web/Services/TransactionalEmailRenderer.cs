using System.Reflection;
using System.Text.RegularExpressions;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.AspNetCore.Html;
using Microsoft.Extensions.Options;
using RazorLight;

namespace LocalSeo.Web.Services.TransactionalEmails;

public interface IEmailTemplateRenderer
{
    Task<RenderedEmail> RenderAsync(string templateKey, object model, CancellationToken ct);
}

public sealed class RazorDiskEmailTemplateRenderer : IEmailTemplateRenderer
{
    private static readonly Regex SubjectTokenRegex = new(@"\{\{\s*(?<name>[A-Za-z0-9_]+)\s*\}\}", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly ITransactionalEmailRepository repository;
    private readonly ILogger<RazorDiskEmailTemplateRenderer> logger;
    private readonly RazorLightEngine razorEngine;
    private readonly string basePathFull;
    private readonly string basePathPrefix;

    public RazorDiskEmailTemplateRenderer(
        ITransactionalEmailRepository repository,
        IWebHostEnvironment environment,
        IOptions<EmailTemplatePathOptions> pathOptions,
        ILogger<RazorDiskEmailTemplateRenderer> logger)
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

    public async Task<RenderedEmail> RenderAsync(string templateKey, object model, CancellationToken ct)
    {
        var template = await repository.GetEnabledTemplateByKeyAsync(templateKey, ct)
            ?? throw new InvalidOperationException($"Email template '{templateKey}' is missing or disabled.");

        var settings = await repository.GetEmailSettingsAsync(ct)
            ?? throw new InvalidOperationException("Email settings row is missing.");

        var bodyPath = ResolveSafePath(template.ViewPath);
        var wrapperPath = ResolveSafePath(settings.WrapperViewPath);

        var bodyHtml = await RenderViewFileAsync(bodyPath.RelativePath, bodyPath.FullPath, model, ct);
        var subject = RenderSubject(template.SubjectTemplate, model);

        var wrapperModel = new EmailWrapperRenderModel
        {
            Subject = subject,
            BodyHtml = new HtmlString(bodyHtml),
            SignatureHtml = new HtmlString(settings.GlobalSignatureHtml ?? string.Empty),
            BrandName = settings.FromName,
            FromName = settings.FromName
        };

        var wrappedHtml = await RenderViewFileAsync(wrapperPath.RelativePath, wrapperPath.FullPath, wrapperModel, ct);
        return new RenderedEmail(subject, wrappedHtml);
    }

    private async Task<string> RenderViewFileAsync(string relativePath, string fullPath, object model, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var file = new FileInfo(fullPath);
        if (!file.Exists)
            throw new FileNotFoundException($"Email template file not found: {relativePath}", fullPath);

        var source = await File.ReadAllTextAsync(fullPath, ct);
        var cacheKey = $"{relativePath}|{file.LastWriteTimeUtc.Ticks}";
        return await razorEngine.CompileRenderStringAsync(cacheKey, source, model);
    }

    private (string RelativePath, string FullPath) ResolveSafePath(string relativePath)
    {
        var normalized = (relativePath ?? string.Empty).Trim().Replace('\\', '/');
        normalized = normalized.TrimStart('/');
        if (normalized.Length == 0)
            throw new InvalidOperationException("Email view path is missing.");
        if (Path.IsPathRooted(normalized))
            throw new InvalidOperationException($"Absolute email view path is not allowed: {relativePath}");
        if (!normalized.EndsWith(".cshtml", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Email view path must point to a .cshtml file: {relativePath}");

        var combined = Path.Combine(basePathFull, normalized.Replace('/', Path.DirectorySeparatorChar));
        var fullPath = Path.GetFullPath(combined);
        if (!fullPath.StartsWith(basePathPrefix, StringComparison.OrdinalIgnoreCase))
        {
            logger.LogWarning("Rejected email template path outside base directory. RequestedPath={Path}", relativePath);
            throw new InvalidOperationException("Template path is outside allowed email template directory.");
        }

        var relativeForCache = fullPath[basePathPrefix.Length..].Replace(Path.DirectorySeparatorChar, '/');
        return (relativeForCache, fullPath);
    }

    private static string RenderSubject(string subjectTemplate, object model)
    {
        var template = (subjectTemplate ?? string.Empty).Trim();
        if (template.Length == 0 || model is null)
            return template;

        var valueMap = BuildValueMap(model);
        return SubjectTokenRegex.Replace(template, match =>
        {
            var name = match.Groups["name"].Value;
            return valueMap.TryGetValue(name, out var value) ? value : match.Value;
        });
    }

    private static Dictionary<string, string> BuildValueMap(object model)
    {
        if (model is IReadOnlyDictionary<string, string> roDict)
            return new Dictionary<string, string>(roDict, StringComparer.OrdinalIgnoreCase);
        if (model is IDictionary<string, string> dict)
            return new Dictionary<string, string>(dict, StringComparer.OrdinalIgnoreCase);

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var props = model.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
        foreach (var prop in props)
        {
            if (!prop.CanRead)
                continue;
            var value = prop.GetValue(model);
            if (value is null)
                continue;
            map[prop.Name] = value.ToString() ?? string.Empty;
        }
        return map;
    }
}
