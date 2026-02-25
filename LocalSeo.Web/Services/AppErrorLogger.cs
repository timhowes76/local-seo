using System.Security.Claims;
using System.Text;
using LocalSeo.Web.Models;
using UAParser;

namespace LocalSeo.Web.Services;

public interface IUserAgentInfoParser
{
    ParsedUserAgentInfo Parse(string? userAgentRaw);
}

public sealed record ParsedUserAgentInfo(
    string? BrowserName,
    string? BrowserVersion,
    string? OsName,
    string? OsVersion,
    string DeviceType);

public sealed class UserAgentInfoParser : IUserAgentInfoParser
{
    private static readonly string[] BotHints =
    [
        "bot",
        "spider",
        "crawler",
        "crawl",
        "slurp",
        "bingpreview",
        "facebookexternalhit",
        "duckduckbot",
        "baiduspider",
        "yandexbot",
        "ahrefsbot",
        "semrushbot",
        "uptime"
    ];

    private readonly Parser parser = Parser.GetDefault();

    public ParsedUserAgentInfo Parse(string? userAgentRaw)
    {
        var raw = Normalize(userAgentRaw, 512);
        if (string.IsNullOrWhiteSpace(raw))
            return new ParsedUserAgentInfo(null, null, null, null, "Unknown");

        ClientInfo clientInfo;
        try
        {
            clientInfo = parser.Parse(raw);
        }
        catch
        {
            return new ParsedUserAgentInfo(null, null, null, null, InferDeviceType(raw, null, null));
        }

        var browserName = NormalizeFamily(clientInfo.UA?.Family);
        var browserVersion = NormalizeVersion(clientInfo.UA?.Major);
        var osName = NormalizeFamily(clientInfo.OS?.Family);
        var osVersion = NormalizeVersion(clientInfo.OS?.Major);
        var deviceFamily = NormalizeFamily(clientInfo.Device?.Family);
        var deviceType = InferDeviceType(raw, browserName, deviceFamily);

        return new ParsedUserAgentInfo(
            browserName,
            browserVersion,
            osName,
            osVersion,
            deviceType);
    }

    private static string InferDeviceType(string rawUserAgent, string? browserName, string? deviceFamily)
    {
        if (ContainsBotHint(rawUserAgent) || ContainsBotHint(browserName) || ContainsBotHint(deviceFamily))
            return "Bot";

        var device = (deviceFamily ?? string.Empty).Trim();
        if (ContainsAny(device, "tablet", "ipad", "kindle", "tab"))
            return "Tablet";
        if (ContainsAny(device, "mobile", "phone", "iphone", "android"))
            return "Mobile";
        if (ContainsAny(device, "desktop", "windows", "mac", "linux", "pc"))
            return "Desktop";

        if (ContainsAny(rawUserAgent, "tablet", "ipad", "kindle", "tab"))
            return "Tablet";
        if (ContainsAny(rawUserAgent, "mobile", "iphone", "android"))
            return "Mobile";
        if (ContainsAny(rawUserAgent, "windows nt", "macintosh", "x11", "linux"))
            return "Desktop";

        return "Unknown";
    }

    private static bool ContainsBotHint(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        return BotHints.Any(hint => value.Contains(hint, StringComparison.OrdinalIgnoreCase));
    }

    private static bool ContainsAny(string? value, params string[] terms)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        return terms.Any(term => value.Contains(term, StringComparison.OrdinalIgnoreCase));
    }

    private static string? NormalizeFamily(string? value)
    {
        var normalized = Normalize(value, 64);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;
        if (string.Equals(normalized, "Other", StringComparison.OrdinalIgnoreCase))
            return null;
        return normalized;
    }

    private static string? NormalizeVersion(string? value)
    {
        var normalized = Normalize(value, 32);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;
        return normalized;
    }

    private static string? Normalize(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}

public interface IAppErrorLogger
{
    Task LogHttpExceptionAsync(HttpContext httpContext, Exception exception, CancellationToken ct = default);
    Task LogHttpStatus500WithoutExceptionAsync(HttpContext httpContext, string detailMessage, CancellationToken ct = default);
    Task LogExceptionAsync(Exception ex, BackgroundContext ctx, CancellationToken ct = default);
}

public sealed class AppErrorLogger(
    IAppErrorRepository appErrorRepository,
    IUserAgentInfoParser userAgentInfoParser,
    ILogger<AppErrorLogger> logger) : IAppErrorLogger
{
    public async Task LogHttpExceptionAsync(HttpContext httpContext, Exception exception, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(exception);

        var request = httpContext.Request;
        var rawUserAgent = request.Headers.UserAgent.ToString();
        var parsedUserAgent = userAgentInfoParser.Parse(rawUserAgent);

        var row = new AppErrorRow
        {
            CreatedUtc = DateTime.UtcNow,
            UserId = GetCurrentUserId(httpContext.User),
            IpAddress = httpContext.Connection.RemoteIpAddress?.ToString(),
            TraceId = httpContext.TraceIdentifier,
            HttpMethod = request.Method,
            StatusCode = 500,
            FullUrl = BuildFullUrl(request),
            Referrer = request.Headers.Referer.ToString(),
            UserAgentRaw = rawUserAgent,
            BrowserName = parsedUserAgent.BrowserName,
            BrowserVersion = parsedUserAgent.BrowserVersion,
            OsName = parsedUserAgent.OsName,
            OsVersion = parsedUserAgent.OsVersion,
            DeviceType = parsedUserAgent.DeviceType,
            ExceptionType = exception.GetType().FullName,
            ExceptionMessage = exception.Message,
            ExceptionDetail = exception.ToString()
        };

        await WriteSafeAsync(row, ct);
    }

    public async Task LogHttpStatus500WithoutExceptionAsync(HttpContext httpContext, string detailMessage, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var request = httpContext.Request;
        var rawUserAgent = request.Headers.UserAgent.ToString();
        var parsedUserAgent = userAgentInfoParser.Parse(rawUserAgent);
        var detail = string.IsNullOrWhiteSpace(detailMessage)
            ? "Returned StatusCode(500) without exception."
            : detailMessage.Trim();

        var row = new AppErrorRow
        {
            CreatedUtc = DateTime.UtcNow,
            UserId = GetCurrentUserId(httpContext.User),
            IpAddress = httpContext.Connection.RemoteIpAddress?.ToString(),
            TraceId = httpContext.TraceIdentifier,
            HttpMethod = request.Method,
            StatusCode = 500,
            FullUrl = BuildFullUrl(request),
            Referrer = request.Headers.Referer.ToString(),
            UserAgentRaw = rawUserAgent,
            BrowserName = parsedUserAgent.BrowserName,
            BrowserVersion = parsedUserAgent.BrowserVersion,
            OsName = parsedUserAgent.OsName,
            OsVersion = parsedUserAgent.OsVersion,
            DeviceType = parsedUserAgent.DeviceType,
            ExceptionType = "HTTP 500 (no exception)",
            ExceptionMessage = detail,
            ExceptionDetail = detail
        };

        await WriteSafeAsync(row, ct);
    }

    public async Task LogExceptionAsync(Exception ex, BackgroundContext ctx, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(ex);
        ArgumentNullException.ThrowIfNull(ctx);

        var jobName = string.IsNullOrWhiteSpace(ctx.JobName)
            ? "BackgroundTask"
            : ctx.JobName.Trim();

        var detailBuilder = new StringBuilder();
        detailBuilder.AppendLine($"Background job: {jobName}");
        if (!string.IsNullOrWhiteSpace(ctx.TraceId))
            detailBuilder.AppendLine($"TraceId: {ctx.TraceId.Trim()}");
        if (!string.IsNullOrWhiteSpace(ctx.ExtraText))
            detailBuilder.AppendLine($"Context: {ctx.ExtraText.Trim()}");
        detailBuilder.AppendLine();
        detailBuilder.Append(ex.ToString());

        var row = new AppErrorRow
        {
            CreatedUtc = DateTime.UtcNow,
            UserId = ctx.UserId,
            IpAddress = null,
            TraceId = ctx.TraceId,
            HttpMethod = null,
            StatusCode = 500,
            FullUrl = null,
            Referrer = null,
            UserAgentRaw = null,
            BrowserName = null,
            BrowserVersion = null,
            OsName = null,
            OsVersion = null,
            DeviceType = "Unknown",
            ExceptionType = ex.GetType().FullName,
            ExceptionMessage = $"[{jobName}] {ex.Message}",
            ExceptionDetail = detailBuilder.ToString()
        };

        await WriteSafeAsync(row, ct);
    }

    private async Task WriteSafeAsync(AppErrorRow row, CancellationToken ct)
    {
        try
        {
            await appErrorRepository.InsertAsync(row, ct);
        }
        catch (Exception loggingException)
        {
            logger.LogError(loggingException, "Failed to write AppError log row.");
        }
    }

    private static string? BuildFullUrl(HttpRequest request)
    {
        if (request is null)
            return null;

        var pathBase = request.PathBase.HasValue ? request.PathBase.Value : string.Empty;
        var path = request.Path.HasValue ? request.Path.Value : string.Empty;
        var queryString = request.QueryString.HasValue ? request.QueryString.Value : string.Empty;
        return $"{request.Scheme}://{request.Host}{pathBase}{path}{queryString}";
    }

    private static int? GetCurrentUserId(ClaimsPrincipal user)
    {
        var idValue = user.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(idValue, out var userId) && userId > 0
            ? userId
            : null;
    }
}
