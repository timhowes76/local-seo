using System.Text;
using Microsoft.AspNetCore.Hosting;

namespace LocalSeo.Web.Services;

public sealed record RobotsTxtWriteResult(bool Success, string? ErrorMessage = null, Exception? Exception = null);

public interface IRobotsTxtWriter
{
    Task<RobotsTxtWriteResult> WriteAsync(bool blockSearchEngines, CancellationToken ct);
}

public sealed class RobotsTxtWriter(IWebHostEnvironment webHostEnvironment) : IRobotsTxtWriter
{
    private static readonly Encoding Utf8WithoutBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

    public async Task<RobotsTxtWriteResult> WriteAsync(bool blockSearchEngines, CancellationToken ct)
    {
        string? tempPath = null;
        try
        {
            var webRootPath = (webHostEnvironment.WebRootPath ?? string.Empty).Trim();
            if (webRootPath.Length == 0)
                throw new InvalidOperationException("WebRootPath is not configured.");

            Directory.CreateDirectory(webRootPath);

            var robotsPath = Path.Combine(webRootPath, "robots.txt");
            tempPath = Path.Combine(webRootPath, $"robots.{Guid.NewGuid():N}.tmp");
            var content = BuildContent(blockSearchEngines);

            await File.WriteAllTextAsync(tempPath, content, Utf8WithoutBom, ct);
            File.Move(tempPath, robotsPath, overwrite: true);

            return new RobotsTxtWriteResult(true);
        }
        catch (Exception ex)
        {
            if (!string.IsNullOrWhiteSpace(tempPath))
            {
                try
                {
                    if (File.Exists(tempPath))
                        File.Delete(tempPath);
                }
                catch
                {
                    // Ignore cleanup errors and return the original write failure.
                }
            }

            return new RobotsTxtWriteResult(false, ex.Message, ex);
        }
    }

    private static string BuildContent(bool blockSearchEngines)
    {
        if (blockSearchEngines)
            return "User-agent: *\nDisallow: /\n";

        return "User-agent: *\nDisallow:\nAllow: /\n";
    }
}
