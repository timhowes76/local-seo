using System.IO.Compression;
using System.Text;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[AllowAnonymous]
public class DataForSeoCallbackController(IDataForSeoTaskTracker taskTracker, ILogger<DataForSeoCallbackController> logger) : Controller
{
    [IgnoreAntiforgeryToken]
    [HttpPost("/api/dataforseo/postback")]
    public async Task<IActionResult> Postback([FromQuery] string? id, [FromQuery] string? tag, CancellationToken ct)
    {
        string payload;
        await using (var input = new MemoryStream())
        {
            await Request.Body.CopyToAsync(input, ct);
            input.Position = 0;

            var isGzip = Request.Headers.ContentEncoding.Any(x =>
                !string.IsNullOrWhiteSpace(x) && x.Contains("gzip", StringComparison.OrdinalIgnoreCase));
            if (isGzip)
            {
                await using var gzip = new GZipStream(input, CompressionMode.Decompress);
                using var reader = new StreamReader(gzip, Encoding.UTF8);
                payload = await reader.ReadToEndAsync(ct);
            }
            else
            {
                using var reader = new StreamReader(input, Encoding.UTF8);
                payload = await reader.ReadToEndAsync(ct);
            }
        }

        var result = await taskTracker.HandlePostbackAsync(id, tag, payload, ct);
        if (!result.Success)
        {
            logger.LogWarning("DataForSEO postback processing failed. Id={Id}, Tag={Tag}, Message={Message}", id, tag, result.Message);
            return BadRequest(new { ok = false, result.Message });
        }

        return Ok(new { ok = true, result.Message });
    }
}
