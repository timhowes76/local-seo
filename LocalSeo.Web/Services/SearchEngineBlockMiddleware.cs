namespace LocalSeo.Web.Services;

public sealed class SearchEngineBlockMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context, IAdminSettingsService adminSettingsService)
    {
        var blockSearchEngines = false;
        try
        {
            var settings = await adminSettingsService.GetAsync(context.RequestAborted);
            blockSearchEngines = settings.BlockSearchEngines;
        }
        catch
        {
            blockSearchEngines = false;
        }

        context.Items[SearchEngineBlockContext.HttpContextItemKey] = blockSearchEngines;

        if (blockSearchEngines)
        {
            context.Response.OnStarting(() =>
            {
                var contentType = context.Response.ContentType;
                if (!string.IsNullOrWhiteSpace(contentType)
                    && contentType.StartsWith("text/html", StringComparison.OrdinalIgnoreCase))
                {
                    context.Response.Headers["X-Robots-Tag"] = "noindex, nofollow";
                }

                return Task.CompletedTask;
            });
        }

        await next(context);
    }
}
