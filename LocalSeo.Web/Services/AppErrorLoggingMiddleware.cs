namespace LocalSeo.Web.Services;

public sealed class AppErrorLoggingMiddleware(RequestDelegate next)
{
    private const string AlreadyLoggedItemKey = "__app_error_already_logged";

    public async Task InvokeAsync(HttpContext httpContext, IAppErrorLogger appErrorLogger)
    {
        try
        {
            await next(httpContext);

            if (httpContext.Response.StatusCode == 500 && !WasAlreadyLogged(httpContext))
            {
                await appErrorLogger.LogHttpStatus500WithoutExceptionAsync(
                    httpContext,
                    "Returned StatusCode(500) without exception.",
                    CancellationToken.None);
                MarkLogged(httpContext);
            }
        }
        catch (Exception ex)
        {
            if (!WasAlreadyLogged(httpContext))
            {
                await appErrorLogger.LogHttpExceptionAsync(httpContext, ex, CancellationToken.None);
                MarkLogged(httpContext);
            }

            throw;
        }
    }

    private static bool WasAlreadyLogged(HttpContext httpContext)
    {
        return httpContext.Items.TryGetValue(AlreadyLoggedItemKey, out var value)
               && value is bool wasLogged
               && wasLogged;
    }

    private static void MarkLogged(HttpContext httpContext)
    {
        httpContext.Items[AlreadyLoggedItemKey] = true;
    }
}
