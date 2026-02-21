using System.Security.Claims;
using LocalSeo.Web.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Filters;

namespace LocalSeo.Web.Services;

public sealed class AdminOnlyRequirement : IAuthorizationRequirement;

public sealed class AdminOnlyAuthorizationHandler(
    IUserRepository userRepository) : AuthorizationHandler<AdminOnlyRequirement>
{
    protected override async Task HandleRequirementAsync(AuthorizationHandlerContext context, AdminOnlyRequirement requirement)
    {
        if (context.User?.Identity?.IsAuthenticated != true)
            return;

        if (TryResolveHttpContext(context.Resource, out var httpContext)
            && httpContext.Items.TryGetValue("CurrentUserRecord", out var value)
            && value is UserRecord cachedUser)
        {
            if (cachedUser.IsAdmin)
                context.Succeed(requirement);
            return;
        }

        var idRaw = context.User.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!int.TryParse(idRaw, out var userId) || userId <= 0)
            return;

        try
        {
            var user = await userRepository.GetByIdAsync(userId, CancellationToken.None);
            if (user is not null && user.IsAdmin)
                context.Succeed(requirement);
        }
        catch
        {
            // Keep authorization failure behavior if user lookup fails.
        }
    }

    private static bool TryResolveHttpContext(object? resource, out HttpContext httpContext)
    {
        if (resource is HttpContext context)
        {
            httpContext = context;
            return true;
        }

        if (resource is AuthorizationFilterContext filterContext)
        {
            httpContext = filterContext.HttpContext;
            return true;
        }

        httpContext = null!;
        return false;
    }
}
