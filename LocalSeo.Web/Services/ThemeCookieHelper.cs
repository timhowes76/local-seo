using Microsoft.AspNetCore.Http;

namespace LocalSeo.Web.Services;

public static class ThemeCookieHelper
{
    private const string ThemeCookieName = "theme";
    private const string DarkValue = "dark=1";

    public static void ApplyThemeCookie(HttpResponse response, bool isDark)
    {
        if (isDark)
        {
            response.Cookies.Append(
                ThemeCookieName,
                DarkValue,
                new CookieOptions
                {
                    Expires = DateTimeOffset.UtcNow.AddDays(365),
                    Path = "/",
                    Secure = true,
                    HttpOnly = true,
                    SameSite = SameSiteMode.Lax
                });
            return;
        }

        response.Cookies.Delete(
            ThemeCookieName,
            new CookieOptions
            {
                Path = "/",
                Secure = true,
                HttpOnly = true,
                SameSite = SameSiteMode.Lax
            });
    }
}
