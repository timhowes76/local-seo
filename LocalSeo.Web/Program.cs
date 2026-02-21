using System.Net.Http.Headers;
using System.Security.Claims;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, lc) => lc
    .ReadFrom.Configuration(ctx.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console());

builder.Services.Configure<AuthOptions>(builder.Configuration.GetSection("Auth"));
builder.Services.Configure<EmailCodesOptions>(builder.Configuration.GetSection("EmailCodes"));
builder.Services.Configure<GoogleOptions>(builder.Configuration.GetSection("Google"));
builder.Services.Configure<SendGridOptions>(builder.Configuration.GetSection("SendGrid"));
builder.Services.Configure<PlacesOptions>(builder.Configuration.GetSection("Places"));
builder.Services.Configure<DataForSeoOptions>(builder.Configuration.GetSection("DataForSeo"));
builder.Services.Configure<ZohoOAuthOptions>(builder.Configuration.GetSection("ZohoOAuth"));
builder.Services.Configure<CompaniesHouseOptions>(builder.Configuration.GetSection("CompaniesHouse"));
builder.Services.Configure<InviteOptions>(builder.Configuration.GetSection("Invites"));

builder.Services.AddControllersWithViews();
builder.Services.AddHttpClient();
builder.Services.AddMemoryCache();
builder.Services.AddDataProtection();

builder.Services.AddAuthentication("LocalCookie")
    .AddCookie("LocalCookie", options =>
    {
        options.LoginPath = "/login";
        options.Cookie.Name = "localseo.auth";
        options.Cookie.HttpOnly = true;
        options.Cookie.SecurePolicy = CookieSecurePolicy.Always;
        options.Cookie.SameSite = SameSiteMode.Lax;
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
        options.Events = new CookieAuthenticationEvents
        {
            OnValidatePrincipal = async context =>
            {
                if (context.Principal?.Identity?.IsAuthenticated != true)
                    return;

                var idValue = context.Principal.FindFirst(ClaimTypes.NameIdentifier)?.Value;
                if (!int.TryParse(idValue, out var userId) || userId <= 0)
                {
                    context.RejectPrincipal();
                    await context.HttpContext.SignOutAsync("LocalCookie");
                    return;
                }

                try
                {
                    var userRepository = context.HttpContext.RequestServices.GetRequiredService<IUserRepository>();
                    var user = await userRepository.GetByIdAsync(userId, context.HttpContext.RequestAborted);
                    if (user is null || !user.IsActive || user.InviteStatus != UserLifecycleStatus.Active)
                    {
                        context.RejectPrincipal();
                        await context.HttpContext.SignOutAsync("LocalCookie");
                        return;
                    }

                    context.HttpContext.Items["CurrentUserRecord"] = user;
                }
                catch
                {
                    // Keep current principal if DB validation is temporarily unavailable.
                }
            }
        };
    });

builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("StaffOnly", p => p.RequireAuthenticatedUser());
    options.AddPolicy("AdminOnly", p => p.RequireAuthenticatedUser().RequireClaim(AuthClaimTypes.IsAdmin, "true"));
});

builder.Services.AddScoped<ISqlConnectionFactory, SqlConnectionFactory>();
builder.Services.AddScoped<DbBootstrapper>();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddScoped<IEmailAddressNormalizer, EmailAddressNormalizer>();
builder.Services.AddScoped<IAvatarResolver, AvatarResolver>();
builder.Services.AddScoped<IUserRepository, UserRepository>();
builder.Services.AddScoped<IUserLoginLogRepository, UserLoginLogRepository>();
builder.Services.AddScoped<IUserInviteRepository, UserInviteRepository>();
builder.Services.AddScoped<IEmailCodeRepository, EmailCodeRepository>();
builder.Services.AddScoped<ICryptoService, CryptoService>();
builder.Services.AddScoped<IPasswordHasherService, PasswordHasherService>();
builder.Services.AddScoped<IRateLimiterService, RateLimiterService>();
builder.Services.AddScoped<IEmailCodeService, EmailCodeService>();
builder.Services.AddScoped<IAuthService, AuthService>();
builder.Services.AddScoped<IInviteService, InviteService>();
builder.Services.AddScoped<IGooglePlacesClient, GooglePlacesClient>();
builder.Services.AddScoped<ISearchIngestionService, SearchIngestionService>();
builder.Services.AddScoped<IAdminSettingsService, AdminSettingsService>();
builder.Services.AddScoped<IGbLocationDataListService, GbLocationDataListService>();
builder.Services.AddScoped<ICategoryLocationKeywordService, CategoryLocationKeywordService>();
builder.Services.AddScoped<IGoogleBusinessProfileRefreshTokenStore, LocalSecureGoogleRefreshTokenStore>();
builder.Services.AddScoped<IGoogleBusinessProfileOAuthService, GoogleBusinessProfileOAuthService>();
builder.Services.AddScoped<IZohoTokenStore, SqlZohoTokenStore>();
builder.Services.AddScoped<IZohoOAuthService, ZohoOAuthService>();
builder.Services.AddScoped<IZohoTokenService, ZohoTokenService>();
builder.Services.AddScoped<IZohoLeadSyncService, ZohoLeadSyncService>();
builder.Services.AddHttpClient<IZohoCrmClient, ZohoCrmClient>((sp, client) =>
{
    var zohoOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ZohoOAuthOptions>>().Value;
    var crmBaseUrl = (zohoOptions.CrmApiBaseUrl ?? string.Empty).Trim().TrimEnd('/');
    if (Uri.TryCreate($"{crmBaseUrl}/", UriKind.Absolute, out var baseAddress))
        client.BaseAddress = baseAddress;

    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    client.Timeout = TimeSpan.FromSeconds(30);
});
builder.Services.AddHttpClient<ICompaniesHouseService, CompaniesHouseService>((sp, client) =>
{
    var companiesHouseOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<CompaniesHouseOptions>>().Value;
    var baseUrl = (companiesHouseOptions.BaseUrl ?? string.Empty).Trim().TrimEnd('/');
    if (Uri.TryCreate($"{baseUrl}/", UriKind.Absolute, out var baseAddress))
        client.BaseAddress = baseAddress;

    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    client.Timeout = TimeSpan.FromSeconds(30);
});
builder.Services.AddHttpClient<ICompaniesHouseAccountsSyncService, CompaniesHouseAccountsSyncService>((sp, client) =>
{
    client.Timeout = TimeSpan.FromSeconds(60);
});
builder.Services.AddScoped<IGoogleBusinessProfileCategoryService, GoogleBusinessProfileCategoryService>();
builder.Services.AddScoped<IDataForSeoAccountStatusService, DataForSeoAccountStatusService>();
builder.Services.AddScoped<ISendGridEmailService, SendGridEmailService>();
builder.Services.AddScoped<ICodeHasher, CodeHasher>();
builder.Services.AddScoped<IReviewsProviderResolver, ReviewsProviderResolver>();
builder.Services.AddScoped<NullReviewsProvider>();
builder.Services.AddScoped<NotImplementedReviewsProvider>();
builder.Services.AddScoped<DataForSeoReviewsProvider>();
builder.Services.AddScoped<IDataForSeoTaskTracker, DataForSeoReviewsProvider>();
builder.Services.AddScoped<IReviewVelocityService, ReviewVelocityService>();
builder.Services.AddScoped<ICompetitorVelocityAdapter, NullCompetitorVelocityAdapter>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var bootstrapper = scope.ServiceProvider.GetRequiredService<DbBootstrapper>();
    await bootstrapper.EnsureSchemaAsync(CancellationToken.None);
}

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseWhen(
    context =>
        string.Equals(context.Request.Host.Host, "localhost", StringComparison.OrdinalIgnoreCase)
        && context.Request.Host.Port == 5000,
    branch => branch.UseDeveloperExceptionPage());

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
