using System.Net.Http.Headers;
using System.Security.Claims;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using LocalSeo.Web.Services.ApiStatusChecks;
using TransactionalEmails = LocalSeo.Web.Services.TransactionalEmails;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
var contentRootPath = builder.Environment.ContentRootPath;
builder.Configuration.AddUserSecrets<Program>(optional: true, reloadOnChange: true);

string ResolveContentRootPath(string? configuredPath)
{
    var raw = (configuredPath ?? string.Empty).Trim();
    if (raw.Length == 0)
        return string.Empty;

    var normalized = raw.Replace("{ContentRoot}", contentRootPath, StringComparison.OrdinalIgnoreCase);
    if (Path.IsPathRooted(normalized))
        return Path.GetFullPath(normalized);

    return Path.GetFullPath(Path.Combine(contentRootPath, normalized));
}

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
builder.Services.Configure<ChangePasswordOptions>(builder.Configuration.GetSection("ChangePassword"));
builder.Services.Configure<EmailTemplatePathOptions>(builder.Configuration.GetSection("EmailTemplate"));
builder.Services.Configure<BrandSettings>(builder.Configuration.GetSection("Brand"));
builder.Services.Configure<ReportsOptions>(builder.Configuration.GetSection("Reports"));
builder.Services.Configure<OpenAiOptions>(builder.Configuration.GetSection("OpenAi"));
builder.Services.AddOptions<AzureMapsOptions>()
    .Bind(builder.Configuration.GetSection("Integrations:AzureMaps"))
    .Validate(options => !string.IsNullOrWhiteSpace(options.PrimaryKey), "Integrations:AzureMaps:PrimaryKey is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.SecondaryKey), "Integrations:AzureMaps:SecondaryKey is required.")
    .ValidateOnStart();
builder.Services.AddOptions<AppleMapsOptions>()
    .Bind(builder.Configuration.GetSection("Integrations:AppleMaps"))
    .PostConfigure(options =>
    {
        options.TeamId = (options.TeamId ?? string.Empty).Trim();
        options.KeyId = (options.KeyId ?? string.Empty).Trim();
        options.P8Path = ResolveContentRootPath(options.P8Path);
    })
    .Validate(options => !string.IsNullOrWhiteSpace(options.TeamId), "Integrations:AppleMaps:TeamId is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.KeyId), "Integrations:AppleMaps:KeyId is required.")
    .Validate(options =>
    {
        if (string.IsNullOrWhiteSpace(options.P8Path) || !File.Exists(options.P8Path))
            return false;

        try
        {
            using var stream = File.Open(options.P8Path, FileMode.Open, FileAccess.Read, FileShare.Read);
            return stream.CanRead;
        }
        catch
        {
            return false;
        }
    }, "Integrations:AppleMaps:P8Path must point to an existing readable .p8 file.")
    .ValidateOnStart();

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

                var sessionVersionValue = context.Principal.FindFirst(AuthClaimTypes.SessionVersion)?.Value;
                if (!int.TryParse(sessionVersionValue, out var sessionVersionClaim) || sessionVersionClaim < 0)
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

                    if (user.SessionVersion != sessionVersionClaim)
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
    options.AddPolicy("AdminOnly", p =>
    {
        p.RequireAuthenticatedUser();
        p.AddRequirements(new AdminOnlyRequirement());
    });
});

builder.Services.AddScoped<ISqlConnectionFactory, SqlConnectionFactory>();
builder.Services.AddScoped<Microsoft.AspNetCore.Authorization.IAuthorizationHandler, AdminOnlyAuthorizationHandler>();
builder.Services.AddScoped<DbBootstrapper>();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddScoped<IEmailAddressNormalizer, EmailAddressNormalizer>();
builder.Services.AddScoped<IAvatarResolver, AvatarResolver>();
builder.Services.AddScoped<IUserRepository, UserRepository>();
builder.Services.AddScoped<IUserLoginLogRepository, UserLoginLogRepository>();
builder.Services.AddScoped<IUserInviteRepository, UserInviteRepository>();
builder.Services.AddScoped<IUserOtpRepository, UserOtpRepository>();
builder.Services.AddScoped<IEmailCodeRepository, EmailCodeRepository>();
builder.Services.AddScoped<ICryptoService, CryptoService>();
builder.Services.AddScoped<IPasswordHasherService, PasswordHasherService>();
builder.Services.AddScoped<IRateLimiterService, RateLimiterService>();
builder.Services.AddScoped<IEmailCodeService, EmailCodeService>();
builder.Services.AddScoped<IEmailTemplateRepository, EmailTemplateRepository>();
builder.Services.AddScoped<IEmailTemplateService, EmailTemplateService>();
builder.Services.AddScoped<IEmailTemplateRenderer, EmailTemplateRenderer>();
builder.Services.AddScoped<IEmailSignatureSettingsService, EmailSignatureSettingsService>();
builder.Services.AddScoped<IEmailWrapperComposer, RazorEmailWrapperComposer>();
builder.Services.AddScoped<IEmailRedactionService, EmailRedactionService>();
builder.Services.AddScoped<IEmailTokenFactory, EmailTokenFactory>();
builder.Services.AddScoped<IEmailLogRepository, EmailLogRepository>();
builder.Services.AddScoped<IEmailProviderEventRepository>(sp => (IEmailProviderEventRepository)sp.GetRequiredService<IEmailLogRepository>());
builder.Services.AddScoped<IEmailLogQueryService, EmailLogQueryService>();
builder.Services.AddScoped<IEmailDeliveryStatusSyncService, SendGridEmailDeliveryStatusSyncService>();
builder.Services.AddScoped<TransactionalEmails.ITransactionalEmailRepository, TransactionalEmails.TransactionalEmailRepository>();
builder.Services.AddScoped<TransactionalEmails.IEmailTemplateRenderer, TransactionalEmails.RazorDiskEmailTemplateRenderer>();
builder.Services.AddScoped<TransactionalEmails.IEmailSender, TransactionalEmails.SendGridTransactionalEmailSender>();
builder.Services.AddScoped<IEmailSenderService, EmailSenderService>();
builder.Services.AddScoped<ISendGridWebhookSignatureValidator, SendGridWebhookSignatureValidator>();
builder.Services.AddScoped<ISendGridWebhookIngestionService, SendGridWebhookIngestionService>();
builder.Services.AddScoped<IAuthService, AuthService>();
builder.Services.AddScoped<IInviteService, InviteService>();
builder.Services.AddScoped<IPasswordChangeService, PasswordChangeService>();
builder.Services.AddScoped<IGooglePlacesClient, GooglePlacesClient>();
builder.Services.AddScoped<ISearchIngestionService, SearchIngestionService>();
builder.Services.AddSingleton<ISearchRunExecutor, SearchRunExecutor>();
builder.Services.AddScoped<IAdminSettingsService, AdminSettingsService>();
builder.Services.AddScoped<ISecuritySettingsProvider, SecuritySettingsProvider>();
builder.Services.AddScoped<IGbLocationDataListService, GbLocationDataListService>();
builder.Services.AddScoped<ICategoryLocationKeywordService, CategoryLocationKeywordService>();
builder.Services.AddScoped<IKeyphraseSuggestionService, KeyphraseSuggestionService>();
builder.Services.AddSingleton<IKeyphraseBulkAddJobService, KeyphraseBulkAddJobService>();
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
builder.Services.AddScoped<IReportsService, ReportsService>();
builder.Services.AddSingleton<IUserAgentInfoParser, UserAgentInfoParser>();
builder.Services.AddScoped<IAppErrorRepository, AppErrorRepository>();
builder.Services.AddScoped<IAppErrorLogger, AppErrorLogger>();
builder.Services.AddSingleton<IAnnouncementHtmlSanitizer, AnnouncementHtmlSanitizer>();
builder.Services.AddScoped<IAnnouncementRepository, AnnouncementRepository>();
builder.Services.AddScoped<IAnnouncementService, AnnouncementService>();
builder.Services.AddScoped<IApiStatusRepository, ApiStatusRepository>();
builder.Services.AddScoped<IApiStatusCheckRunner, ApiStatusCheckRunner>();
builder.Services.AddScoped<IApiStatusService, ApiStatusService>();
builder.Services.AddScoped<IExternalApiHealthRepository, ExternalApiHealthRepository>();
builder.Services.AddScoped<IExternalApiHealthService, ExternalApiHealthService>();
builder.Services.AddSingleton<IApiStatusLatestCache, ApiStatusLatestCache>();
builder.Services.AddSingleton<IApiStatusRefreshRateLimiter, ApiStatusRefreshRateLimiter>();
builder.Services.AddScoped<IApiStatusCheck, OpenAiConfiguredApiStatusCheck>();
builder.Services.AddScoped<IApiStatusCheck, CompaniesHousePingApiStatusCheck>();
builder.Services.AddScoped<IApiStatusCheck, GooglePlacesConfiguredApiStatusCheck>();
builder.Services.AddScoped<IApiStatusCheck, GoogleOAuthApiStatusCheck>();
builder.Services.AddScoped<IApiStatusCheck, SendGridProfileApiStatusCheck>();
builder.Services.AddScoped<IApiStatusCheck, ZohoCrmPingApiStatusCheck>();
builder.Services.AddSingleton<IExternalApiStatusChecker, AppleMapsStatusChecker>();
builder.Services.AddSingleton<IExternalApiStatusChecker, AzureMapsStatusChecker>();
builder.Services.AddHostedService<ApiStatusMonitorHostedService>();
builder.Services.AddHostedService<ExternalApiHealthMonitorHostedService>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var bootstrapper = scope.ServiceProvider.GetRequiredService<DbBootstrapper>();
    await bootstrapper.EnsureSchemaAsync(CancellationToken.None);
    var apiStatusService = scope.ServiceProvider.GetRequiredService<IApiStatusService>();
    await apiStatusService.SeedDefinitionsAsync(CancellationToken.None);
    await apiStatusService.WarmLatestCacheAsync(CancellationToken.None);
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

app.UseMiddleware<AppErrorLoggingMiddleware>();
app.UseHttpsRedirection();
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/Private", StringComparison.OrdinalIgnoreCase))
    {
        context.Response.StatusCode = StatusCodes.Status404NotFound;
        return;
    }

    await next();
});
app.UseStaticFiles();

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
