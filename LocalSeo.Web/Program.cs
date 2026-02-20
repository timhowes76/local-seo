using System.Net.Http.Headers;
using LocalSeo.Web.Data;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
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
builder.Services.AddScoped<IUserRepository, UserRepository>();
builder.Services.AddScoped<IEmailCodeRepository, EmailCodeRepository>();
builder.Services.AddScoped<IPasswordHasherService, PasswordHasherService>();
builder.Services.AddScoped<IRateLimiterService, RateLimiterService>();
builder.Services.AddScoped<IEmailCodeService, EmailCodeService>();
builder.Services.AddScoped<IAuthService, AuthService>();
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
builder.Services.AddScoped<IGoogleBusinessProfileCategoryService, GoogleBusinessProfileCategoryService>();
builder.Services.AddScoped<IDataForSeoAccountStatusService, DataForSeoAccountStatusService>();
builder.Services.AddScoped<IEmailSender, SendGridEmailSender>();
builder.Services.AddScoped<IDataForSeoSocialProfilesService, DataForSeoSocialProfilesService>();
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
