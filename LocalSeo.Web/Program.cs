using System.Security.Claims;
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
builder.Services.Configure<GoogleOptions>(builder.Configuration.GetSection("Google"));
builder.Services.Configure<SendGridOptions>(builder.Configuration.GetSection("SendGrid"));
builder.Services.Configure<PlacesOptions>(builder.Configuration.GetSection("Places"));

builder.Services.AddControllersWithViews();
builder.Services.AddHttpClient();
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
    options.AddPolicy("StaffOnly", p => p.RequireAuthenticatedUser().RequireClaim(ClaimTypes.Email));
});

builder.Services.AddScoped<ISqlConnectionFactory, SqlConnectionFactory>();
builder.Services.AddScoped<DbBootstrapper>();
builder.Services.AddScoped<ILoginService, LoginService>();
builder.Services.AddScoped<IGooglePlacesClient, GooglePlacesClient>();
builder.Services.AddScoped<ISearchIngestionService, SearchIngestionService>();
builder.Services.AddScoped<IAdminMaintenanceService, AdminMaintenanceService>();
builder.Services.AddScoped<IEmailSender, SendGridEmailSender>();
builder.Services.AddScoped<ICodeHasher, CodeHasher>();
builder.Services.AddScoped<IReviewsProviderResolver, ReviewsProviderResolver>();
builder.Services.AddScoped<NullReviewsProvider>();
builder.Services.AddScoped<NotImplementedReviewsProvider>();

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

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
