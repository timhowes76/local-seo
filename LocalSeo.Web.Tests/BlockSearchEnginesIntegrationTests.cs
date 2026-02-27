using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using System.Linq;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.RegularExpressions;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace LocalSeo.Web.Tests;

public class BlockSearchEnginesIntegrationTests
{
    private static readonly Regex RobotsMetaRegex = new(
        "<meta\\s+name=\"robots\"\\s+content=\"noindex,\\s*nofollow\"\\s*/?>",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    [Fact]
    public async Task ToggleOn_WritesDisallowRobots_AddsMetaAndHeader()
    {
        await using var factory = new RobotsFeatureWebApplicationFactory(failRobotsWriter: false);
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies = true
        });

        await SaveSecuritySettingsAsync(client, blockSearchEngines: true);

        var robotsPath = Path.Combine(factory.WebRootPath, "robots.txt");
        Assert.True(File.Exists(robotsPath));
        var robotsText = await File.ReadAllTextAsync(robotsPath);
        Assert.Contains("User-agent: *", robotsText, StringComparison.Ordinal);
        Assert.Contains("Disallow: /", robotsText, StringComparison.Ordinal);

        var pageResponse = await client.GetAsync("/login");
        var pageHtml = await pageResponse.Content.ReadAsStringAsync();

        Assert.True(pageResponse.Headers.TryGetValues("X-Robots-Tag", out var robotsHeaders));
        Assert.Contains("noindex, nofollow", robotsHeaders, StringComparer.OrdinalIgnoreCase);
        Assert.Matches(RobotsMetaRegex, pageHtml);

        var robotsResponse = await client.GetAsync("/robots.txt");
        Assert.Equal(HttpStatusCode.OK, robotsResponse.StatusCode);
        Assert.False(robotsResponse.Headers.Contains("X-Robots-Tag"));
    }

    [Fact]
    public async Task ToggleOff_WritesAllowRobots_RemovesMetaAndHeader()
    {
        await using var factory = new RobotsFeatureWebApplicationFactory(failRobotsWriter: false);
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies = true
        });

        await SaveSecuritySettingsAsync(client, blockSearchEngines: true);
        await SaveSecuritySettingsAsync(client, blockSearchEngines: false);

        var robotsPath = Path.Combine(factory.WebRootPath, "robots.txt");
        Assert.True(File.Exists(robotsPath));
        var robotsText = await File.ReadAllTextAsync(robotsPath);
        Assert.Contains("User-agent: *", robotsText, StringComparison.Ordinal);
        Assert.Contains("Disallow:", robotsText, StringComparison.Ordinal);
        Assert.Contains("Allow: /", robotsText, StringComparison.Ordinal);
        Assert.DoesNotContain("Disallow: /", robotsText, StringComparison.Ordinal);

        var pageResponse = await client.GetAsync("/login");
        var pageHtml = await pageResponse.Content.ReadAsStringAsync();

        Assert.False(pageResponse.Headers.Contains("X-Robots-Tag"));
        Assert.DoesNotMatch(RobotsMetaRegex, pageHtml);
    }

    [Fact]
    public async Task ToggleOn_WhenRobotsWriteFails_ShowsErrorAndRollsBackSetting()
    {
        await using var factory = new RobotsFeatureWebApplicationFactory(failRobotsWriter: true);
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies = true
        });

        var securityPageHtml = await SaveSecuritySettingsAsync(client, blockSearchEngines: true);
        Assert.Contains("robots.txt update failed", securityPageHtml, StringComparison.OrdinalIgnoreCase);

        var settings = await factory.SettingsService.GetAsync(CancellationToken.None);
        Assert.False(settings.BlockSearchEngines);
    }

    private static async Task<string> SaveSecuritySettingsAsync(HttpClient client, bool blockSearchEngines)
    {
        var formPageRequest = new HttpRequestMessage(HttpMethod.Get, "/admin/settings/security");
        formPageRequest.Headers.Add(TestAdminAuthHandler.HeaderName, "true");
        var formPageResponse = await client.SendAsync(formPageRequest);
        Assert.Equal(HttpStatusCode.OK, formPageResponse.StatusCode);
        var formPageHtml = await formPageResponse.Content.ReadAsStringAsync();
        var antiForgeryToken = ExtractAntiForgeryToken(formPageHtml);

        var form = new Dictionary<string, string>
        {
            ["__RequestVerificationToken"] = antiForgeryToken,
            ["MinimumPasswordLength"] = "12",
            ["LoginLockoutThreshold"] = "5",
            ["LoginLockoutMinutes"] = "15",
            ["EmailCodeCooldownSeconds"] = "60",
            ["EmailCodeMaxPerHourPerEmail"] = "10",
            ["EmailCodeMaxPerHourPerIp"] = "50",
            ["EmailCodeExpiryMinutes"] = "10",
            ["EmailCodeMaxFailedAttemptsPerCode"] = "5",
            ["InviteExpiryHours"] = "24",
            ["InviteOtpExpiryMinutes"] = "10",
            ["InviteOtpCooldownSeconds"] = "60",
            ["InviteOtpMaxPerHourPerInvite"] = "3",
            ["InviteOtpMaxPerHourPerIp"] = "25",
            ["InviteOtpMaxAttempts"] = "5",
            ["InviteOtpLockMinutes"] = "15",
            ["InviteMaxAttempts"] = "10",
            ["InviteLockMinutes"] = "15",
            ["ChangePasswordOtpExpiryMinutes"] = "10",
            ["ChangePasswordOtpCooldownSeconds"] = "60",
            ["ChangePasswordOtpMaxPerHourPerUser"] = "3",
            ["ChangePasswordOtpMaxPerHourPerIp"] = "25",
            ["ChangePasswordOtpMaxAttempts"] = "5",
            ["ChangePasswordOtpLockMinutes"] = "15",
            ["PasswordRequiresNumber"] = "true",
            ["PasswordRequiresCapitalLetter"] = "true",
            ["PasswordRequiresSpecialCharacter"] = "true"
        };

        if (blockSearchEngines)
            form["BlockSearchEngines"] = "true";

        var saveRequest = new HttpRequestMessage(HttpMethod.Post, "/admin/settings/security")
        {
            Content = new FormUrlEncodedContent(form)
        };
        saveRequest.Headers.Add(TestAdminAuthHandler.HeaderName, "true");
        saveRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");

        var saveResponse = await client.SendAsync(saveRequest);
        Assert.Equal(HttpStatusCode.Redirect, saveResponse.StatusCode);
        Assert.NotNull(saveResponse.Headers.Location);

        var followRequest = new HttpRequestMessage(HttpMethod.Get, saveResponse.Headers.Location);
        followRequest.Headers.Add(TestAdminAuthHandler.HeaderName, "true");
        var followResponse = await client.SendAsync(followRequest);
        Assert.Equal(HttpStatusCode.OK, followResponse.StatusCode);
        return await followResponse.Content.ReadAsStringAsync();
    }

    private static string ExtractAntiForgeryToken(string html)
    {
        var match = Regex.Match(
            html,
            "name=\"__RequestVerificationToken\"[^>]*value=\"([^\"]+)\"",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        Assert.True(match.Success, "Anti-forgery token was not found in the security settings form.");
        return match.Groups[1].Value;
    }

    private sealed class RobotsFeatureWebApplicationFactory(bool failRobotsWriter) : WebApplicationFactory<Program>, IAsyncDisposable
    {
        public string WebRootPath { get; } = Path.Combine(Path.GetTempPath(), "localseo-tests", Guid.NewGuid().ToString("N"));
        public InMemoryAdminSettingsService SettingsService { get; } = new();

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            Directory.CreateDirectory(WebRootPath);
            var appleP8Path = Path.Combine(WebRootPath, "test-apple.p8");
            File.WriteAllText(appleP8Path, "test");

            builder.UseEnvironment("Testing");
            builder.UseWebRoot(WebRootPath);
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Testing:SkipStartupInitialization"] = "true",
                    ["Testing:DisableHostedServices"] = "true",
                    ["Integrations:AzureMaps:PrimaryKey"] = "test-primary",
                    ["Integrations:AzureMaps:SecondaryKey"] = "test-secondary",
                    ["Integrations:AppleMaps:TeamId"] = "test-team",
                    ["Integrations:AppleMaps:KeyId"] = "test-key",
                    ["Integrations:AppleMaps:P8Path"] = appleP8Path,
                    ["ConnectionStrings:Sql"] = "Server=(localdb)\\MSSQLLocalDB;Database=LocalSeoTests;Integrated Security=true;TrustServerCertificate=True"
                });
            });

            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IAdminSettingsService>();
                services.AddSingleton<IAdminSettingsService>(SettingsService);

                services.RemoveAll<IUserRepository>();
                services.AddSingleton<IUserRepository>(new TestUserRepository());

                services.RemoveAll<IAnnouncementService>();
                var announcementService = new Mock<IAnnouncementService>();
                announcementService
                    .Setup(x => x.GetUnreadCountAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(0);
                services.AddSingleton(announcementService.Object);

                var adminOnlyHandlerDescriptor = services.FirstOrDefault(d =>
                    d.ServiceType == typeof(IAuthorizationHandler)
                    && d.ImplementationType == typeof(AdminOnlyAuthorizationHandler));
                if (adminOnlyHandlerDescriptor is not null)
                    services.Remove(adminOnlyHandlerDescriptor);
                services.AddSingleton<IAuthorizationHandler, TestAdminOnlyAuthorizationHandler>();

                services.AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = TestAdminAuthHandler.SchemeName;
                    options.DefaultChallengeScheme = TestAdminAuthHandler.SchemeName;
                    options.DefaultScheme = TestAdminAuthHandler.SchemeName;
                }).AddScheme<AuthenticationSchemeOptions, TestAdminAuthHandler>(TestAdminAuthHandler.SchemeName, _ => { });

                if (failRobotsWriter)
                {
                    services.RemoveAll<IRobotsTxtWriter>();
                    services.AddSingleton<IRobotsTxtWriter, AlwaysFailRobotsTxtWriter>();
                }
            });
        }

        public new async ValueTask DisposeAsync()
        {
            base.Dispose();
            await Task.CompletedTask;
            TryDeleteDirectory(WebRootPath);
        }

        private static void TryDeleteDirectory(string directory)
        {
            try
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
            catch
            {
                // Keep cleanup best-effort only.
            }
        }
    }

    private sealed class InMemoryAdminSettingsService : IAdminSettingsService
    {
        private readonly object sync = new();
        private AdminSettingsModel current = new();

        public Task<AdminSettingsModel> GetAsync(CancellationToken ct)
        {
            lock (sync)
            {
                return Task.FromResult(Clone(current));
            }
        }

        public Task SaveAsync(AdminSettingsModel model, CancellationToken ct)
        {
            lock (sync)
            {
                current = Clone(model);
                return Task.CompletedTask;
            }
        }

        private static AdminSettingsModel Clone(AdminSettingsModel source)
        {
            var clone = new AdminSettingsModel();
            var properties = typeof(AdminSettingsModel).GetProperties();
            foreach (var property in properties)
            {
                if (property.CanRead && property.CanWrite)
                    property.SetValue(clone, property.GetValue(source));
            }

            return clone;
        }
    }

    private sealed class AlwaysFailRobotsTxtWriter : IRobotsTxtWriter
    {
        public Task<RobotsTxtWriteResult> WriteAsync(bool blockSearchEngines, CancellationToken ct)
            => Task.FromResult(new RobotsTxtWriteResult(false, "Simulated write failure."));
    }

    private sealed class TestAdminOnlyAuthorizationHandler : AuthorizationHandler<AdminOnlyRequirement>
    {
        protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, AdminOnlyRequirement requirement)
        {
            if (context.User.Identity?.IsAuthenticated == true)
                context.Succeed(requirement);

            return Task.CompletedTask;
        }
    }

    private sealed class TestAdminAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public const string SchemeName = "TestAdmin";
        public const string HeaderName = "X-Test-Admin";

        public TestAdminAuthHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue(HeaderName, out var values)
                || !string.Equals(values.ToString(), "true", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            var claims = new List<Claim>
            {
                new(ClaimTypes.NameIdentifier, "1"),
                new(ClaimTypes.Name, "Test Admin"),
                new(ClaimTypes.Email, "admin@test.local"),
                new(AuthClaimTypes.IsAdmin, "true")
            };

            var identity = new ClaimsIdentity(claims, SchemeName);
            var principal = new ClaimsPrincipal(identity);
            var ticket = new AuthenticationTicket(principal, SchemeName);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }

    private sealed class TestUserRepository : IUserRepository
    {
        private static readonly UserRecord AdminUser = new(
            Id: 1,
            FirstName: "Test",
            LastName: "Admin",
            EmailAddress: "admin@test.local",
            EmailAddressNormalized: "admin@test.local",
            PasswordHash: null,
            PasswordHashVersion: 1,
            IsActive: true,
            IsAdmin: true,
            DateCreatedAtUtc: DateTime.UtcNow,
            DatePasswordLastSetUtc: null,
            LastLoginAtUtc: null,
            FailedPasswordAttempts: 0,
            LockedoutUntilUtc: null,
            InviteStatus: UserLifecycleStatus.Active,
            SessionVersion: 0,
            UseGravatar: false,
            IsDarkMode: false);

        public Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct)
            => Task.FromResult(
                string.Equals(emailNormalized, AdminUser.EmailAddressNormalized, StringComparison.OrdinalIgnoreCase)
                    ? AdminUser
                    : null);

        public Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct)
            => Task.FromResult(id == AdminUser.Id ? AdminUser : null);

        public Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, string? searchTerm, CancellationToken ct)
            => Task.FromResult<IReadOnlyList<AdminUserListRow>>([]);

        public Task<bool> UpdateProfileAsync(int userId, string firstName, string lastName, bool useGravatar, bool isDarkMode, CancellationToken ct)
            => Task.FromResult(false);

        public Task<bool> UpdateUserAsync(int userId, string firstName, string lastName, string emailAddress, string emailAddressNormalized, bool isAdmin, UserLifecycleStatus inviteStatus, CancellationToken ct)
            => Task.FromResult(false);

        public Task<bool> DeleteUserAsync(int userId, CancellationToken ct)
            => Task.FromResult(false);

        public Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct)
            => Task.CompletedTask;

        public Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task<bool> UpdatePasswordAndBumpSessionVersionAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
            => Task.FromResult(false);
    }
}
