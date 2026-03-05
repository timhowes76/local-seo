using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LocalSeo.Web.Tests;

public sealed class LoginTrustThisDeviceIntegrationTests
{
    [Fact]
    public async Task TwoFactor_WithTrustThisDeviceTrue_IssuesPersistentAuthCookie()
    {
        await using var factory = new LoginTrustDeviceWebApplicationFactory();
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies = true
        });

        var token = await GetAntiForgeryTokenAsync(client);
        var response = await PostTwoFactorAsync(client, token, trustThisDevice: true);

        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var authCookie = GetAuthCookieHeader(response);
        Assert.Contains("expires=", authCookie, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TwoFactor_WithTrustThisDeviceFalse_IssuesSessionAuthCookie()
    {
        await using var factory = new LoginTrustDeviceWebApplicationFactory();
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies = true
        });

        var token = await GetAntiForgeryTokenAsync(client);
        var response = await PostTwoFactorAsync(client, token, trustThisDevice: false);

        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var authCookie = GetAuthCookieHeader(response);
        Assert.DoesNotContain("expires=", authCookie, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("max-age=", authCookie, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<string> GetAntiForgeryTokenAsync(HttpClient client)
    {
        var response = await client.GetAsync("/twofactor?rid=1&email=user%40example.test");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var match = Regex.Match(
            html,
            "name=\"__RequestVerificationToken\"[^>]*value=\"([^\"]+)\"",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        Assert.True(match.Success, "Anti-forgery token not found on /twofactor page.");
        return match.Groups[1].Value;
    }

    private static async Task<HttpResponseMessage> PostTwoFactorAsync(HttpClient client, string antiForgeryToken, bool trustThisDevice)
    {
        var form = new Dictionary<string, string>
        {
            ["__RequestVerificationToken"] = antiForgeryToken,
            ["Rid"] = "1",
            ["Email"] = "user@example.test",
            ["Code"] = "123456",
            ["TrustThisDevice"] = trustThisDevice ? "true" : "false"
        };

        var request = new HttpRequestMessage(HttpMethod.Post, "/twofactor")
        {
            Content = new FormUrlEncodedContent(form)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");
        return await client.SendAsync(request);
    }

    private static string GetAuthCookieHeader(HttpResponseMessage response)
    {
        Assert.True(response.Headers.TryGetValues("Set-Cookie", out var setCookies), "Expected Set-Cookie header.");
        var authCookie = setCookies.FirstOrDefault(x => x.StartsWith("localseo.auth=", StringComparison.OrdinalIgnoreCase));
        Assert.False(string.IsNullOrWhiteSpace(authCookie), "Expected localseo.auth cookie.");
        return authCookie!;
    }

    private sealed class LoginTrustDeviceWebApplicationFactory : WebApplicationFactory<Program>, IAsyncDisposable
    {
        private readonly string webRootPath = Path.Combine(Path.GetTempPath(), "localseo-login-tests", Guid.NewGuid().ToString("N"));

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            Directory.CreateDirectory(webRootPath);
            var appleP8Path = Path.Combine(webRootPath, "test-apple.p8");
            File.WriteAllText(appleP8Path, "test");

            builder.UseEnvironment("Testing");
            builder.UseWebRoot(webRootPath);
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
                services.RemoveAll<IAuthService>();
                services.AddSingleton<IAuthService>(new TestAuthService());
            });
        }

        public new async ValueTask DisposeAsync()
        {
            base.Dispose();
            await Task.CompletedTask;
            try
            {
                if (Directory.Exists(webRootPath))
                    Directory.Delete(webRootPath, recursive: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    private sealed class TestAuthService : IAuthService
    {
        private static readonly UserRecord User = new(
            Id: 1,
            FirstName: "Test",
            LastName: "User",
            EmailAddress: "user@example.test",
            EmailAddressNormalized: "user@example.test",
            PasswordHash: null,
            PasswordHashVersion: 1,
            IsActive: true,
            IsAdmin: false,
            DateCreatedAtUtc: DateTime.UtcNow,
            DatePasswordLastSetUtc: null,
            LastLoginAtUtc: null,
            FailedPasswordAttempts: 0,
            LockedoutUntilUtc: null,
            InviteStatus: UserLifecycleStatus.Active,
            SessionVersion: 0,
            UseGravatar: false,
            IsDarkMode: false);

        public Task<BeginLoginResult> BeginLoginAsync(string emailAddress, string password, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct)
            => Task.FromResult(new BeginLoginResult(true, "ok", 1, User.EmailAddress));

        public Task<CompleteTwoFactorResult> CompleteTwoFactorLoginAsync(int rid, string emailAddress, string code, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct)
            => Task.FromResult(new CompleteTwoFactorResult(true, "ok", User));

        public Task<string> RequestForgotPasswordAsync(string emailAddress, string appBaseUrl, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct)
            => Task.FromResult("ok");

        public Task<ResetPasswordResult> ResetPasswordAsync(int rid, string emailAddress, string code, string newPassword, string confirmPassword, CancellationToken ct)
            => Task.FromResult(new ResetPasswordResult(true, "ok", User));
    }
}
