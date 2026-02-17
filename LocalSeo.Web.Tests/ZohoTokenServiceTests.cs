using System.Net;
using System.Net.Http;
using System.Text.Json;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace LocalSeo.Web.Tests;

public class ZohoTokenServiceTests
{
    [Fact]
    public async Task GetValidAccessTokenAsync_ReturnsCachedToken_WhenNotNearExpiry()
    {
        var tokenStore = new Mock<IZohoTokenStore>();
        tokenStore
            .Setup(x => x.LoadAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ZohoTokenSnapshot(
                RefreshToken: "refresh-token",
                AccessToken: "cached-access",
                AccessTokenExpiresAtUtc: DateTime.UtcNow.AddMinutes(10)));

        var httpClientFactory = new Mock<IHttpClientFactory>();
        var service = CreateService(httpClientFactory.Object, tokenStore.Object);

        var token = await service.GetValidAccessTokenAsync(CancellationToken.None);

        Assert.Equal("cached-access", token);
        httpClientFactory.Verify(x => x.CreateClient(It.IsAny<string>()), Times.Never);
        tokenStore.Verify(x => x.SaveAsync(It.IsAny<ZohoTokenSnapshot>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetValidAccessTokenAsync_RefreshesAndPersistsToken_WhenExpiredWithinSafetySkew()
    {
        var now = DateTime.UtcNow;
        var expiredSnapshot = new ZohoTokenSnapshot(
            RefreshToken: "refresh-token",
            AccessToken: "old-access",
            AccessTokenExpiresAtUtc: now.AddSeconds(30));

        var tokenStore = new Mock<IZohoTokenStore>();
        tokenStore
            .SetupSequence(x => x.LoadAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(expiredSnapshot)
            .ReturnsAsync(expiredSnapshot);

        ZohoTokenSnapshot? savedSnapshot = null;
        tokenStore
            .Setup(x => x.SaveAsync(It.IsAny<ZohoTokenSnapshot>(), It.IsAny<CancellationToken>()))
            .Callback<ZohoTokenSnapshot, CancellationToken>((snapshot, _) => savedSnapshot = snapshot)
            .Returns(Task.CompletedTask);

        string? refreshRequestQuery = null;
        var handler = new StubHttpMessageHandler((request, _) =>
        {
            refreshRequestQuery = request.RequestUri?.Query;
            var payload = JsonSerializer.Serialize(new { access_token = "refreshed-access", expires_in = 3600 });
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(payload)
            });
        });
        var httpClient = new HttpClient(handler);

        var httpClientFactory = new Mock<IHttpClientFactory>();
        httpClientFactory
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(httpClient);

        var service = CreateService(httpClientFactory.Object, tokenStore.Object);
        var token = await service.GetValidAccessTokenAsync(CancellationToken.None);

        Assert.Equal("refreshed-access", token);
        Assert.NotNull(refreshRequestQuery);
        Assert.Contains("grant_type=refresh_token", refreshRequestQuery);
        Assert.Contains("refresh_token=refresh-token", refreshRequestQuery);

        Assert.NotNull(savedSnapshot);
        Assert.Equal("refresh-token", savedSnapshot!.RefreshToken);
        Assert.Equal("refreshed-access", savedSnapshot.AccessToken);
        Assert.True(savedSnapshot.AccessTokenExpiresAtUtc > now.AddMinutes(30));
    }

    private static ZohoTokenService CreateService(IHttpClientFactory httpClientFactory, IZohoTokenStore tokenStore)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new ZohoOAuthOptions
        {
            AccountsBaseUrl = "https://accounts.zoho.eu",
            ClientId = "client-id",
            ClientSecret = "client-secret",
            RedirectUri = "https://localhost/zoho/oauth/callback",
            Scopes = "ZohoCRM.modules.leads.ALL,ZohoCRM.settings.modules.READ"
        });

        return new ZohoTokenService(
            httpClientFactory,
            options,
            tokenStore,
            NullLogger<ZohoTokenService>.Instance);
    }

    private sealed class StubHttpMessageHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> callback) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => callback(request, cancellationToken);
    }
}
