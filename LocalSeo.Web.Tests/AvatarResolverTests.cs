using LocalSeo.Web.Models;
using LocalSeo.Web.Services;

namespace LocalSeo.Web.Tests;

public class AvatarResolverTests
{
    [Fact]
    public void ComputeGravatarHash_UsesTrimmedLowerUtf8Md5()
    {
        var resolver = new AvatarResolver();

        var hash = resolver.ComputeGravatarHash("  MyEmailAddress@example.com ");

        Assert.Equal("0bc83cb571cd1c50ba6f3e8a78ef1346", hash);
    }

    [Fact]
    public void Resolve_UsesGravatarWhenEnabled()
    {
        var resolver = new AvatarResolver();
        var user = new UserRecord(
            Id: 7,
            FirstName: "Tim",
            LastName: "Howes",
            EmailAddress: "tim.howes@kontrolit.net",
            EmailAddressNormalized: "tim.howes@kontrolit.net",
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
            UseGravatar: true);

        var avatar = resolver.Resolve(user, 64);

        Assert.True(avatar.UsesGravatar);
        Assert.NotNull(avatar.ImageUrl);
        Assert.Contains("/avatar/", avatar.ImageUrl!, StringComparison.Ordinal);
        Assert.Equal("TH", avatar.Initials);
    }

    [Fact]
    public void Resolve_FallsBackToInitialsWhenDisabled()
    {
        var resolver = new AvatarResolver();
        var user = new UserRecord(
            Id: 8,
            FirstName: "Jane",
            LastName: "Smith",
            EmailAddress: "jane@example.test",
            EmailAddressNormalized: "jane@example.test",
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
            UseGravatar: false);

        var avatar = resolver.Resolve(user, 64);

        Assert.False(avatar.UsesGravatar);
        Assert.Null(avatar.ImageUrl);
        Assert.Equal("JS", avatar.Initials);
    }
}
