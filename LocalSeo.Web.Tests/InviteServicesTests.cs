using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Linq;

namespace LocalSeo.Web.Tests;

public class InviteServicesTests
{
    [Fact]
    public void CryptoService_Base64UrlRoundTrip_AndDeterministicHmac()
    {
        var crypto = new CryptoService(Microsoft.Extensions.Options.Options.Create(new InviteOptions
        {
            HmacSecret = "12345678901234567890123456789012"
        }));

        var payload = crypto.GenerateRandomBytes(32);
        var token = crypto.Base64UrlEncode(payload);
        var decodedOk = crypto.TryBase64UrlDecode(token, out var decoded);

        Assert.True(decodedOk);
        Assert.Equal(payload, decoded);

        var h1 = crypto.ComputeHmacSha256(payload);
        var h2 = crypto.ComputeHmacSha256(payload);
        Assert.Equal(32, h1.Length);
        Assert.True(crypto.FixedTimeEquals(h1, h2));
    }

    [Fact]
    public async Task VerifyOtp_LocksAfterMaxAttempts()
    {
        var now = new DateTimeOffset(2026, 2, 21, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new TestTimeProvider(now);
        var userRepo = new InMemoryUserRepository(
            new UserRecord(
                Id: 42,
                FirstName: "Alex",
                LastName: "Jones",
                EmailAddress: "alex@example.test",
                EmailAddressNormalized: "alex@example.test",
                PasswordHash: null,
                PasswordHashVersion: 1,
                IsActive: false,
                IsAdmin: false,
                DateCreatedAtUtc: now.UtcDateTime,
                DatePasswordLastSetUtc: null,
                LastLoginAtUtc: null,
                FailedPasswordAttempts: 0,
                LockedoutUntilUtc: null,
                InviteStatus: UserLifecycleStatus.Pending,
                SessionVersion: 0,
                UseGravatar: false));

        var crypto = new CryptoService(Microsoft.Extensions.Options.Options.Create(new InviteOptions
        {
            HmacSecret = "abcdefghijklmnopqrstuvwxyz123456"
        }));

        var rawToken = crypto.GenerateRandomBytes(32);
        var token = crypto.Base64UrlEncode(rawToken);
        var tokenHash = crypto.ComputeHmacSha256(rawToken);

        var inviteRepo = new InMemoryUserInviteRepository();
        inviteRepo.AddInvite(new UserInviteRecord(
            UserInviteId: 1001,
            UserId: 42,
            FirstName: "Alex",
            LastName: "Jones",
            EmailAddress: "alex@example.test",
            EmailNormalized: "alex@example.test",
            TokenHash: tokenHash,
            ExpiresAtUtc: now.UtcDateTime.AddHours(24),
            UsedAtUtc: null,
            CreatedAtUtc: now.UtcDateTime,
            CreatedByUserId: 7,
            ResentAtUtc: null,
            Status: UserInviteStatus.Active,
            AttemptCount: 0,
            LastAttemptAtUtc: null,
            LockedUntilUtc: null,
            OtpVerifiedAtUtc: null,
            LastOtpSentAtUtc: null));

        var email = new CaptureSendGridEmailService();
        var service = new InviteService(
            userRepo,
            inviteRepo,
            new EmailAddressNormalizer(),
            new PasswordHasherService(),
            crypto,
            email,
            new FixedSecuritySettingsProvider(BuildSecuritySettings()),
            timeProvider,
            NullLogger<InviteService>.Instance);

        var send = await service.SendOtpAsync(token, "127.0.0.1", CancellationToken.None);
        Assert.True(send.Success);
        Assert.True(send.OtpSent);
        Assert.NotNull(email.LastInviteOtpCode);

        for (var i = 0; i < 5; i++)
        {
            var wrong = await service.VerifyOtpAsync(token, "000000", "127.0.0.1", CancellationToken.None);
            Assert.False(wrong.Success);
        }

        var locked = await service.VerifyOtpAsync(token, email.LastInviteOtpCode, "127.0.0.1", CancellationToken.None);
        Assert.False(locked.Success);
        Assert.Contains("Too many attempts", locked.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class CaptureSendGridEmailService : ISendGridEmailService
    {
        public string? LastInviteOtpCode { get; private set; }

        public Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct) => Task.CompletedTask;

        public Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct) => Task.CompletedTask;

        public Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct) => Task.CompletedTask;

        public Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
        {
            LastInviteOtpCode = code;
            return Task.CompletedTask;
        }

        public Task SendChangePasswordOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
            => Task.CompletedTask;
    }

    private sealed class InMemoryUserRepository(UserRecord user) : IUserRepository
    {
        private UserRecord current = user;

        public Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct)
            => Task.FromResult(string.Equals(current.EmailAddressNormalized, emailNormalized, StringComparison.Ordinal) ? current : null);

        public Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct)
            => Task.FromResult(current.Id == id ? current : null);

        public Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, string? searchTerm, CancellationToken ct)
            => Task.FromResult<IReadOnlyList<AdminUserListRow>>([]);

        public Task<bool> UpdateProfileAsync(int userId, string firstName, string lastName, bool useGravatar, CancellationToken ct)
        {
            if (current.Id != userId)
                return Task.FromResult(false);

            current = current with
            {
                FirstName = firstName,
                LastName = lastName,
                UseGravatar = useGravatar
            };
            return Task.FromResult(true);
        }

        public Task<bool> UpdateUserAsync(int userId, string firstName, string lastName, string emailAddress, string emailAddressNormalized, bool isAdmin, UserLifecycleStatus inviteStatus, CancellationToken ct)
        {
            if (current.Id != userId)
                return Task.FromResult(false);

            current = current with
            {
                FirstName = firstName,
                LastName = lastName,
                EmailAddress = emailAddress,
                EmailAddressNormalized = emailAddressNormalized,
                IsAdmin = isAdmin,
                InviteStatus = inviteStatus,
                IsActive = inviteStatus == UserLifecycleStatus.Active
            };
            return Task.FromResult(true);
        }

        public Task<bool> DeleteUserAsync(int userId, CancellationToken ct)
            => Task.FromResult(current.Id == userId);

        public Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct)
            => Task.CompletedTask;

        public Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id == userId)
            {
                current = current with
                {
                    PasswordHash = passwordHash,
                    PasswordHashVersion = passwordHashVersion,
                    DatePasswordLastSetUtc = nowUtc
                };
            }
            return Task.CompletedTask;
        }

        public Task<bool> UpdatePasswordAndBumpSessionVersionAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id != userId)
                return Task.FromResult(false);

            current = current with
            {
                PasswordHash = passwordHash,
                PasswordHashVersion = passwordHashVersion,
                DatePasswordLastSetUtc = nowUtc,
                SessionVersion = current.SessionVersion + 1
            };
            return Task.FromResult(true);
        }
    }

    private sealed class InMemoryUserInviteRepository : IUserInviteRepository
    {
        private readonly List<UserInviteRecord> invites = [];
        private readonly List<InviteOtpRecord> otps = [];
        private long nextOtpId = 1;
        private int nextUserId = 100;
        private long nextInviteId = 1;

        public void AddInvite(UserInviteRecord invite)
        {
            invites.Add(invite);
            nextInviteId = Math.Max(nextInviteId, invite.UserInviteId + 1);
        }

        public Task<int> CreatePendingUserAsync(string firstName, string lastName, string emailAddress, string emailAddressNormalized, DateTime nowUtc, CancellationToken ct)
            => Task.FromResult(nextUserId++);

        public Task<long> CreateInviteAsync(UserInviteCreateRequest request, CancellationToken ct)
        {
            var id = nextInviteId++;
            invites.Add(new UserInviteRecord(
                id,
                request.UserId,
                "First",
                "Last",
                request.EmailNormalized,
                request.EmailNormalized,
                request.TokenHash,
                request.ExpiresAtUtc,
                null,
                request.CreatedAtUtc,
                request.CreatedByUserId,
                null,
                UserInviteStatus.Active,
                0,
                null,
                null,
                null,
                null));
            return Task.FromResult(id);
        }

        public Task<int> RevokeActiveInvitesForUserAsync(int userId, DateTime nowUtc, CancellationToken ct)
        {
            var count = 0;
            for (var i = 0; i < invites.Count; i++)
            {
                var invite = invites[i];
                if (invite.UserId == userId && invite.Status == UserInviteStatus.Active && !invite.UsedAtUtc.HasValue)
                {
                    invites[i] = invite with { Status = UserInviteStatus.Revoked, ResentAtUtc = nowUtc };
                    count++;
                }
            }
            return Task.FromResult(count);
        }

        public Task<UserInviteRecord?> GetInviteByTokenHashAsync(byte[] tokenHash, CancellationToken ct)
            => Task.FromResult(invites.SingleOrDefault(x => x.TokenHash.SequenceEqual(tokenHash)));

        public Task<UserInviteRecord?> GetLatestInviteByUserIdAsync(int userId, CancellationToken ct)
            => Task.FromResult(invites.Where(x => x.UserId == userId).OrderByDescending(x => x.CreatedAtUtc).FirstOrDefault());

        public Task MarkInviteExpiredAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
        {
            var idx = invites.FindIndex(x => x.UserInviteId == userInviteId);
            if (idx >= 0)
                invites[idx] = invites[idx] with { Status = UserInviteStatus.Expired, LastAttemptAtUtc = nowUtc };
            return Task.CompletedTask;
        }

        public Task MarkInviteAttemptFailureAsync(long userInviteId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
        {
            var idx = invites.FindIndex(x => x.UserInviteId == userInviteId);
            if (idx >= 0)
            {
                var invite = invites[idx];
                var attempts = invite.AttemptCount + 1;
                var lockUntil = attempts >= maxAttempts ? nowUtc.AddMinutes(lockMinutes) : invite.LockedUntilUtc;
                invites[idx] = invite with { AttemptCount = attempts, LastAttemptAtUtc = nowUtc, LockedUntilUtc = lockUntil };
            }
            return Task.CompletedTask;
        }

        public Task MarkInviteOtpVerifiedAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
        {
            var idx = invites.FindIndex(x => x.UserInviteId == userInviteId);
            if (idx >= 0)
                invites[idx] = invites[idx] with { OtpVerifiedAtUtc = nowUtc };
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLatestOtpSentAtUtcAsync(long userInviteId, CancellationToken ct)
            => Task.FromResult(otps.Where(x => x.UserInviteId == userInviteId).Select(x => (DateTime?)x.SentAtUtc).OrderByDescending(x => x).FirstOrDefault());

        public Task<int> CountOtpSentSinceAsync(long userInviteId, DateTime sinceUtc, CancellationToken ct)
            => Task.FromResult(otps.Count(x => x.UserInviteId == userInviteId && x.SentAtUtc >= sinceUtc));

        public Task<int> CountOtpSentSinceForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct)
            => Task.FromResult(0);

        public Task<long> CreateInviteOtpAsync(InviteOtpCreateRequest request, CancellationToken ct)
        {
            var id = nextOtpId++;
            otps.Add(new InviteOtpRecord(
                id,
                request.UserInviteId,
                request.CodeHash,
                request.ExpiresAtUtc,
                request.SentAtUtc,
                0,
                null,
                null));
            var inviteIdx = invites.FindIndex(x => x.UserInviteId == request.UserInviteId);
            if (inviteIdx >= 0)
                invites[inviteIdx] = invites[inviteIdx] with { LastOtpSentAtUtc = request.SentAtUtc };
            return Task.FromResult(id);
        }

        public Task<InviteOtpRecord?> GetLatestInviteOtpAsync(long userInviteId, DateTime nowUtc, CancellationToken ct)
        {
            var row = otps
                .Where(x => x.UserInviteId == userInviteId && x.ExpiresAtUtc >= nowUtc && !x.UsedAtUtc.HasValue)
                .OrderByDescending(x => x.SentAtUtc)
                .ThenByDescending(x => x.InviteOtpId)
                .FirstOrDefault();
            return Task.FromResult(row);
        }

        public Task MarkInviteOtpAttemptFailureAsync(long inviteOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
        {
            var idx = otps.FindIndex(x => x.InviteOtpId == inviteOtpId);
            if (idx >= 0)
            {
                var otp = otps[idx];
                var attempts = otp.AttemptCount + 1;
                var lockUntil = attempts >= maxAttempts ? nowUtc.AddMinutes(lockMinutes) : otp.LockedUntilUtc;
                otps[idx] = otp with { AttemptCount = attempts, LockedUntilUtc = lockUntil };
            }
            return Task.CompletedTask;
        }

        public Task<bool> MarkInviteOtpUsedAsync(long inviteOtpId, DateTime nowUtc, CancellationToken ct)
        {
            var idx = otps.FindIndex(x => x.InviteOtpId == inviteOtpId);
            if (idx < 0 || otps[idx].UsedAtUtc.HasValue)
                return Task.FromResult(false);
            otps[idx] = otps[idx] with { UsedAtUtc = nowUtc };
            return Task.FromResult(true);
        }

        public Task<bool> CompleteInviteAsync(long userInviteId, int userId, byte[] passwordHash, byte passwordHashVersion, bool useGravatar, DateTime nowUtc, CancellationToken ct)
        {
            var idx = invites.FindIndex(x => x.UserInviteId == userInviteId && x.UserId == userId);
            if (idx < 0)
                return Task.FromResult(false);

            var invite = invites[idx];
            if (invite.Status != UserInviteStatus.Active || invite.ExpiresAtUtc < nowUtc || invite.OtpVerifiedAtUtc is null)
                return Task.FromResult(false);

            invites[idx] = invite with { Status = UserInviteStatus.Used, UsedAtUtc = nowUtc };
            return Task.FromResult(true);
        }
    }

    private sealed class TestTimeProvider(DateTimeOffset nowUtc) : TimeProvider
    {
        private DateTimeOffset current = nowUtc;

        public override DateTimeOffset GetUtcNow() => current;

        public void Advance(TimeSpan delta) => current = current.Add(delta);
    }

    private sealed class FixedSecuritySettingsProvider(SecuritySettingsSnapshot settings) : ISecuritySettingsProvider
    {
        public Task<SecuritySettingsSnapshot> GetAsync(CancellationToken ct) => Task.FromResult(settings);
    }

    private static SecuritySettingsSnapshot BuildSecuritySettings()
    {
        return new SecuritySettingsSnapshot(
            PasswordPolicy: new PasswordPolicyRules(12, true, true, true),
            LoginLockoutThreshold: 5,
            LoginLockoutMinutes: 15,
            EmailCodeCooldownSeconds: 60,
            EmailCodeMaxPerHourPerEmail: 10,
            EmailCodeMaxPerHourPerIp: 50,
            EmailCodeExpiryMinutes: 10,
            EmailCodeMaxFailedAttemptsPerCode: 5,
            InviteExpiryHours: 24,
            InviteOtpExpiryMinutes: 10,
            InviteOtpCooldownSeconds: 1,
            InviteOtpMaxPerHourPerInvite: 10,
            InviteOtpMaxPerHourPerIp: 50,
            InviteOtpMaxAttempts: 5,
            InviteOtpLockMinutes: 10,
            InviteMaxAttempts: 10,
            InviteLockMinutes: 15,
            ChangePasswordOtpExpiryMinutes: 10,
            ChangePasswordOtpCooldownSeconds: 60,
            ChangePasswordOtpMaxPerHourPerUser: 3,
            ChangePasswordOtpMaxPerHourPerIp: 25,
            ChangePasswordOtpMaxAttempts: 5,
            ChangePasswordOtpLockMinutes: 15);
    }
}
