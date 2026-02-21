using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Linq;

namespace LocalSeo.Web.Tests;

public class PasswordChangeServiceTests
{
    [Fact]
    public async Task StartAndVerify_ChangesPassword_AndBumpsSessionVersion()
    {
        var now = new DateTimeOffset(2026, 2, 21, 14, 0, 0, TimeSpan.Zero);
        var timeProvider = new TestTimeProvider(now);
        var passwordHasher = new PasswordHasherService();
        var userRepo = new InMemoryUserRepository(new UserRecord(
            Id: 7,
            FirstName: "Alex",
            LastName: "Jones",
            EmailAddress: "alex@example.test",
            EmailAddressNormalized: "alex@example.test",
            PasswordHash: passwordHasher.HashPassword("CurrentPass!123"),
            PasswordHashVersion: 1,
            IsActive: true,
            IsAdmin: false,
            DateCreatedAtUtc: now.UtcDateTime,
            DatePasswordLastSetUtc: now.UtcDateTime.AddDays(-30),
            LastLoginAtUtc: now.UtcDateTime.AddDays(-1),
            FailedPasswordAttempts: 0,
            LockedoutUntilUtc: null,
            InviteStatus: UserLifecycleStatus.Active,
            SessionVersion: 0,
            UseGravatar: false));
        var otpRepo = new InMemoryUserOtpRepository();
        var email = new CaptureSendGridEmailService();
        var crypto = new CryptoService(Microsoft.Extensions.Options.Options.Create(new InviteOptions
        {
            HmacSecret = "abcdefghijklmnopqrstuvwxyz123456"
        }));

        var service = new PasswordChangeService(
            userRepo,
            otpRepo,
            passwordHasher,
            crypto,
            email,
            Microsoft.Extensions.Options.Options.Create(new AuthOptions { LockoutThreshold = 5, LockoutMinutes = 15 }),
            Microsoft.Extensions.Options.Options.Create(new ChangePasswordOptions
            {
                OtpExpiryMinutes = 10,
                OtpCooldownSeconds = 1,
                OtpMaxPerHourPerUser = 10,
                OtpMaxPerHourPerIp = 50,
                OtpMaxAttempts = 5,
                OtpLockMinutes = 15,
                PasswordMinLength = 10
            }),
            timeProvider,
            NullLogger<PasswordChangeService>.Instance);

        var start = await service.StartAsync(7, "CurrentPass!123", "127.0.0.1", "unit-test", "corr-a", CancellationToken.None);
        Assert.True(start.Success);
        Assert.False(string.IsNullOrWhiteSpace(start.CorrelationId));
        Assert.NotNull(email.LastChangePasswordCode);

        var verify = await service.VerifyAndChangePasswordAsync(
            7,
            start.CorrelationId,
            email.LastChangePasswordCode,
            "BrandNew!456",
            "BrandNew!456",
            "127.0.0.1",
            "unit-test",
            "verify-correlation",
            CancellationToken.None);

        Assert.True(verify.Success);
        var updated = await userRepo.GetByIdAsync(7, CancellationToken.None);
        Assert.NotNull(updated);
        Assert.Equal(1, updated!.SessionVersion);
        Assert.True(passwordHasher.VerifyPassword(updated.PasswordHash!, "BrandNew!456", out _));
    }

    [Fact]
    public async Task VerifyOtp_LocksAfterMaxAttempts()
    {
        var now = new DateTimeOffset(2026, 2, 21, 15, 0, 0, TimeSpan.Zero);
        var timeProvider = new TestTimeProvider(now);
        var passwordHasher = new PasswordHasherService();
        var userRepo = new InMemoryUserRepository(new UserRecord(
            Id: 11,
            FirstName: "Chris",
            LastName: "Miles",
            EmailAddress: "chris@example.test",
            EmailAddressNormalized: "chris@example.test",
            PasswordHash: passwordHasher.HashPassword("CurrentPass!123"),
            PasswordHashVersion: 1,
            IsActive: true,
            IsAdmin: false,
            DateCreatedAtUtc: now.UtcDateTime,
            DatePasswordLastSetUtc: now.UtcDateTime.AddDays(-10),
            LastLoginAtUtc: now.UtcDateTime,
            FailedPasswordAttempts: 0,
            LockedoutUntilUtc: null,
            InviteStatus: UserLifecycleStatus.Active,
            SessionVersion: 0,
            UseGravatar: false));
        var otpRepo = new InMemoryUserOtpRepository();
        var email = new CaptureSendGridEmailService();
        var crypto = new CryptoService(Microsoft.Extensions.Options.Options.Create(new InviteOptions
        {
            HmacSecret = "abcdefghijklmnopqrstuvwxyz123456"
        }));

        var service = new PasswordChangeService(
            userRepo,
            otpRepo,
            passwordHasher,
            crypto,
            email,
            Microsoft.Extensions.Options.Options.Create(new AuthOptions { LockoutThreshold = 5, LockoutMinutes = 15 }),
            Microsoft.Extensions.Options.Options.Create(new ChangePasswordOptions
            {
                OtpExpiryMinutes = 10,
                OtpCooldownSeconds = 1,
                OtpMaxPerHourPerUser = 10,
                OtpMaxPerHourPerIp = 50,
                OtpMaxAttempts = 5,
                OtpLockMinutes = 15,
                PasswordMinLength = 10
            }),
            timeProvider,
            NullLogger<PasswordChangeService>.Instance);

        var start = await service.StartAsync(11, "CurrentPass!123", "127.0.0.1", "unit-test", "corr-b", CancellationToken.None);
        Assert.True(start.Success);
        Assert.False(string.IsNullOrWhiteSpace(start.CorrelationId));

        for (var i = 0; i < 5; i++)
        {
            var wrong = await service.VerifyAndChangePasswordAsync(
                11,
                start.CorrelationId,
                "000000",
                "BrandNew!456",
                "BrandNew!456",
                "127.0.0.1",
                "unit-test",
                "verify-correlation",
                CancellationToken.None);
            Assert.False(wrong.Success);
        }

        var blocked = await service.VerifyAndChangePasswordAsync(
            11,
            start.CorrelationId,
            email.LastChangePasswordCode,
            "BrandNew!456",
            "BrandNew!456",
            "127.0.0.1",
            "unit-test",
            "verify-correlation",
            CancellationToken.None);
        Assert.False(blocked.Success);
    }

    private sealed class CaptureSendGridEmailService : ISendGridEmailService
    {
        public string? LastChangePasswordCode { get; private set; }

        public Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct) => Task.CompletedTask;
        public Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct) => Task.CompletedTask;
        public Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct) => Task.CompletedTask;
        public Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct) => Task.CompletedTask;
        public Task SendChangePasswordOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
        {
            LastChangePasswordCode = code;
            return Task.CompletedTask;
        }
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
            current = current with { FirstName = firstName, LastName = lastName, UseGravatar = useGravatar };
            return Task.FromResult(true);
        }

        public Task<bool> UpdateUserAsync(int userId, string firstName, string lastName, string emailAddress, string emailAddressNormalized, bool isAdmin, UserLifecycleStatus inviteStatus, CancellationToken ct)
            => Task.FromResult(false);

        public Task<bool> DeleteUserAsync(int userId, CancellationToken ct)
            => Task.FromResult(false);

        public Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id == userId)
            {
                var attempts = current.FailedPasswordAttempts + 1;
                var lockout = attempts >= lockoutThreshold ? nowUtc.AddMinutes(lockoutMinutes) : current.LockedoutUntilUtc;
                current = current with { FailedPasswordAttempts = attempts, LockedoutUntilUtc = lockout };
            }
            return Task.CompletedTask;
        }

        public Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct)
        {
            if (current.Id == userId)
                current = current with { FailedPasswordAttempts = 0, LockedoutUntilUtc = null };
            return Task.CompletedTask;
        }

        public Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct) => Task.CompletedTask;

        public Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task<bool> UpdatePasswordAndBumpSessionVersionAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id != userId)
                return Task.FromResult(false);
            current = current with
            {
                PasswordHash = passwordHash,
                PasswordHashVersion = passwordHashVersion,
                DatePasswordLastSetUtc = nowUtc,
                FailedPasswordAttempts = 0,
                LockedoutUntilUtc = null,
                SessionVersion = current.SessionVersion + 1
            };
            return Task.FromResult(true);
        }
    }

    private sealed class InMemoryUserOtpRepository : IUserOtpRepository
    {
        private readonly List<UserOtpRecord> rows = [];
        private long nextId = 1;

        public Task<long> CreateOtpAsync(UserOtpCreateRequest request, CancellationToken ct)
        {
            var id = nextId++;
            rows.Add(new UserOtpRecord(
                id,
                request.UserId,
                request.Purpose,
                request.CodeHash,
                request.ExpiresAtUtc,
                null,
                request.SentAtUtc,
                0,
                null,
                request.CorrelationId,
                request.RequestedFromIp));
            return Task.FromResult(id);
        }

        public Task<DateTime?> GetLatestSentAtUtcAsync(int userId, string purpose, CancellationToken ct)
            => Task.FromResult(rows.Where(x => x.UserId == userId && x.Purpose == purpose).Select(x => (DateTime?)x.SentAtUtc).OrderByDescending(x => x).FirstOrDefault());

        public Task<int> CountSentSinceAsync(int userId, string purpose, DateTime sinceUtc, CancellationToken ct)
            => Task.FromResult(rows.Count(x => x.UserId == userId && x.Purpose == purpose && x.SentAtUtc >= sinceUtc));

        public Task<int> CountSentSinceForIpAsync(string requestedFromIp, string purpose, DateTime sinceUtc, CancellationToken ct)
            => Task.FromResult(rows.Count(x => string.Equals(x.RequestedFromIp, requestedFromIp, StringComparison.Ordinal) && x.Purpose == purpose && x.SentAtUtc >= sinceUtc));

        public Task<UserOtpRecord?> GetLatestByCorrelationAsync(int userId, string purpose, string correlationId, CancellationToken ct)
            => Task.FromResult(rows
                .Where(x => x.UserId == userId && x.Purpose == purpose && string.Equals(x.CorrelationId, correlationId, StringComparison.Ordinal))
                .OrderByDescending(x => x.SentAtUtc)
                .ThenByDescending(x => x.UserOtpId)
                .FirstOrDefault());

        public Task<int> RevokeActiveByCorrelationAsync(int userId, string purpose, string correlationId, DateTime nowUtc, CancellationToken ct)
        {
            var count = 0;
            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                if (row.UserId == userId && row.Purpose == purpose && row.CorrelationId == correlationId && !row.UsedAtUtc.HasValue)
                {
                    rows[i] = row with { UsedAtUtc = nowUtc };
                    count++;
                }
            }
            return Task.FromResult(count);
        }

        public Task<int> RevokeActiveByUserPurposeAsync(int userId, string purpose, DateTime nowUtc, CancellationToken ct)
        {
            var count = 0;
            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                if (row.UserId == userId && row.Purpose == purpose && !row.UsedAtUtc.HasValue)
                {
                    rows[i] = row with { UsedAtUtc = nowUtc };
                    count++;
                }
            }
            return Task.FromResult(count);
        }

        public Task MarkAttemptFailureAsync(long userOtpId, DateTime nowUtc, int maxAttempts, int lockMinutes, CancellationToken ct)
        {
            var idx = rows.FindIndex(x => x.UserOtpId == userOtpId && !x.UsedAtUtc.HasValue);
            if (idx >= 0)
            {
                var row = rows[idx];
                var attempts = row.AttemptCount + 1;
                var lockedUntil = attempts >= maxAttempts ? nowUtc.AddMinutes(lockMinutes) : row.LockedUntilUtc;
                rows[idx] = row with { AttemptCount = attempts, LockedUntilUtc = lockedUntil };
            }
            return Task.CompletedTask;
        }

        public Task<bool> MarkUsedAsync(long userOtpId, DateTime nowUtc, CancellationToken ct)
        {
            var idx = rows.FindIndex(x => x.UserOtpId == userOtpId && !x.UsedAtUtc.HasValue);
            if (idx < 0)
                return Task.FromResult(false);

            rows[idx] = rows[idx] with { UsedAtUtc = nowUtc };
            return Task.FromResult(true);
        }
    }

    private sealed class TestTimeProvider(DateTimeOffset nowUtc) : TimeProvider
    {
        private DateTimeOffset current = nowUtc;
        public override DateTimeOffset GetUtcNow() => current;
    }
}
