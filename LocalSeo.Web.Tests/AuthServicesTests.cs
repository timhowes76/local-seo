using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using LocalSeo.Web.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Linq;

namespace LocalSeo.Web.Tests;

public class AuthServicesTests
{
    [Fact]
    public void Normalize_CanonicalizesEquivalentEmails_ForUniqueness()
    {
        var normalizer = new EmailAddressNormalizer();

        var a = normalizer.Normalize(" TIM.HOWES@Kontrolit.NET ");
        var b = normalizer.Normalize("tim.howes@kontrolit.net");

        Assert.Equal("tim.howes@kontrolit.net", a);
        Assert.Equal(a, b);
    }

    [Fact]
    public async Task BeginLogin_LocksUserAfterThresholdFailures()
    {
        var timeProvider = new TestTimeProvider(new DateTimeOffset(2026, 2, 20, 9, 0, 0, TimeSpan.Zero));
        var passwordHasher = new PasswordHasherService();
        var knownPassword = "Passw0rd!123";
        var userRepository = new InMemoryUserRepository(
            new UserRecord(
                Id: 1,
                FirstName: "Tim",
                LastName: "Howes",
                EmailAddress: "tim.howes@kontrolit.net",
                EmailAddressNormalized: "tim.howes@kontrolit.net",
                PasswordHash: passwordHasher.HashPassword(knownPassword),
                PasswordHashVersion: 1,
                IsActive: true,
                IsAdmin: true,
                DateCreatedAtUtc: timeProvider.GetUtcNow().UtcDateTime,
                DatePasswordLastSetUtc: null,
                LastLoginAtUtc: null,
                FailedPasswordAttempts: 0,
                LockedoutUntilUtc: null,
                InviteStatus: UserLifecycleStatus.Active));

        var service = new AuthService(
            userRepository,
            new NoopEmailCodeService(),
            new AllowAllRateLimiterService(),
            new NoopSendGridEmailService(),
            passwordHasher,
            new EmailAddressNormalizer(),
            Microsoft.Extensions.Options.Options.Create(new AuthOptions { LockoutThreshold = 2, LockoutMinutes = 15 }),
            timeProvider,
            NullLogger<AuthService>.Instance);

        var first = await service.BeginLoginAsync("tim.howes@kontrolit.net", "wrong-one", "127.0.0.1", "unit-test", CancellationToken.None);
        var second = await service.BeginLoginAsync("tim.howes@kontrolit.net", "wrong-two", "127.0.0.1", "unit-test", CancellationToken.None);
        var lockedAttempt = await service.BeginLoginAsync("tim.howes@kontrolit.net", knownPassword, "127.0.0.1", "unit-test", CancellationToken.None);

        Assert.False(first.Success);
        Assert.False(second.Success);
        Assert.False(lockedAttempt.Success);

        var user = await userRepository.GetByIdAsync(1, CancellationToken.None);
        Assert.NotNull(user);
        Assert.Equal(2, user!.FailedPasswordAttempts);
        Assert.Equal(timeProvider.GetUtcNow().UtcDateTime.AddMinutes(15), user.LockedoutUntilUtc);
    }

    [Fact]
    public async Task EmailCode_ExpiresAndIsSingleUse()
    {
        var timeProvider = new TestTimeProvider(new DateTimeOffset(2026, 2, 20, 11, 0, 0, TimeSpan.Zero));
        var repository = new InMemoryEmailCodeRepository(timeProvider);
        var service = new EmailCodeService(
            repository,
            new CodeHasher(),
            Microsoft.Extensions.Options.Options.Create(new EmailCodesOptions { ExpiryMinutes = 10 }),
            timeProvider,
            NullLogger<EmailCodeService>.Instance);

        var issued = await service.IssueAsync(
            EmailCodePurpose.Login2Fa,
            "tim.howes@kontrolit.net",
            "tim.howes@kontrolit.net",
            "127.0.0.1",
            "unit-test",
            CancellationToken.None);

        var firstConsume = await service.TryConsumeAsync(
            issued.Rid,
            EmailCodePurpose.Login2Fa,
            "tim.howes@kontrolit.net",
            issued.Code,
            CancellationToken.None);
        var secondConsume = await service.TryConsumeAsync(
            issued.Rid,
            EmailCodePurpose.Login2Fa,
            "tim.howes@kontrolit.net",
            issued.Code,
            CancellationToken.None);

        Assert.True(firstConsume);
        Assert.False(secondConsume);

        var expiringCode = await service.IssueAsync(
            EmailCodePurpose.ForgotPassword,
            "tim.howes@kontrolit.net",
            "tim.howes@kontrolit.net",
            "127.0.0.1",
            "unit-test",
            CancellationToken.None);

        timeProvider.Advance(TimeSpan.FromMinutes(11));

        var expiredConsume = await service.TryConsumeAsync(
            expiringCode.Rid,
            EmailCodePurpose.ForgotPassword,
            "tim.howes@kontrolit.net",
            expiringCode.Code,
            CancellationToken.None);

        Assert.False(expiredConsume);
    }

    [Fact]
    public async Task ForgotPassword_ResponseMessage_IsSameForKnownAndUnknownEmail()
    {
        var timeProvider = new TestTimeProvider(new DateTimeOffset(2026, 2, 20, 12, 0, 0, TimeSpan.Zero));
        var userRepository = new InMemoryUserRepository(
            new UserRecord(
                Id: 1,
                FirstName: "Tim",
                LastName: "Howes",
                EmailAddress: "tim.howes@kontrolit.net",
                EmailAddressNormalized: "tim.howes@kontrolit.net",
                PasswordHash: null,
                PasswordHashVersion: 1,
                IsActive: true,
                IsAdmin: true,
                DateCreatedAtUtc: timeProvider.GetUtcNow().UtcDateTime,
                DatePasswordLastSetUtc: null,
                LastLoginAtUtc: null,
                FailedPasswordAttempts: 0,
                LockedoutUntilUtc: null,
                InviteStatus: UserLifecycleStatus.Active));

        var sendGrid = new NoopSendGridEmailService();
        var service = new AuthService(
            userRepository,
            new NoopEmailCodeService(),
            new AllowAllRateLimiterService(),
            sendGrid,
            new PasswordHasherService(),
            new EmailAddressNormalizer(),
            Microsoft.Extensions.Options.Options.Create(new AuthOptions()),
            timeProvider,
            NullLogger<AuthService>.Instance);

        var known = await service.RequestForgotPasswordAsync("tim.howes@kontrolit.net", "https://example.test", "127.0.0.1", "unit-test", CancellationToken.None);
        var unknown = await service.RequestForgotPasswordAsync("nobody@example.test", "https://example.test", "127.0.0.1", "unit-test", CancellationToken.None);

        Assert.Equal("If that email exists, we've sent a code to continue.", known);
        Assert.Equal(known, unknown);
        Assert.Single(sendGrid.ForgotPasswordEmails);
    }

    private sealed class NoopEmailCodeService : IEmailCodeService
    {
        public Task<IssuedEmailCode> IssueAsync(EmailCodePurpose purpose, string email, string emailNormalized, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct)
            => Task.FromResult(new IssuedEmailCode(123, "123456", DateTime.UtcNow.AddMinutes(10)));

        public Task<bool> TryConsumeAsync(int rid, EmailCodePurpose purpose, string emailNormalized, string code, CancellationToken ct)
            => Task.FromResult(true);
    }

    private sealed class AllowAllRateLimiterService : IRateLimiterService
    {
        public Task<RateLimitDecision> CanRequestCodeAsync(string emailNormalized, string? requestedFromIp, CancellationToken ct)
            => Task.FromResult(new RateLimitDecision(true, null));
    }

    private sealed class NoopSendGridEmailService : ISendGridEmailService
    {
        public List<(string Email, string Code, string Url)> ForgotPasswordEmails { get; } = [];

        public Task SendLoginTwoFactorCodeAsync(string email, string code, CancellationToken ct) => Task.CompletedTask;

        public Task SendForgotPasswordCodeAsync(string email, string code, string resetUrl, CancellationToken ct)
        {
            ForgotPasswordEmails.Add((email, code, resetUrl));
            return Task.CompletedTask;
        }

        public Task SendUserInviteAsync(string email, string recipientName, string inviteUrl, DateTime expiresAtUtc, CancellationToken ct)
            => Task.CompletedTask;

        public Task SendInviteOtpAsync(string email, string code, DateTime expiresAtUtc, CancellationToken ct)
            => Task.CompletedTask;
    }

    private sealed class InMemoryUserRepository(UserRecord user) : IUserRepository
    {
        private UserRecord current = user;

        public Task<UserRecord?> GetByNormalizedEmailAsync(string emailNormalized, CancellationToken ct)
        {
            return Task.FromResult(
                string.Equals(current.EmailAddressNormalized, emailNormalized, StringComparison.Ordinal)
                    ? current
                    : null);
        }

        public Task<UserRecord?> GetByIdAsync(int id, CancellationToken ct)
        {
            return Task.FromResult(current.Id == id ? current : null);
        }

        public Task<IReadOnlyList<AdminUserListRow>> ListByStatusAsync(UserStatusFilter filter, string? searchTerm, CancellationToken ct)
        {
            IReadOnlyList<AdminUserListRow> rows =
            [
                new AdminUserListRow(
                    current.Id,
                    $"{current.FirstName} {current.LastName}",
                    current.EmailAddress,
                    current.DateCreatedAtUtc,
                    current.IsActive,
                    current.InviteStatus,
                    LastInviteCreatedAtUtc: null,
                    HasActiveInvite: false)
            ];
            return Task.FromResult(rows);
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
        {
            if (current.Id != userId)
                return Task.FromResult(false);

            current = current with
            {
                IsActive = false,
                InviteStatus = UserLifecycleStatus.Disabled
            };
            return Task.FromResult(true);
        }

        public Task RecordFailedPasswordAttemptAsync(int userId, int lockoutThreshold, int lockoutMinutes, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id != userId)
                return Task.CompletedTask;

            var nextAttempts = current.FailedPasswordAttempts + 1;
            var lockoutUntilUtc = nextAttempts >= lockoutThreshold ? nowUtc.AddMinutes(lockoutMinutes) : current.LockedoutUntilUtc;
            current = current with { FailedPasswordAttempts = nextAttempts, LockedoutUntilUtc = lockoutUntilUtc };
            return Task.CompletedTask;
        }

        public Task ClearFailedPasswordAttemptsAsync(int userId, CancellationToken ct)
        {
            if (current.Id == userId)
                current = current with { FailedPasswordAttempts = 0, LockedoutUntilUtc = null };
            return Task.CompletedTask;
        }

        public Task UpdateLastLoginAsync(int userId, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id == userId)
                current = current with { LastLoginAtUtc = nowUtc };
            return Task.CompletedTask;
        }

        public Task UpdatePasswordAsync(int userId, byte[] passwordHash, byte passwordHashVersion, DateTime nowUtc, CancellationToken ct)
        {
            if (current.Id == userId)
            {
                current = current with
                {
                    PasswordHash = passwordHash,
                    PasswordHashVersion = passwordHashVersion,
                    DatePasswordLastSetUtc = nowUtc,
                    FailedPasswordAttempts = 0,
                    LockedoutUntilUtc = null
                };
            }

            return Task.CompletedTask;
        }
    }

    private sealed class InMemoryEmailCodeRepository(TestTimeProvider timeProvider) : IEmailCodeRepository
    {
        private readonly List<EmailCodeRecord> rows = [];
        private int nextId = 1;

        public Task<int> CreateAsync(EmailCodeCreateRequest request, CancellationToken ct)
        {
            var id = nextId++;
            rows.Add(new EmailCodeRecord(
                id,
                request.Purpose,
                request.Email,
                request.EmailNormalized,
                request.CodeHash,
                request.Salt,
                request.ExpiresAtUtc,
                timeProvider.GetUtcNow().UtcDateTime,
                0,
                false));
            return Task.FromResult(id);
        }

        public Task<EmailCodeRecord?> GetByIdAsync(int emailCodeId, CancellationToken ct)
        {
            return Task.FromResult(rows.SingleOrDefault(x => x.EmailCodeId == emailCodeId));
        }

        public Task IncrementFailedAttemptsAsync(int emailCodeId, CancellationToken ct)
        {
            var idx = rows.FindIndex(x => x.EmailCodeId == emailCodeId);
            if (idx >= 0)
            {
                var row = rows[idx];
                rows[idx] = row with { FailedAttempts = row.FailedAttempts + 1 };
            }
            return Task.CompletedTask;
        }

        public Task<bool> TryMarkUsedAsync(int emailCodeId, CancellationToken ct)
        {
            var idx = rows.FindIndex(x => x.EmailCodeId == emailCodeId);
            if (idx < 0 || rows[idx].IsUsed)
                return Task.FromResult(false);

            rows[idx] = rows[idx] with { IsUsed = true };
            return Task.FromResult(true);
        }

        public Task<DateTime?> GetLatestCreatedAtUtcAsync(string emailNormalized, CancellationToken ct)
        {
            var latest = rows
                .Where(x => string.Equals(x.EmailNormalized, emailNormalized, StringComparison.Ordinal))
                .Select(x => (DateTime?)x.CreatedAtUtc)
                .OrderByDescending(x => x)
                .FirstOrDefault();
            return Task.FromResult(latest);
        }

        public Task<int> CountCreatedInLastHourForEmailAsync(string emailNormalized, DateTime sinceUtc, CancellationToken ct)
        {
            var count = rows.Count(x =>
                string.Equals(x.EmailNormalized, emailNormalized, StringComparison.Ordinal)
                && x.CreatedAtUtc >= sinceUtc);
            return Task.FromResult(count);
        }

        public Task<int> CountCreatedInLastHourForIpAsync(string requestedFromIp, DateTime sinceUtc, CancellationToken ct)
        {
            return Task.FromResult(0);
        }
    }

    private sealed class TestTimeProvider(DateTimeOffset nowUtc) : TimeProvider
    {
        private DateTimeOffset current = nowUtc;

        public override DateTimeOffset GetUtcNow() => current;

        public void Advance(TimeSpan delta) => current = current.Add(delta);
    }
}
