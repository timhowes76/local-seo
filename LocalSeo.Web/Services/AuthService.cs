using System.Diagnostics;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record BeginLoginResult(bool Success, string Message, int Rid, string EmailAddress);
public sealed record CompleteTwoFactorResult(bool Success, string Message, UserRecord? User);
public sealed record ResetPasswordResult(bool Success, string Message, UserRecord? User);

public interface IAuthService
{
    Task<BeginLoginResult> BeginLoginAsync(string emailAddress, string password, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct);
    Task<CompleteTwoFactorResult> CompleteTwoFactorLoginAsync(int rid, string emailAddress, string code, CancellationToken ct);
    Task<string> RequestForgotPasswordAsync(string emailAddress, string appBaseUrl, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct);
    Task<ResetPasswordResult> ResetPasswordAsync(int rid, string emailAddress, string code, string newPassword, string confirmPassword, CancellationToken ct);
}

public sealed class AuthService(
    IUserRepository userRepository,
    IEmailCodeService emailCodeService,
    IRateLimiterService rateLimiterService,
    ISendGridEmailService sendGridEmailService,
    IPasswordHasherService passwordHasherService,
    IEmailAddressNormalizer emailAddressNormalizer,
    IOptions<AuthOptions> authOptions,
    TimeProvider timeProvider,
    ILogger<AuthService> logger) : IAuthService
{
    private static readonly TimeSpan ForgotPasswordMinimumResponseTime = TimeSpan.FromMilliseconds(400);

    public async Task<BeginLoginResult> BeginLoginAsync(string emailAddress, string password, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct)
    {
        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        const string genericFailureMessage = "Unable to sign in with those credentials.";

        var user = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        if (user is null || !user.IsActive)
        {
            logger.LogWarning("Login failed. Category={Category} Email={EmailNormalized}", "inactive_or_missing_user", normalizedEmail);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (user.LockedoutUntilUtc.HasValue && user.LockedoutUntilUtc.Value > nowUtc)
        {
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "locked_out", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (user.PasswordHash is null || user.PasswordHash.Length == 0)
        {
            await userRepository.RecordFailedPasswordAttemptAsync(user.Id, authOptions.Value.LockoutThreshold, authOptions.Value.LockoutMinutes, nowUtc, ct);
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "password_not_set", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (!passwordHasherService.VerifyPassword(user.PasswordHash, password, out var needsRehash))
        {
            await userRepository.RecordFailedPasswordAttemptAsync(user.Id, authOptions.Value.LockoutThreshold, authOptions.Value.LockoutMinutes, nowUtc, ct);
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "password_mismatch", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (needsRehash)
        {
            var upgradedHash = passwordHasherService.HashPassword(password);
            await userRepository.UpdatePasswordAsync(user.Id, upgradedHash, passwordHasherService.PasswordHashVersion, nowUtc, ct);
        }

        await userRepository.ClearFailedPasswordAttemptsAsync(user.Id, ct);

        var rateLimitDecision = await rateLimiterService.CanRequestCodeAsync(normalizedEmail, requestedFromIp, ct);
        if (!rateLimitDecision.Allowed)
        {
            logger.LogWarning(
                "Login failed. Category={Category} UserId={UserId} Email={EmailNormalized}",
                rateLimitDecision.ReasonCategory ?? "rate_limited",
                user.Id,
                normalizedEmail);
            return new BeginLoginResult(false, "Sign-in temporarily unavailable. Please try again shortly.", 0, string.Empty);
        }

        var issued = await emailCodeService.IssueAsync(
            EmailCodePurpose.Login2Fa,
            user.EmailAddress,
            user.EmailAddressNormalized,
            requestedFromIp,
            requestedUserAgent,
            ct);

        await sendGridEmailService.SendLoginTwoFactorCodeAsync(user.EmailAddress, issued.Code, ct);
        return new BeginLoginResult(true, "A verification code was sent to your email address.", issued.Rid, user.EmailAddress);
    }

    public async Task<CompleteTwoFactorResult> CompleteTwoFactorLoginAsync(int rid, string emailAddress, string code, CancellationToken ct)
    {
        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        const string genericFailureMessage = "Verification failed.";

        if (rid <= 0)
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);

        var valid = await emailCodeService.TryConsumeAsync(rid, EmailCodePurpose.Login2Fa, normalizedEmail, code, ct);
        if (!valid)
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);

        var user = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        if (user is null || !user.IsActive)
        {
            logger.LogWarning("2FA verify failed. Category={Category} Email={EmailNormalized}", "inactive_or_missing_user", normalizedEmail);
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);
        }

        // TODO(auth-v2): add trusted-device support to optionally remember successful MFA devices.
        await userRepository.UpdateLastLoginAsync(user.Id, timeProvider.GetUtcNow().UtcDateTime, ct);
        return new CompleteTwoFactorResult(true, "Login successful.", user);
    }

    public async Task<string> RequestForgotPasswordAsync(string emailAddress, string appBaseUrl, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct)
    {
        var startedAt = Stopwatch.StartNew();
        const string genericResponse = "If that email exists, we've sent a code to continue.";
        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);

        try
        {
            if (string.IsNullOrWhiteSpace(normalizedEmail))
                return genericResponse;

            var rateLimitDecision = await rateLimiterService.CanRequestCodeAsync(normalizedEmail, requestedFromIp, ct);
            if (!rateLimitDecision.Allowed)
            {
                logger.LogWarning(
                    "Forgot password request throttled. Category={Category} Email={EmailNormalized}",
                    rateLimitDecision.ReasonCategory ?? "rate_limited",
                    normalizedEmail);
                return genericResponse;
            }

            var user = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
            if (user is not null && user.IsActive)
            {
                var issued = await emailCodeService.IssueAsync(
                    EmailCodePurpose.ForgotPassword,
                    user.EmailAddress,
                    user.EmailAddressNormalized,
                    requestedFromIp,
                    requestedUserAgent,
                    ct);

                var trimmedBaseUrl = (appBaseUrl ?? string.Empty).Trim().TrimEnd('/');
                var resetUrl = $"{trimmedBaseUrl}/reset-password?rid={issued.Rid}";
                await sendGridEmailService.SendForgotPasswordCodeAsync(user.EmailAddress, issued.Code, resetUrl, ct);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Forgot password request failed for {EmailNormalized}", normalizedEmail);
        }
        finally
        {
            var remaining = ForgotPasswordMinimumResponseTime - startedAt.Elapsed;
            if (remaining > TimeSpan.Zero)
                await Task.Delay(remaining, ct);
        }

        return genericResponse;
    }

    public async Task<ResetPasswordResult> ResetPasswordAsync(int rid, string emailAddress, string code, string newPassword, string confirmPassword, CancellationToken ct)
    {
        const string genericFailure = "Unable to reset password with the details provided.";
        if (rid <= 0)
            return new ResetPasswordResult(false, genericFailure, null);
        if (string.IsNullOrWhiteSpace(newPassword) || !string.Equals(newPassword, confirmPassword, StringComparison.Ordinal))
            return new ResetPasswordResult(false, genericFailure, null);

        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        var consumed = await emailCodeService.TryConsumeAsync(rid, EmailCodePurpose.ForgotPassword, normalizedEmail, code, ct);
        if (!consumed)
        {
            logger.LogWarning("Password reset failed. Category={Category} Rid={Rid}", "invalid_code", rid);
            return new ResetPasswordResult(false, genericFailure, null);
        }

        var user = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        if (user is null || !user.IsActive)
        {
            logger.LogWarning("Password reset failed. Category={Category} Email={EmailNormalized}", "inactive_or_missing_user", normalizedEmail);
            return new ResetPasswordResult(false, genericFailure, null);
        }

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var passwordHash = passwordHasherService.HashPassword(newPassword);
        await userRepository.UpdatePasswordAsync(user.Id, passwordHash, passwordHasherService.PasswordHashVersion, nowUtc, ct);

        logger.LogInformation("Password reset succeeded. UserId={UserId}", user.Id);
        var refreshedUser = await userRepository.GetByIdAsync(user.Id, ct) ?? user;
        return new ResetPasswordResult(true, "Password reset successful.", refreshedUser);
    }
}
