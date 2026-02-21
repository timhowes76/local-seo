using System.Diagnostics;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record BeginLoginResult(bool Success, string Message, int Rid, string EmailAddress);
public sealed record CompleteTwoFactorResult(bool Success, string Message, UserRecord? User);
public sealed record ResetPasswordResult(bool Success, string Message, UserRecord? User);

public interface IAuthService
{
    Task<BeginLoginResult> BeginLoginAsync(string emailAddress, string password, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct);
    Task<CompleteTwoFactorResult> CompleteTwoFactorLoginAsync(int rid, string emailAddress, string code, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct);
    Task<string> RequestForgotPasswordAsync(string emailAddress, string appBaseUrl, string? requestedFromIp, string? requestedUserAgent, CancellationToken ct);
    Task<ResetPasswordResult> ResetPasswordAsync(int rid, string emailAddress, string code, string newPassword, string confirmPassword, CancellationToken ct);
}

public sealed class AuthService(
    IUserRepository userRepository,
    IUserLoginLogRepository userLoginLogRepository,
    IEmailCodeService emailCodeService,
    IRateLimiterService rateLimiterService,
    ISendGridEmailService sendGridEmailService,
    IPasswordHasherService passwordHasherService,
    IEmailAddressNormalizer emailAddressNormalizer,
    ISecuritySettingsProvider securitySettingsProvider,
    TimeProvider timeProvider,
    ILogger<AuthService> logger) : IAuthService
{
    private static readonly TimeSpan ForgotPasswordMinimumResponseTime = TimeSpan.FromMilliseconds(400);
    private const string AuthStagePassword = "Password";
    private const string AuthStageTwoFactor = "TwoFactor";

    public async Task<BeginLoginResult> BeginLoginAsync(string emailAddress, string password, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct)
    {
        var enteredEmail = NormalizeNullable(emailAddress, 320);
        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var normalizedIpAddress = NormalizeIpAddress(requestedFromIp);
        var normalizedUserAgent = NormalizeNullable(requestedUserAgent, 512);
        var normalizedCorrelationId = NormalizeNullable(correlationId, 64);
        var securitySettings = await securitySettingsProvider.GetAsync(ct);
        const string genericFailureMessage = "Unable to sign in with those credentials.";
        const string temporaryFailureMessage = "Sign-in temporarily unavailable. Please try again shortly.";

        Task WriteLogAsync(bool succeeded, string? failureReason, int? userId)
        {
            return TryLogLoginAttemptAsync(new UserLoginAttempt
            {
                AttemptedAtUtc = nowUtc,
                EmailEntered = enteredEmail,
                EmailNormalized = normalizedEmail,
                UserId = userId,
                IpAddress = normalizedIpAddress,
                Succeeded = succeeded,
                FailureReason = failureReason,
                AuthStage = AuthStagePassword,
                UserAgent = normalizedUserAgent,
                CorrelationId = normalizedCorrelationId
            }, ct);
        }

        var user = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        if (user is null)
        {
            await WriteLogAsync(false, "UnknownEmail", null);
            logger.LogWarning("Login failed. Category={Category} Email={EmailNormalized}", "unknown_email", normalizedEmail);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (!user.IsActive)
        {
            await WriteLogAsync(false, "DisabledUser", user.Id);
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "disabled_user", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (user.LockedoutUntilUtc.HasValue && user.LockedoutUntilUtc.Value > nowUtc)
        {
            await WriteLogAsync(false, "LockedOut", user.Id);
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "locked_out", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (user.PasswordHash is null || user.PasswordHash.Length == 0)
        {
            await userRepository.RecordFailedPasswordAttemptAsync(user.Id, securitySettings.LoginLockoutThreshold, securitySettings.LoginLockoutMinutes, nowUtc, ct);
            await WriteLogAsync(false, "InvalidPassword", user.Id);
            logger.LogWarning("Login failed. Category={Category} UserId={UserId}", "password_not_set", user.Id);
            return new BeginLoginResult(false, genericFailureMessage, 0, string.Empty);
        }

        if (!passwordHasherService.VerifyPassword(user.PasswordHash, password, out var needsRehash))
        {
            await userRepository.RecordFailedPasswordAttemptAsync(user.Id, securitySettings.LoginLockoutThreshold, securitySettings.LoginLockoutMinutes, nowUtc, ct);
            await WriteLogAsync(false, "InvalidPassword", user.Id);
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
            await WriteLogAsync(false, "RateLimited", user.Id);
            return new BeginLoginResult(false, temporaryFailureMessage, 0, string.Empty);
        }

        try
        {
            var issued = await emailCodeService.IssueAsync(
                EmailCodePurpose.Login2Fa,
                user.EmailAddress,
                user.EmailAddressNormalized,
                requestedFromIp,
                requestedUserAgent,
                ct);

            await sendGridEmailService.SendLoginTwoFactorCodeAsync(user.EmailAddress, issued.Code, ct);
            await WriteLogAsync(true, null, user.Id);
            return new BeginLoginResult(true, "A verification code was sent to your email address.", issued.Rid, user.EmailAddress);
        }
        catch (Exception ex)
        {
            await WriteLogAsync(false, "TwoFactorDeliveryFailed", user.Id);
            logger.LogError(ex, "Login 2FA delivery failed for UserId={UserId}", user.Id);
            return new BeginLoginResult(false, temporaryFailureMessage, 0, string.Empty);
        }
    }

    public async Task<CompleteTwoFactorResult> CompleteTwoFactorLoginAsync(int rid, string emailAddress, string code, string? requestedFromIp, string? requestedUserAgent, string? correlationId, CancellationToken ct)
    {
        var enteredEmail = NormalizeNullable(emailAddress, 320);
        var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var normalizedIpAddress = NormalizeIpAddress(requestedFromIp);
        var normalizedUserAgent = NormalizeNullable(requestedUserAgent, 512);
        var normalizedCorrelationId = NormalizeNullable(correlationId, 64);
        const string genericFailureMessage = "Verification failed.";

        var mappedUser = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
        Task WriteLogAsync(bool succeeded, string? failureReason, int? userId)
        {
            return TryLogLoginAttemptAsync(new UserLoginAttempt
            {
                AttemptedAtUtc = nowUtc,
                EmailEntered = enteredEmail,
                EmailNormalized = normalizedEmail,
                UserId = userId,
                IpAddress = normalizedIpAddress,
                Succeeded = succeeded,
                FailureReason = failureReason,
                AuthStage = AuthStageTwoFactor,
                UserAgent = normalizedUserAgent,
                CorrelationId = normalizedCorrelationId
            }, ct);
        }

        if (rid <= 0)
        {
            await WriteLogAsync(false, "TwoFactorFailed", mappedUser?.Id);
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);
        }

        var valid = await emailCodeService.TryConsumeAsync(rid, EmailCodePurpose.Login2Fa, normalizedEmail, code, ct);
        if (!valid)
        {
            await WriteLogAsync(false, "TwoFactorFailed", mappedUser?.Id);
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);
        }

        var user = mappedUser;
        if (user is null)
        {
            await WriteLogAsync(false, "UnknownEmail", null);
            logger.LogWarning("2FA verify failed. Category={Category} Email={EmailNormalized}", "missing_user", normalizedEmail);
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);
        }

        if (!user.IsActive)
        {
            await WriteLogAsync(false, "DisabledUser", user.Id);
            logger.LogWarning("2FA verify failed. Category={Category} UserId={UserId}", "disabled_user", user.Id);
            return new CompleteTwoFactorResult(false, genericFailureMessage, null);
        }

        // TODO(auth-v2): add trusted-device support to optionally remember successful MFA devices.
        await userRepository.UpdateLastLoginAsync(user.Id, timeProvider.GetUtcNow().UtcDateTime, ct);
        await WriteLogAsync(true, null, user.Id);
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

    private async Task TryLogLoginAttemptAsync(UserLoginAttempt attempt, CancellationToken ct)
    {
        try
        {
            await userLoginLogRepository.InsertAsync(attempt, ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to write login log row. EmailNormalized={EmailNormalized} Stage={AuthStage}", attempt.EmailNormalized, attempt.AuthStage);
        }
    }

    private static string NormalizeIpAddress(string? requestedFromIp)
    {
        var trimmed = (requestedFromIp ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return "unknown";

        return trimmed.Length <= 45 ? trimmed : trimmed[..45];
    }

    private static string? NormalizeNullable(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;

        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
