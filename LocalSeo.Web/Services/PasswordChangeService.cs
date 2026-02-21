using System.Security.Cryptography;
using System.Text;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record ChangePasswordStartResult(bool Success, string Message, string? CorrelationId);
public sealed record ChangePasswordChallengeResult(bool Success, string Message, UserOtpRecord? Challenge);
public sealed record ChangePasswordVerifyResult(bool Success, string Message);

public interface IPasswordChangeService
{
    Task<ChangePasswordStartResult> StartAsync(int userId, string? currentPassword, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct);
    Task<ChangePasswordStartResult> ResendAsync(int userId, string? correlationId, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct);
    Task<ChangePasswordChallengeResult> GetChallengeAsync(int userId, string? correlationId, CancellationToken ct);
    Task<ChangePasswordVerifyResult> VerifyAndChangePasswordAsync(int userId, string? correlationId, string? otpCode, string? newPassword, string? confirmPassword, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct);
}

public sealed class PasswordChangeService(
    IUserRepository userRepository,
    IUserOtpRepository userOtpRepository,
    IPasswordHasherService passwordHasherService,
    ICryptoService cryptoService,
    ISendGridEmailService sendGridEmailService,
    IOptions<AuthOptions> authOptions,
    IOptions<ChangePasswordOptions> options,
    TimeProvider timeProvider,
    ILogger<PasswordChangeService> logger) : IPasswordChangeService
{
    private const string GenericInvalidCredentials = "Invalid credentials.";
    private const string GenericInvalidOrExpiredLink = "This challenge is invalid or expired.";
    private const string GenericVerificationFailure = "Verification failed. Please try again.";

    public async Task<ChangePasswordStartResult> StartAsync(int userId, string? currentPassword, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct)
    {
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        try
        {
            var user = await userRepository.GetByIdAsync(userId, ct);
            if (user is null || !user.IsActive || user.InviteStatus != UserLifecycleStatus.Active)
                return new ChangePasswordStartResult(false, GenericInvalidCredentials, null);

            if (user.LockedoutUntilUtc.HasValue && user.LockedoutUntilUtc.Value > nowUtc)
                return new ChangePasswordStartResult(false, GenericInvalidCredentials, null);

            var current = (currentPassword ?? string.Empty).Trim();
            if (current.Length == 0 || user.PasswordHash is null || user.PasswordHash.Length == 0)
            {
                await userRepository.RecordFailedPasswordAttemptAsync(userId, authOptions.Value.LockoutThreshold, authOptions.Value.LockoutMinutes, nowUtc, ct);
                return new ChangePasswordStartResult(false, GenericInvalidCredentials, null);
            }

            if (!passwordHasherService.VerifyPassword(user.PasswordHash, current, out _))
            {
                await userRepository.RecordFailedPasswordAttemptAsync(userId, authOptions.Value.LockoutThreshold, authOptions.Value.LockoutMinutes, nowUtc, ct);
                logger.LogWarning("Audit PasswordChangeStartFailed UserId={UserId} AtUtc={AtUtc} Reason=InvalidCurrentPassword", userId, nowUtc);
                return new ChangePasswordStartResult(false, GenericInvalidCredentials, null);
            }

            await userRepository.ClearFailedPasswordAttemptsAsync(userId, ct);

            var rateLimitDecision = await CheckOtpRateLimitAsync(userId, requestedFromIp, nowUtc, ct);
            if (!rateLimitDecision.Allowed)
                return new ChangePasswordStartResult(false, "Verification is temporarily unavailable. Please try again shortly.", null);

            var challengeCorrelationId = cryptoService.Base64UrlEncode(cryptoService.GenerateRandomBytes(24));
            await userOtpRepository.RevokeActiveByCorrelationAsync(userId, UserOtpPurpose.ChangePassword, challengeCorrelationId, nowUtc, ct);

            var code = RandomNumberGenerator.GetInt32(0, 1_000_000).ToString("D6");
            var codeHash = ComputeChangePasswordOtpHash(userId, challengeCorrelationId, code);
            var expiresAtUtc = nowUtc.AddMinutes(Math.Max(1, options.Value.OtpExpiryMinutes));
            await userOtpRepository.CreateOtpAsync(new UserOtpCreateRequest(
                userId,
                UserOtpPurpose.ChangePassword,
                codeHash,
                expiresAtUtc,
                nowUtc,
                challengeCorrelationId,
                requestedFromIp), ct);

            await sendGridEmailService.SendChangePasswordOtpAsync(user.EmailAddress, code, expiresAtUtc, ct);
            logger.LogInformation(
                "Audit PasswordChangeOtpSent UserId={UserId} CorrelationId={CorrelationId} RequestedFromIp={RequestedFromIp} UserAgent={UserAgent} AuditCorrelationId={AuditCorrelationId} AtUtc={AtUtc}",
                userId,
                challengeCorrelationId,
                Normalize(requestedFromIp, 45),
                Normalize(requestedUserAgent, 512),
                Normalize(auditCorrelationId, 64),
                nowUtc);

            return new ChangePasswordStartResult(true, "A verification code has been sent to your email.", challengeCorrelationId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Change password start failed for UserId={UserId}", userId);
            return new ChangePasswordStartResult(false, "Unable to start password change right now.", null);
        }
    }

    public async Task<ChangePasswordStartResult> ResendAsync(int userId, string? correlationId, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct)
    {
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        try
        {
            var challengeResult = await GetChallengeAsync(userId, correlationId, ct);
            if (!challengeResult.Success || challengeResult.Challenge is null)
                return new ChangePasswordStartResult(false, GenericInvalidOrExpiredLink, null);

            var user = await userRepository.GetByIdAsync(userId, ct);
            if (user is null || !user.IsActive || user.InviteStatus != UserLifecycleStatus.Active)
                return new ChangePasswordStartResult(false, GenericInvalidOrExpiredLink, null);

            var rateLimitDecision = await CheckOtpRateLimitAsync(userId, requestedFromIp, nowUtc, ct);
            if (!rateLimitDecision.Allowed)
                return new ChangePasswordStartResult(false, "Please wait before requesting another verification code.", challengeResult.Challenge.CorrelationId);

            await userOtpRepository.RevokeActiveByCorrelationAsync(userId, UserOtpPurpose.ChangePassword, challengeResult.Challenge.CorrelationId ?? string.Empty, nowUtc, ct);

            var code = RandomNumberGenerator.GetInt32(0, 1_000_000).ToString("D6");
            var codeHash = ComputeChangePasswordOtpHash(userId, challengeResult.Challenge.CorrelationId ?? string.Empty, code);
            var expiresAtUtc = nowUtc.AddMinutes(Math.Max(1, options.Value.OtpExpiryMinutes));
            await userOtpRepository.CreateOtpAsync(new UserOtpCreateRequest(
                userId,
                UserOtpPurpose.ChangePassword,
                codeHash,
                expiresAtUtc,
                nowUtc,
                challengeResult.Challenge.CorrelationId ?? string.Empty,
                requestedFromIp), ct);

            await sendGridEmailService.SendChangePasswordOtpAsync(user.EmailAddress, code, expiresAtUtc, ct);
            logger.LogInformation(
                "Audit PasswordChangeOtpResent UserId={UserId} CorrelationId={CorrelationId} RequestedFromIp={RequestedFromIp} UserAgent={UserAgent} AuditCorrelationId={AuditCorrelationId} AtUtc={AtUtc}",
                userId,
                challengeResult.Challenge.CorrelationId,
                Normalize(requestedFromIp, 45),
                Normalize(requestedUserAgent, 512),
                Normalize(auditCorrelationId, 64),
                nowUtc);

            return new ChangePasswordStartResult(true, "A new verification code has been sent.", challengeResult.Challenge.CorrelationId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Change password resend failed for UserId={UserId}", userId);
            return new ChangePasswordStartResult(false, "Unable to resend verification code right now.", null);
        }
    }

    public async Task<ChangePasswordChallengeResult> GetChallengeAsync(int userId, string? correlationId, CancellationToken ct)
    {
        try
        {
            var normalizedCorrelation = Normalize(correlationId, 64);
            if (string.IsNullOrWhiteSpace(normalizedCorrelation))
                return new ChangePasswordChallengeResult(false, GenericInvalidOrExpiredLink, null);

            var challenge = await userOtpRepository.GetLatestByCorrelationAsync(userId, UserOtpPurpose.ChangePassword, normalizedCorrelation, ct);
            if (challenge is null)
                return new ChangePasswordChallengeResult(false, GenericInvalidOrExpiredLink, null);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            if (challenge.UsedAtUtc.HasValue || challenge.ExpiresAtUtc < nowUtc)
                return new ChangePasswordChallengeResult(false, GenericInvalidOrExpiredLink, null);

            return new ChangePasswordChallengeResult(true, string.Empty, challenge);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Change password challenge lookup failed for UserId={UserId}", userId);
            return new ChangePasswordChallengeResult(false, GenericInvalidOrExpiredLink, null);
        }
    }

    public async Task<ChangePasswordVerifyResult> VerifyAndChangePasswordAsync(int userId, string? correlationId, string? otpCode, string? newPassword, string? confirmPassword, string? requestedFromIp, string? requestedUserAgent, string? auditCorrelationId, CancellationToken ct)
    {
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        try
        {
            var challengeResult = await GetChallengeAsync(userId, correlationId, ct);
            if (!challengeResult.Success || challengeResult.Challenge is null)
                return new ChangePasswordVerifyResult(false, GenericInvalidOrExpiredLink);

            var challenge = challengeResult.Challenge;
            if (challenge.LockedUntilUtc.HasValue && challenge.LockedUntilUtc.Value > nowUtc)
                return new ChangePasswordVerifyResult(false, GenericVerificationFailure);

            var code = (otpCode ?? string.Empty).Trim();
            if (code.Length != 6 || !code.All(char.IsDigit))
                return new ChangePasswordVerifyResult(false, GenericVerificationFailure);

            var expectedHash = ComputeChangePasswordOtpHash(userId, challenge.CorrelationId ?? string.Empty, code);
            if (!cryptoService.FixedTimeEquals(challenge.CodeHash, expectedHash))
            {
                await userOtpRepository.MarkAttemptFailureAsync(challenge.UserOtpId, nowUtc, options.Value.OtpMaxAttempts, options.Value.OtpLockMinutes, ct);
                logger.LogWarning(
                    "Audit PasswordChangeOtpFailed UserId={UserId} CorrelationId={CorrelationId} RequestedFromIp={RequestedFromIp} UserAgent={UserAgent} AuditCorrelationId={AuditCorrelationId} AtUtc={AtUtc}",
                    userId,
                    challenge.CorrelationId,
                    Normalize(requestedFromIp, 45),
                    Normalize(requestedUserAgent, 512),
                    Normalize(auditCorrelationId, 64),
                    nowUtc);
                return new ChangePasswordVerifyResult(false, GenericVerificationFailure);
            }

            var password = newPassword ?? string.Empty;
            var confirm = confirmPassword ?? string.Empty;
            var minLength = Math.Max(8, options.Value.PasswordMinLength);
            if (password.Length < minLength)
                return new ChangePasswordVerifyResult(false, $"Password must be at least {minLength} characters.");
            if (!string.Equals(password, confirm, StringComparison.Ordinal))
                return new ChangePasswordVerifyResult(false, "Password and confirmation do not match.");

            var markedUsed = await userOtpRepository.MarkUsedAsync(challenge.UserOtpId, nowUtc, ct);
            if (!markedUsed)
                return new ChangePasswordVerifyResult(false, GenericVerificationFailure);

            var passwordHash = passwordHasherService.HashPassword(password);
            var updated = await userRepository.UpdatePasswordAndBumpSessionVersionAsync(
                userId,
                passwordHash,
                passwordHasherService.PasswordHashVersion,
                nowUtc,
                ct);
            if (!updated)
                return new ChangePasswordVerifyResult(false, "Password change failed. Please try again.");

            await userOtpRepository.RevokeActiveByUserPurposeAsync(userId, UserOtpPurpose.ChangePassword, nowUtc, ct);
            logger.LogInformation(
                "Audit PasswordChanged UserId={UserId} RequestedFromIp={RequestedFromIp} UserAgent={UserAgent} AuditCorrelationId={AuditCorrelationId} AtUtc={AtUtc}",
                userId,
                Normalize(requestedFromIp, 45),
                Normalize(requestedUserAgent, 512),
                Normalize(auditCorrelationId, 64),
                nowUtc);

            return new ChangePasswordVerifyResult(true, "Password changed successfully.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Change password verification failed for UserId={UserId}", userId);
            return new ChangePasswordVerifyResult(false, "Unable to change password right now.");
        }
    }

    private async Task<RateLimitDecision> CheckOtpRateLimitAsync(int userId, string? requestedFromIp, DateTime nowUtc, CancellationToken ct)
    {
        var cfg = options.Value;
        var latestSentAt = await userOtpRepository.GetLatestSentAtUtcAsync(userId, UserOtpPurpose.ChangePassword, ct);
        if (latestSentAt.HasValue && latestSentAt.Value > nowUtc.AddSeconds(-Math.Max(1, cfg.OtpCooldownSeconds)))
            return new RateLimitDecision(false, "cooldown");

        var sentThisHour = await userOtpRepository.CountSentSinceAsync(userId, UserOtpPurpose.ChangePassword, nowUtc.AddHours(-1), ct);
        if (sentThisHour >= Math.Max(1, cfg.OtpMaxPerHourPerUser))
            return new RateLimitDecision(false, "max_per_hour_user");

        var normalizedIp = Normalize(requestedFromIp, 45);
        if (!string.IsNullOrWhiteSpace(normalizedIp))
        {
            var sentForIp = await userOtpRepository.CountSentSinceForIpAsync(normalizedIp, UserOtpPurpose.ChangePassword, nowUtc.AddHours(-1), ct);
            if (sentForIp >= Math.Max(1, cfg.OtpMaxPerHourPerIp))
                return new RateLimitDecision(false, "max_per_hour_ip");
        }

        return new RateLimitDecision(true, null);
    }

    private byte[] ComputeChangePasswordOtpHash(int userId, string correlationId, string otpCode)
    {
        var payload = Encoding.UTF8.GetBytes($"{userId}:{UserOtpPurpose.ChangePassword}:{correlationId.Trim()}:{otpCode.Trim()}");
        return cryptoService.ComputeHmacSha256(payload);
    }

    private static string? Normalize(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
