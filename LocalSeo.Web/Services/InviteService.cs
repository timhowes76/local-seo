using System.Security.Cryptography;
using System.Text;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record AdminInviteResult(bool Success, string Message, int? UserId);
public sealed record InviteTokenValidationResult(bool Success, string Message, UserInviteRecord? Invite);
public sealed record InviteOtpResult(bool Success, string Message, bool OtpSent, bool OtpVerified, UserInviteRecord? Invite);
public sealed record InviteSetPasswordResult(bool Success, string Message);

public interface IInviteService
{
    Task<AdminInviteResult> CreateUserAndInviteAsync(string firstName, string lastName, string emailAddress, int? createdByUserId, string appBaseUrl, string? requestedFromIp, CancellationToken ct);
    Task<AdminInviteResult> ResendInviteAsync(int userId, int? createdByUserId, string appBaseUrl, string? requestedFromIp, CancellationToken ct);
    Task<InviteTokenValidationResult> ValidateInviteTokenAsync(string? token, CancellationToken ct);
    Task<InviteOtpResult> SendOtpAsync(string? token, string? requestedFromIp, CancellationToken ct);
    Task<InviteOtpResult> VerifyOtpAsync(string? token, string? code, string? requestedFromIp, CancellationToken ct);
    Task<InviteSetPasswordResult> SetPasswordAsync(string? token, string? newPassword, string? confirmPassword, bool useGravatar, CancellationToken ct);
    string MaskEmailAddress(string? emailAddress);
}

public sealed class InviteService(
    IUserRepository userRepository,
    IUserInviteRepository userInviteRepository,
    IEmailAddressNormalizer emailAddressNormalizer,
    IPasswordHasherService passwordHasherService,
    ICryptoService cryptoService,
    ISendGridEmailService sendGridEmailService,
    ISecuritySettingsProvider securitySettingsProvider,
    TimeProvider timeProvider,
    ILogger<InviteService> logger) : IInviteService
{
    private const int InviteTokenByteLength = 32;
    private const string GenericInvalidLinkMessage = "This link is invalid or expired.";
    private const string GenericOtpFailureMessage = "Verification failed. Please try again.";

    public async Task<AdminInviteResult> CreateUserAndInviteAsync(string firstName, string lastName, string emailAddress, int? createdByUserId, string appBaseUrl, string? requestedFromIp, CancellationToken ct)
    {
        try
        {
            var normalizedFirstName = NormalizeName(firstName, 100);
            var normalizedLastName = NormalizeName(lastName, 100);
            var normalizedEmail = emailAddressNormalizer.Normalize(emailAddress);
            if (normalizedFirstName is null || normalizedLastName is null || normalizedEmail.Length == 0)
                return new AdminInviteResult(false, "First name, last name, and a valid email are required.", null);

            var existing = await userRepository.GetByNormalizedEmailAsync(normalizedEmail, ct);
            if (existing is not null)
                return new AdminInviteResult(false, "A user with that email already exists.", existing.Id);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            var userId = await userInviteRepository.CreatePendingUserAsync(
                normalizedFirstName,
                normalizedLastName,
                normalizedEmail,
                normalizedEmail,
                nowUtc,
                ct);

            var inviteCreated = await CreateAndSendInviteAsync(userId, normalizedEmail, createdByUserId, appBaseUrl, requestedFromIp, nowUtc, ct);
            if (!inviteCreated)
                return new AdminInviteResult(false, "User created but invite email failed to send. Check email settings/logs, then use resend invite.", userId);

            logger.LogInformation(
                "Audit InviteCreated UserId={UserId} CreatedByUserId={CreatedByUserId} RequestedFromIp={RequestedFromIp} AtUtc={AtUtc}",
                userId,
                createdByUserId,
                requestedFromIp,
                nowUtc);

            return new AdminInviteResult(true, "User created and invite sent.", userId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create user invite. CreatedByUserId={CreatedByUserId}", createdByUserId);
            return new AdminInviteResult(false, "Unable to create user invite at this time.", null);
        }
    }

    public async Task<AdminInviteResult> ResendInviteAsync(int userId, int? createdByUserId, string appBaseUrl, string? requestedFromIp, CancellationToken ct)
    {
        try
        {
            var user = await userRepository.GetByIdAsync(userId, ct);
            if (user is null)
                return new AdminInviteResult(false, "User was not found.", null);

            if (user.InviteStatus != UserLifecycleStatus.Pending)
                return new AdminInviteResult(false, "Only pending users can be resent an invite.", user.Id);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            await userInviteRepository.RevokeActiveInvitesForUserAsync(user.Id, nowUtc, ct);

            var sent = await CreateAndSendInviteAsync(user.Id, user.EmailAddressNormalized, createdByUserId, appBaseUrl, requestedFromIp, nowUtc, ct);
            if (!sent)
                return new AdminInviteResult(false, "Invite email failed to send. Check email settings/logs and try again.", user.Id);

            logger.LogInformation(
                "Audit InviteResent UserId={UserId} CreatedByUserId={CreatedByUserId} RequestedFromIp={RequestedFromIp} AtUtc={AtUtc}",
                user.Id,
                createdByUserId,
                requestedFromIp,
                nowUtc);

            return new AdminInviteResult(true, "Invite resent.", user.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to resend invite. UserId={UserId} CreatedByUserId={CreatedByUserId}", userId, createdByUserId);
            return new AdminInviteResult(false, "Invite could not be resent at this time.", userId);
        }
    }

    public async Task<InviteTokenValidationResult> ValidateInviteTokenAsync(string? token, CancellationToken ct)
    {
        try
        {
            if (!TryComputeTokenHash(token, out var tokenHash))
                return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);

            var invite = await userInviteRepository.GetInviteByTokenHashAsync(tokenHash, ct);
            if (invite is null)
                return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            if (invite.LockedUntilUtc.HasValue && invite.LockedUntilUtc.Value > nowUtc)
                return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);

            if (invite.Status != UserInviteStatus.Active || invite.UsedAtUtc.HasValue)
                return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);

            if (invite.ExpiresAtUtc < nowUtc)
            {
                await userInviteRepository.MarkInviteExpiredAsync(invite.UserInviteId, nowUtc, ct);
                return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);
            }

            return new InviteTokenValidationResult(true, string.Empty, invite);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Invite token validation failed due to internal error.");
            return new InviteTokenValidationResult(false, GenericInvalidLinkMessage, null);
        }
    }

    public async Task<InviteOtpResult> SendOtpAsync(string? token, string? requestedFromIp, CancellationToken ct)
    {
        try
        {
            var inviteResult = await ValidateInviteTokenAsync(token, ct);
            if (!inviteResult.Success || inviteResult.Invite is null)
                return new InviteOtpResult(false, GenericInvalidLinkMessage, false, false, null);

            var invite = inviteResult.Invite;
            if (invite.OtpVerifiedAtUtc.HasValue)
                return new InviteOtpResult(true, "Email already verified. You can continue to set your password.", false, true, invite);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            var cfg = await securitySettingsProvider.GetAsync(ct);
            var latestOtpSentAtUtc = await userInviteRepository.GetLatestOtpSentAtUtcAsync(invite.UserInviteId, ct);
            if (latestOtpSentAtUtc.HasValue && latestOtpSentAtUtc.Value > nowUtc.AddSeconds(-Math.Max(1, cfg.InviteOtpCooldownSeconds)))
            {
                return new InviteOtpResult(false, "Please wait before requesting another code.", false, false, invite);
            }

            var sendsThisHour = await userInviteRepository.CountOtpSentSinceAsync(invite.UserInviteId, nowUtc.AddHours(-1), ct);
            if (sendsThisHour >= Math.Max(1, cfg.InviteOtpMaxPerHourPerInvite))
            {
                return new InviteOtpResult(false, "Verification is temporarily unavailable. Please contact an administrator.", false, false, invite);
            }

            if (!string.IsNullOrWhiteSpace(requestedFromIp))
            {
                var ipCountThisHour = await userInviteRepository.CountOtpSentSinceForIpAsync(requestedFromIp, nowUtc.AddHours(-1), ct);
                if (ipCountThisHour >= Math.Max(1, cfg.InviteOtpMaxPerHourPerIp))
                {
                    return new InviteOtpResult(false, "Verification is temporarily unavailable. Please contact an administrator.", false, false, invite);
                }
            }

            var otpCode = RandomNumberGenerator.GetInt32(0, 1_000_000).ToString("D6");
            var otpHash = ComputeOtpHash(invite.UserInviteId, otpCode);
            var expiresAtUtc = nowUtc.AddMinutes(Math.Max(1, cfg.InviteOtpExpiryMinutes));
            await userInviteRepository.CreateInviteOtpAsync(new InviteOtpCreateRequest(
                invite.UserInviteId,
                otpHash,
                expiresAtUtc,
                nowUtc,
                requestedFromIp), ct);

            await sendGridEmailService.SendInviteOtpAsync(invite.EmailAddress, otpCode, expiresAtUtc, ct);

            logger.LogInformation(
                "Audit InviteOtpSent UserId={UserId} UserInviteId={UserInviteId} RequestedFromIp={RequestedFromIp} AtUtc={AtUtc}",
                invite.UserId,
                invite.UserInviteId,
                requestedFromIp,
                nowUtc);

            var refreshedInvite = await userInviteRepository.GetInviteByTokenHashAsync(invite.TokenHash, ct) ?? invite;
            return new InviteOtpResult(true, "A verification code has been emailed to you.", true, false, refreshedInvite);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send invite OTP.");
            return new InviteOtpResult(false, "Verification is temporarily unavailable. Please contact an administrator.", false, false, null);
        }
    }

    public async Task<InviteOtpResult> VerifyOtpAsync(string? token, string? code, string? requestedFromIp, CancellationToken ct)
    {
        try
        {
            var inviteResult = await ValidateInviteTokenAsync(token, ct);
            if (!inviteResult.Success || inviteResult.Invite is null)
                return new InviteOtpResult(false, GenericInvalidLinkMessage, false, false, null);

            var invite = inviteResult.Invite;
            if (invite.OtpVerifiedAtUtc.HasValue)
                return new InviteOtpResult(true, "Verification successful.", false, true, invite);

            var otpCode = (code ?? string.Empty).Trim();
            if (otpCode.Length == 0)
                return new InviteOtpResult(false, GenericOtpFailureMessage, false, false, invite);

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            var cfg = await securitySettingsProvider.GetAsync(ct);
            var otp = await userInviteRepository.GetLatestInviteOtpAsync(invite.UserInviteId, nowUtc, ct);
            if (otp is null)
                return new InviteOtpResult(false, GenericOtpFailureMessage, false, false, invite);

            if (otp.LockedUntilUtc.HasValue && otp.LockedUntilUtc.Value > nowUtc)
                return new InviteOtpResult(false, "Too many attempts. Please wait and try again.", false, false, invite);

            var expectedHash = ComputeOtpHash(invite.UserInviteId, otpCode);
            if (!cryptoService.FixedTimeEquals(otp.CodeHash, expectedHash))
            {
                await userInviteRepository.MarkInviteOtpAttemptFailureAsync(otp.InviteOtpId, nowUtc, cfg.InviteOtpMaxAttempts, cfg.InviteOtpLockMinutes, ct);
                await userInviteRepository.MarkInviteAttemptFailureAsync(invite.UserInviteId, nowUtc, cfg.InviteMaxAttempts, cfg.InviteLockMinutes, ct);
                logger.LogWarning(
                    "Audit InviteOtpFailed UserId={UserId} UserInviteId={UserInviteId} RequestedFromIp={RequestedFromIp} AtUtc={AtUtc}",
                    invite.UserId,
                    invite.UserInviteId,
                    requestedFromIp,
                    nowUtc);
                return new InviteOtpResult(false, GenericOtpFailureMessage, false, false, invite);
            }

            var markedOtpUsed = await userInviteRepository.MarkInviteOtpUsedAsync(otp.InviteOtpId, nowUtc, ct);
            if (!markedOtpUsed)
                return new InviteOtpResult(false, GenericOtpFailureMessage, false, false, invite);

            await userInviteRepository.MarkInviteOtpVerifiedAsync(invite.UserInviteId, nowUtc, ct);

            logger.LogInformation(
                "Audit InviteOtpVerified UserId={UserId} UserInviteId={UserInviteId} RequestedFromIp={RequestedFromIp} AtUtc={AtUtc}",
                invite.UserId,
                invite.UserInviteId,
                requestedFromIp,
                nowUtc);

            var refreshedInvite = await userInviteRepository.GetInviteByTokenHashAsync(invite.TokenHash, ct) ?? invite;
            return new InviteOtpResult(true, "Verification successful.", false, true, refreshedInvite);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to verify invite OTP.");
            return new InviteOtpResult(false, GenericOtpFailureMessage, false, false, null);
        }
    }

    public async Task<InviteSetPasswordResult> SetPasswordAsync(string? token, string? newPassword, string? confirmPassword, bool useGravatar, CancellationToken ct)
    {
        try
        {
            var inviteResult = await ValidateInviteTokenAsync(token, ct);
            if (!inviteResult.Success || inviteResult.Invite is null)
                return new InviteSetPasswordResult(false, GenericInvalidLinkMessage);

            var invite = inviteResult.Invite;
            if (!invite.OtpVerifiedAtUtc.HasValue)
                return new InviteSetPasswordResult(false, "Please verify your email first.");

            var password = newPassword ?? string.Empty;
            var confirm = confirmPassword ?? string.Empty;
            var settings = await securitySettingsProvider.GetAsync(ct);
            var passwordValidation = PasswordPolicyEvaluator.Validate(password, settings.PasswordPolicy);
            if (!passwordValidation.IsValid)
                return new InviteSetPasswordResult(false, PasswordPolicyEvaluator.BuildGuidanceMessage(passwordValidation));
            if (!string.Equals(password, confirm, StringComparison.Ordinal))
                return new InviteSetPasswordResult(false, "Password and confirmation do not match.");

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            var passwordHash = passwordHasherService.HashPassword(password);
            var completed = await userInviteRepository.CompleteInviteAsync(
                invite.UserInviteId,
                invite.UserId,
                passwordHash,
                passwordHasherService.PasswordHashVersion,
                useGravatar,
                nowUtc,
                ct);

            if (!completed)
                return new InviteSetPasswordResult(false, GenericInvalidLinkMessage);

            logger.LogInformation(
                "Audit InvitePasswordSet UserId={UserId} UserInviteId={UserInviteId} AtUtc={AtUtc}",
                invite.UserId,
                invite.UserInviteId,
                nowUtc);

            return new InviteSetPasswordResult(true, "Password set successfully. You can now sign in.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to set password for invite.");
            return new InviteSetPasswordResult(false, GenericInvalidLinkMessage);
        }
    }

    public string MaskEmailAddress(string? emailAddress)
    {
        var email = (emailAddress ?? string.Empty).Trim();
        var atIndex = email.IndexOf('@', StringComparison.Ordinal);
        if (atIndex <= 1 || atIndex >= email.Length - 1)
            return "your email address";

        var local = email[..atIndex];
        var domain = email[(atIndex + 1)..];
        var visibleLocal = local.Length <= 2 ? local[..1] : local[..2];
        return $"{visibleLocal}***@{domain}";
    }

    private async Task<bool> CreateAndSendInviteAsync(int userId, string emailNormalized, int? createdByUserId, string appBaseUrl, string? requestedFromIp, DateTime nowUtc, CancellationToken ct)
    {
        try
        {
            var user = await userRepository.GetByIdAsync(userId, ct);
            if (user is null)
                return false;

            var rawToken = cryptoService.GenerateRandomBytes(InviteTokenByteLength);
            var tokenHash = cryptoService.ComputeHmacSha256(rawToken);
            var token = cryptoService.Base64UrlEncode(rawToken);
            var settings = await securitySettingsProvider.GetAsync(ct);
            var expiresAtUtc = nowUtc.AddHours(Math.Max(1, settings.InviteExpiryHours));

            await userInviteRepository.CreateInviteAsync(new UserInviteCreateRequest(
                user.Id,
                emailNormalized,
                tokenHash,
                expiresAtUtc,
                createdByUserId,
                nowUtc), ct);

            var baseUrl = NormalizeBaseUrl(appBaseUrl);
            var inviteUrl = $"{baseUrl}/invite/accept?token={Uri.EscapeDataString(token)}";
            await sendGridEmailService.SendUserInviteAsync(user.EmailAddress, $"{user.FirstName} {user.LastName}".Trim(), inviteUrl, expiresAtUtc, ct);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Failed to create/send invite email. UserId={UserId} RequestedFromIp={RequestedFromIp}",
                userId,
                requestedFromIp);
            return false;
        }
    }

    private bool TryComputeTokenHash(string? token, out byte[] tokenHash)
    {
        tokenHash = [];
        if (!cryptoService.TryBase64UrlDecode(token, out var tokenBytes))
            return false;
        if (tokenBytes.Length != InviteTokenByteLength)
            return false;

        tokenHash = cryptoService.ComputeHmacSha256(tokenBytes);
        return true;
    }

    private byte[] ComputeOtpHash(long userInviteId, string otpCode)
    {
        var payload = Encoding.UTF8.GetBytes($"{userInviteId}:{otpCode.Trim()}");
        return cryptoService.ComputeHmacSha256(payload);
    }

    private static string NormalizeBaseUrl(string appBaseUrl)
    {
        var normalized = (appBaseUrl ?? string.Empty).Trim().TrimEnd('/');
        return normalized.Length == 0 ? "https://localhost" : normalized;
    }

    private static string? NormalizeName(string value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }
}
