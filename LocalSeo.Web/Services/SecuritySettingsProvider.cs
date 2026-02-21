using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record PasswordPolicyRules(
    int MinimumLength,
    bool RequiresNumber,
    bool RequiresCapitalLetter,
    bool RequiresSpecialCharacter);

public sealed record SecuritySettingsSnapshot(
    PasswordPolicyRules PasswordPolicy,
    int LoginLockoutThreshold,
    int LoginLockoutMinutes,
    int EmailCodeCooldownSeconds,
    int EmailCodeMaxPerHourPerEmail,
    int EmailCodeMaxPerHourPerIp,
    int EmailCodeExpiryMinutes,
    int EmailCodeMaxFailedAttemptsPerCode,
    int InviteExpiryHours,
    int InviteOtpExpiryMinutes,
    int InviteOtpCooldownSeconds,
    int InviteOtpMaxPerHourPerInvite,
    int InviteOtpMaxPerHourPerIp,
    int InviteOtpMaxAttempts,
    int InviteOtpLockMinutes,
    int InviteMaxAttempts,
    int InviteLockMinutes,
    int ChangePasswordOtpExpiryMinutes,
    int ChangePasswordOtpCooldownSeconds,
    int ChangePasswordOtpMaxPerHourPerUser,
    int ChangePasswordOtpMaxPerHourPerIp,
    int ChangePasswordOtpMaxAttempts,
    int ChangePasswordOtpLockMinutes);

public interface ISecuritySettingsProvider
{
    Task<SecuritySettingsSnapshot> GetAsync(CancellationToken ct);
}

public sealed class SecuritySettingsProvider(
    IAdminSettingsService adminSettingsService,
    IOptions<AuthOptions> authOptions,
    IOptions<EmailCodesOptions> emailCodesOptions,
    IOptions<InviteOptions> inviteOptions,
    IOptions<ChangePasswordOptions> changePasswordOptions,
    ILogger<SecuritySettingsProvider> logger) : ISecuritySettingsProvider
{
    public async Task<SecuritySettingsSnapshot> GetAsync(CancellationToken ct)
    {
        try
        {
            var settings = await adminSettingsService.GetAsync(ct);
            return MapFromAdminSettings(settings);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to read security settings from AppSettings. Falling back to configured options.");
            return MapFallbackFromOptions();
        }
    }

    private SecuritySettingsSnapshot MapFromAdminSettings(AdminSettingsModel settings)
    {
        return new SecuritySettingsSnapshot(
            PasswordPolicy: new PasswordPolicyRules(
                MinimumLength: Math.Clamp(settings.MinimumPasswordLength, 8, 128),
                RequiresNumber: settings.PasswordRequiresNumber,
                RequiresCapitalLetter: settings.PasswordRequiresCapitalLetter,
                RequiresSpecialCharacter: settings.PasswordRequiresSpecialCharacter),
            LoginLockoutThreshold: Math.Clamp(settings.LoginLockoutThreshold, 1, 1000),
            LoginLockoutMinutes: Math.Clamp(settings.LoginLockoutMinutes, 1, 7 * 24 * 60),
            EmailCodeCooldownSeconds: Math.Clamp(settings.EmailCodeCooldownSeconds, 1, 24 * 60 * 60),
            EmailCodeMaxPerHourPerEmail: Math.Clamp(settings.EmailCodeMaxPerHourPerEmail, 1, 1000),
            EmailCodeMaxPerHourPerIp: Math.Clamp(settings.EmailCodeMaxPerHourPerIp, 1, 5000),
            EmailCodeExpiryMinutes: Math.Clamp(settings.EmailCodeExpiryMinutes, 1, 24 * 60),
            EmailCodeMaxFailedAttemptsPerCode: Math.Clamp(settings.EmailCodeMaxFailedAttemptsPerCode, 1, 100),
            InviteExpiryHours: Math.Clamp(settings.InviteExpiryHours, 1, 24 * 30),
            InviteOtpExpiryMinutes: Math.Clamp(settings.InviteOtpExpiryMinutes, 1, 24 * 60),
            InviteOtpCooldownSeconds: Math.Clamp(settings.InviteOtpCooldownSeconds, 1, 24 * 60 * 60),
            InviteOtpMaxPerHourPerInvite: Math.Clamp(settings.InviteOtpMaxPerHourPerInvite, 1, 1000),
            InviteOtpMaxPerHourPerIp: Math.Clamp(settings.InviteOtpMaxPerHourPerIp, 1, 5000),
            InviteOtpMaxAttempts: Math.Clamp(settings.InviteOtpMaxAttempts, 1, 100),
            InviteOtpLockMinutes: Math.Clamp(settings.InviteOtpLockMinutes, 1, 7 * 24 * 60),
            InviteMaxAttempts: Math.Clamp(settings.InviteMaxAttempts, 1, 1000),
            InviteLockMinutes: Math.Clamp(settings.InviteLockMinutes, 1, 7 * 24 * 60),
            ChangePasswordOtpExpiryMinutes: Math.Clamp(settings.ChangePasswordOtpExpiryMinutes, 1, 24 * 60),
            ChangePasswordOtpCooldownSeconds: Math.Clamp(settings.ChangePasswordOtpCooldownSeconds, 1, 24 * 60 * 60),
            ChangePasswordOtpMaxPerHourPerUser: Math.Clamp(settings.ChangePasswordOtpMaxPerHourPerUser, 1, 1000),
            ChangePasswordOtpMaxPerHourPerIp: Math.Clamp(settings.ChangePasswordOtpMaxPerHourPerIp, 1, 5000),
            ChangePasswordOtpMaxAttempts: Math.Clamp(settings.ChangePasswordOtpMaxAttempts, 1, 100),
            ChangePasswordOtpLockMinutes: Math.Clamp(settings.ChangePasswordOtpLockMinutes, 1, 7 * 24 * 60));
    }

    private SecuritySettingsSnapshot MapFallbackFromOptions()
    {
        var auth = authOptions.Value;
        var email = emailCodesOptions.Value;
        var invite = inviteOptions.Value;
        var changePassword = changePasswordOptions.Value;

        return new SecuritySettingsSnapshot(
            PasswordPolicy: new PasswordPolicyRules(
                MinimumLength: Math.Clamp(Math.Max(invite.PasswordMinLength, changePassword.PasswordMinLength), 8, 128),
                RequiresNumber: true,
                RequiresCapitalLetter: true,
                RequiresSpecialCharacter: true),
            LoginLockoutThreshold: Math.Clamp(auth.LockoutThreshold, 1, 1000),
            LoginLockoutMinutes: Math.Clamp(auth.LockoutMinutes, 1, 7 * 24 * 60),
            EmailCodeCooldownSeconds: Math.Clamp(email.CooldownSeconds, 1, 24 * 60 * 60),
            EmailCodeMaxPerHourPerEmail: Math.Clamp(email.MaxPerHourPerEmail, 1, 1000),
            EmailCodeMaxPerHourPerIp: Math.Clamp(email.MaxPerHourPerIp, 1, 5000),
            EmailCodeExpiryMinutes: Math.Clamp(email.ExpiryMinutes, 1, 24 * 60),
            EmailCodeMaxFailedAttemptsPerCode: 5,
            InviteExpiryHours: Math.Clamp(invite.InviteExpiryHours, 1, 24 * 30),
            InviteOtpExpiryMinutes: Math.Clamp(invite.OtpExpiryMinutes, 1, 24 * 60),
            InviteOtpCooldownSeconds: Math.Clamp(invite.OtpCooldownSeconds, 1, 24 * 60 * 60),
            InviteOtpMaxPerHourPerInvite: Math.Clamp(invite.OtpMaxPerHourPerInvite, 1, 1000),
            InviteOtpMaxPerHourPerIp: Math.Clamp(invite.OtpMaxPerHourPerIp, 1, 5000),
            InviteOtpMaxAttempts: Math.Clamp(invite.OtpMaxAttempts, 1, 100),
            InviteOtpLockMinutes: Math.Clamp(invite.OtpLockMinutes, 1, 7 * 24 * 60),
            InviteMaxAttempts: Math.Clamp(invite.InviteMaxAttempts, 1, 1000),
            InviteLockMinutes: Math.Clamp(invite.InviteLockMinutes, 1, 7 * 24 * 60),
            ChangePasswordOtpExpiryMinutes: Math.Clamp(changePassword.OtpExpiryMinutes, 1, 24 * 60),
            ChangePasswordOtpCooldownSeconds: Math.Clamp(changePassword.OtpCooldownSeconds, 1, 24 * 60 * 60),
            ChangePasswordOtpMaxPerHourPerUser: Math.Clamp(changePassword.OtpMaxPerHourPerUser, 1, 1000),
            ChangePasswordOtpMaxPerHourPerIp: Math.Clamp(changePassword.OtpMaxPerHourPerIp, 1, 5000),
            ChangePasswordOtpMaxAttempts: Math.Clamp(changePassword.OtpMaxAttempts, 1, 100),
            ChangePasswordOtpLockMinutes: Math.Clamp(changePassword.OtpLockMinutes, 1, 7 * 24 * 60));
    }
}
