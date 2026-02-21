namespace LocalSeo.Web.Models;

public static class UserOtpPurpose
{
    public const string ChangePassword = "ChangePassword";
}

public sealed record UserOtpRecord(
    long UserOtpId,
    int UserId,
    string Purpose,
    byte[] CodeHash,
    DateTime ExpiresAtUtc,
    DateTime? UsedAtUtc,
    DateTime SentAtUtc,
    int AttemptCount,
    DateTime? LockedUntilUtc,
    string? CorrelationId,
    string? RequestedFromIp);

public sealed class ChangePasswordStartRequestModel
{
    public string CurrentPassword { get; set; } = string.Empty;
}

public sealed class ChangePasswordVerifyRequestModel
{
    public string CorrelationId { get; set; } = string.Empty;
    public string OtpCode { get; set; } = string.Empty;
    public string NewPassword { get; set; } = string.Empty;
    public string ConfirmNewPassword { get; set; } = string.Empty;
}

public sealed class ChangePasswordResendRequestModel
{
    public string CorrelationId { get; set; } = string.Empty;
}

public sealed class ChangePasswordStartViewModel
{
    public string? Message { get; init; }
    public ChangePasswordStartRequestModel Form { get; init; } = new();
}

public sealed class ChangePasswordVerifyViewModel
{
    public string? Message { get; init; }
    public bool IsInvalidOrExpired { get; init; }
    public string CorrelationId { get; init; } = string.Empty;
    public DateTime? ExpiresAtUtc { get; init; }
    public DateTime? LockedUntilUtc { get; init; }
    public ChangePasswordVerifyRequestModel Form { get; init; } = new();
    public PasswordPolicyViewModel PasswordPolicy { get; init; } = new();
}
