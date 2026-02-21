namespace LocalSeo.Web.Models;

public enum EmailCodePurpose : byte
{
    Login2Fa = 1,
    ForgotPassword = 2
}

public enum UserStatusFilter
{
    Active,
    Pending,
    Disabled,
    All
}

public enum UserLifecycleStatus : byte
{
    Pending = 0,
    Active = 1,
    Disabled = 2
}

public enum UserInviteStatus : byte
{
    Active = 1,
    Used = 2,
    Expired = 3,
    Revoked = 4
}

public sealed class LoginRequestModel
{
    public string Email { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

public sealed class TwoFactorRequestModel
{
    public int Rid { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
}

public sealed class ForgotPasswordRequestModel
{
    public string Email { get; set; } = string.Empty;
}

public sealed class ResetPasswordRequestModel
{
    public int Rid { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
    public string NewPassword { get; set; } = string.Empty;
    public string ConfirmPassword { get; set; } = string.Empty;
}

public sealed record UserRecord(
    int Id,
    string FirstName,
    string LastName,
    string EmailAddress,
    string EmailAddressNormalized,
    byte[]? PasswordHash,
    byte PasswordHashVersion,
    bool IsActive,
    bool IsAdmin,
    DateTime DateCreatedAtUtc,
    DateTime? DatePasswordLastSetUtc,
    DateTime? LastLoginAtUtc,
    int FailedPasswordAttempts,
    DateTime? LockedoutUntilUtc,
    UserLifecycleStatus InviteStatus,
    bool UseGravatar);

public sealed record EmailCodeRecord(
    int EmailCodeId,
    EmailCodePurpose Purpose,
    string Email,
    string EmailNormalized,
    byte[] CodeHash,
    byte[] Salt,
    DateTime ExpiresAtUtc,
    DateTime CreatedAtUtc,
    int FailedAttempts,
    bool IsUsed);

public sealed record AdminUserListRow(
    int Id,
    string Name,
    string EmailAddress,
    DateTime DateCreatedAtUtc,
    bool IsActive,
    UserLifecycleStatus InviteStatus,
    DateTime? LastInviteCreatedAtUtc,
    bool HasActiveInvite);

public sealed class AdminUsersListViewModel
{
    public string Filter { get; init; } = "active";
    public string SearchTerm { get; init; } = string.Empty;
    public int? CurrentUserId { get; init; }
    public IReadOnlyList<AdminUserListRow> Rows { get; init; } = [];
}

public sealed class AdminCreateUserRequestModel
{
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string EmailAddress { get; set; } = string.Empty;
}

public sealed class AdminCreateUserViewModel
{
    public string Filter { get; init; } = "active";
    public string SearchTerm { get; init; } = string.Empty;
    public AdminCreateUserRequestModel CreateUser { get; init; } = new();
}

public sealed class AdminEditUserRequestModel
{
    public int Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string EmailAddress { get; set; } = string.Empty;
    public bool IsAdmin { get; set; }
    public UserLifecycleStatus InviteStatus { get; set; } = UserLifecycleStatus.Active;
}

public sealed class AdminEditUserViewModel
{
    public string Filter { get; init; } = "active";
    public string SearchTerm { get; init; } = string.Empty;
    public required AdminEditUserRequestModel User { get; init; }
}

public sealed class AdminUserDetailsViewModel
{
    public required UserRecord User { get; init; }
    public string Filter { get; init; } = "active";
    public string SearchTerm { get; init; } = string.Empty;
    public int? CurrentUserId { get; init; }
    public DateTime? LastInviteCreatedAtUtc { get; init; }
    public bool HasActiveInvite { get; init; }
}

public class InviteTokenFormModel
{
    public string Token { get; set; } = string.Empty;
}

public sealed class InviteVerifyOtpFormModel : InviteTokenFormModel
{
    public string Code { get; set; } = string.Empty;
}

public sealed class InviteSetPasswordFormModel : InviteTokenFormModel
{
    public string NewPassword { get; set; } = string.Empty;
    public string ConfirmPassword { get; set; } = string.Empty;
    public bool UseGravatar { get; set; }
}

public sealed class InviteAcceptViewModel
{
    public string Token { get; init; } = string.Empty;
    public string EmailAddressMasked { get; init; } = string.Empty;
    public bool CanSendOtp { get; init; }
    public bool OtpSent { get; init; }
    public bool OtpVerified { get; init; }
    public string? Message { get; init; }
}

public sealed class InviteSetPasswordViewModel
{
    public string Token { get; init; } = string.Empty;
    public string EmailAddressMasked { get; init; } = string.Empty;
    public string? Message { get; init; }
    public bool UseGravatar { get; init; }
}

public sealed record UserInviteRecord(
    long UserInviteId,
    int UserId,
    string FirstName,
    string LastName,
    string EmailAddress,
    string EmailNormalized,
    byte[] TokenHash,
    DateTime ExpiresAtUtc,
    DateTime? UsedAtUtc,
    DateTime CreatedAtUtc,
    int? CreatedByUserId,
    DateTime? ResentAtUtc,
    UserInviteStatus Status,
    int AttemptCount,
    DateTime? LastAttemptAtUtc,
    DateTime? LockedUntilUtc,
    DateTime? OtpVerifiedAtUtc,
    DateTime? LastOtpSentAtUtc);

public sealed record InviteOtpRecord(
    long InviteOtpId,
    long UserInviteId,
    byte[] CodeHash,
    DateTime ExpiresAtUtc,
    DateTime SentAtUtc,
    int AttemptCount,
    DateTime? LockedUntilUtc,
    DateTime? UsedAtUtc);
