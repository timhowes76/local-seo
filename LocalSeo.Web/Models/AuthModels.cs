namespace LocalSeo.Web.Models;

public enum EmailCodePurpose : byte
{
    Login2Fa = 1,
    ForgotPassword = 2
}

public enum UserStatusFilter
{
    Active,
    Inactive,
    All
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
    DateTime? LockedoutUntilUtc);

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
    bool IsActive);

public sealed class AdminUsersListViewModel
{
    public string Filter { get; init; } = "active";
    public IReadOnlyList<AdminUserListRow> Rows { get; init; } = [];
}

public sealed class AdminUserDetailsViewModel
{
    public required UserRecord User { get; init; }
    public string Filter { get; init; } = "active";
}
