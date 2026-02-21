namespace LocalSeo.Web.Options;

public sealed class InviteOptions
{
    public string HmacSecret { get; set; } = "change-this-invite-hmac-secret";
    public int InviteExpiryHours { get; set; } = 24;
    public int OtpExpiryMinutes { get; set; } = 10;
    public int OtpCooldownSeconds { get; set; } = 60;
    public int OtpMaxPerHourPerInvite { get; set; } = 3;
    public int OtpMaxPerHourPerIp { get; set; } = 25;
    public int OtpMaxAttempts { get; set; } = 5;
    public int OtpLockMinutes { get; set; } = 15;
    public int InviteMaxAttempts { get; set; } = 10;
    public int InviteLockMinutes { get; set; } = 15;
    public int PasswordMinLength { get; set; } = 12;
}
