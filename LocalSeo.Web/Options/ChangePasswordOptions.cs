namespace LocalSeo.Web.Options;

public sealed class ChangePasswordOptions
{
    public int OtpExpiryMinutes { get; set; } = 10;
    public int OtpCooldownSeconds { get; set; } = 60;
    public int OtpMaxPerHourPerUser { get; set; } = 3;
    public int OtpMaxPerHourPerIp { get; set; } = 25;
    public int OtpMaxAttempts { get; set; } = 5;
    public int OtpLockMinutes { get; set; } = 15;
    public int PasswordMinLength { get; set; } = 12;
}
