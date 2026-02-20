namespace LocalSeo.Web.Options;

public sealed class EmailCodesOptions
{
    public int CooldownSeconds { get; set; } = 60;
    public int MaxPerHourPerEmail { get; set; } = 10;
    public int MaxPerHourPerIp { get; set; } = 50;
    public int ExpiryMinutes { get; set; } = 10;
}
