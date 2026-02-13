namespace LocalSeo.Web.Options;

public sealed class AuthOptions
{
    public string AllowedDomain { get; set; } = "kontrolit.net";
    public int CodeTtlMinutes { get; set; } = 10;
    public int MaxAttempts { get; set; } = 5;
    public int MaxSendsPerHour { get; set; } = 3;
}
