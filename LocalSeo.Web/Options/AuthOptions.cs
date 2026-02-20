namespace LocalSeo.Web.Options;

public sealed class AuthOptions
{
    public int LockoutThreshold { get; set; } = 5;
    public int LockoutMinutes { get; set; } = 15;
}
