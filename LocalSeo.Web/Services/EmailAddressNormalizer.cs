namespace LocalSeo.Web.Services;

public interface IEmailAddressNormalizer
{
    string Normalize(string? emailAddress);
}

public sealed class EmailAddressNormalizer : IEmailAddressNormalizer
{
    public string Normalize(string? emailAddress)
    {
        return (emailAddress ?? string.Empty).Trim().ToLowerInvariant();
    }
}
