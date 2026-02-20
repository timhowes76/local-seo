using Microsoft.AspNetCore.Identity;

namespace LocalSeo.Web.Services;

public interface IPasswordHasherService
{
    byte PasswordHashVersion { get; }
    byte[] HashPassword(string password);
    bool VerifyPassword(byte[] passwordHash, string password, out bool needsRehash);
}

public sealed class PasswordHasherService : IPasswordHasherService
{
    private readonly PasswordHasher<PasswordHasherUserContext> passwordHasher = new();

    public byte PasswordHashVersion => 1;

    public byte[] HashPassword(string password)
    {
        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentException("Password is required.", nameof(password));

        // TODO(auth-v2): enforce stronger configurable password policy.
        var hash = passwordHasher.HashPassword(new PasswordHasherUserContext(), password);
        return System.Text.Encoding.UTF8.GetBytes(hash);
    }

    public bool VerifyPassword(byte[] passwordHash, string password, out bool needsRehash)
    {
        needsRehash = false;
        if (passwordHash.Length == 0 || string.IsNullOrWhiteSpace(password))
            return false;

        var hash = System.Text.Encoding.UTF8.GetString(passwordHash);
        var verification = passwordHasher.VerifyHashedPassword(new PasswordHasherUserContext(), hash, password);
        needsRehash = verification == PasswordVerificationResult.SuccessRehashNeeded;
        return verification != PasswordVerificationResult.Failed;
    }

    private sealed class PasswordHasherUserContext;
}
