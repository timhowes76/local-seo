using System.Security.Cryptography;

namespace LocalSeo.Web.Services;

public interface ICodeHasher
{
    (byte[] hash, byte[] salt) HashCode(string code);
    bool Verify(string code, byte[] salt, byte[] hash);
}

public sealed class CodeHasher : ICodeHasher
{
    public (byte[] hash, byte[] salt) HashCode(string code)
    {
        var salt = RandomNumberGenerator.GetBytes(32);
        var hash = Rfc2898DeriveBytes.Pbkdf2(code, salt, 100_000, HashAlgorithmName.SHA256, 64);
        return (hash, salt);
    }

    public bool Verify(string code, byte[] salt, byte[] hash)
    {
        var computed = Rfc2898DeriveBytes.Pbkdf2(code, salt, 100_000, HashAlgorithmName.SHA256, 64);
        return CryptographicOperations.FixedTimeEquals(hash, computed);
    }
}
