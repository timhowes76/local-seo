using System.Security.Cryptography;
using System.Text;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ICryptoService
{
    byte[] GenerateRandomBytes(int length);
    string Base64UrlEncode(ReadOnlySpan<byte> value);
    bool TryBase64UrlDecode(string? value, out byte[] bytes);
    byte[] ComputeHmacSha256(ReadOnlySpan<byte> value);
    bool FixedTimeEquals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right);
}

public sealed class CryptoService(IOptions<InviteOptions> options) : ICryptoService
{
    private readonly byte[] hmacKey = BuildKey(options.Value.HmacSecret);

    public byte[] GenerateRandomBytes(int length)
    {
        if (length <= 0)
            throw new ArgumentOutOfRangeException(nameof(length), "Length must be positive.");
        return RandomNumberGenerator.GetBytes(length);
    }

    public string Base64UrlEncode(ReadOnlySpan<byte> value)
    {
        var base64 = Convert.ToBase64String(value);
        return base64.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    public bool TryBase64UrlDecode(string? value, out byte[] bytes)
    {
        bytes = [];
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalized = value.Trim().Replace('-', '+').Replace('_', '/');
        switch (normalized.Length % 4)
        {
            case 2:
                normalized += "==";
                break;
            case 3:
                normalized += "=";
                break;
            case 0:
                break;
            default:
                return false;
        }

        try
        {
            bytes = Convert.FromBase64String(normalized);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    public byte[] ComputeHmacSha256(ReadOnlySpan<byte> value)
    {
        using var hmac = new HMACSHA256(hmacKey);
        return hmac.ComputeHash(value.ToArray());
    }

    public bool FixedTimeEquals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
        => CryptographicOperations.FixedTimeEquals(left, right);

    private static byte[] BuildKey(string? configuredSecret)
    {
        var secret = (configuredSecret ?? string.Empty).Trim();
        if (secret.Length < 32)
            throw new InvalidOperationException("Invites:HmacSecret must be configured with at least 32 characters.");
        return Encoding.UTF8.GetBytes(secret);
    }
}
