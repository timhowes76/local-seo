using System.Security.Cryptography;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public sealed record IssuedEmailCode(int Rid, string Code, DateTime ExpiresAtUtc);

public interface IEmailCodeService
{
    Task<IssuedEmailCode> IssueAsync(
        EmailCodePurpose purpose,
        string email,
        string emailNormalized,
        string? requestedFromIp,
        string? requestedUserAgent,
        CancellationToken ct);

    Task<bool> TryConsumeAsync(
        int rid,
        EmailCodePurpose purpose,
        string emailNormalized,
        string code,
        CancellationToken ct);
}

public sealed class EmailCodeService(
    IEmailCodeRepository emailCodeRepository,
    ICodeHasher codeHasher,
    ISecuritySettingsProvider securitySettingsProvider,
    TimeProvider timeProvider,
    ILogger<EmailCodeService> logger) : IEmailCodeService
{
    public async Task<IssuedEmailCode> IssueAsync(
        EmailCodePurpose purpose,
        string email,
        string emailNormalized,
        string? requestedFromIp,
        string? requestedUserAgent,
        CancellationToken ct)
    {
        var settings = await securitySettingsProvider.GetAsync(ct);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var code = RandomNumberGenerator.GetInt32(0, 1_000_000).ToString("D6");
        var (hash, salt) = codeHasher.HashCode(code);
        var expiresAtUtc = nowUtc.AddMinutes(settings.EmailCodeExpiryMinutes);

        var rid = await emailCodeRepository.CreateAsync(new EmailCodeCreateRequest(
            purpose,
            email,
            emailNormalized,
            hash,
            salt,
            expiresAtUtc,
            requestedFromIp,
            requestedUserAgent), ct);

        logger.LogInformation(
            "Email code issued. Purpose={Purpose} Rid={Rid} Email={EmailNormalized}",
            purpose,
            rid,
            emailNormalized);

        return new IssuedEmailCode(rid, code, expiresAtUtc);
    }

    public async Task<bool> TryConsumeAsync(
        int rid,
        EmailCodePurpose purpose,
        string emailNormalized,
        string code,
        CancellationToken ct)
    {
        var settings = await securitySettingsProvider.GetAsync(ct);
        var row = await emailCodeRepository.GetByIdAsync(rid, ct);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

        if (row is null || row.Purpose != purpose || row.IsUsed || row.ExpiresAtUtc < nowUtc || row.FailedAttempts >= settings.EmailCodeMaxFailedAttemptsPerCode)
        {
            logger.LogWarning(
                "Email code verify failed. Purpose={Purpose} Rid={Rid} Category={Category}",
                purpose,
                rid,
                "not_found_or_expired_or_used");
            return false;
        }

        if (!string.Equals(row.EmailNormalized, emailNormalized, StringComparison.Ordinal))
        {
            await emailCodeRepository.IncrementFailedAttemptsAsync(rid, ct);
            logger.LogWarning(
                "Email code verify failed. Purpose={Purpose} Rid={Rid} Category={Category}",
                purpose,
                rid,
                "email_mismatch");
            return false;
        }

        if (!codeHasher.Verify(code.Trim(), row.Salt, row.CodeHash))
        {
            await emailCodeRepository.IncrementFailedAttemptsAsync(rid, ct);
            logger.LogWarning(
                "Email code verify failed. Purpose={Purpose} Rid={Rid} Category={Category}",
                purpose,
                rid,
                "code_mismatch");
            return false;
        }

        var markedUsed = await emailCodeRepository.TryMarkUsedAsync(rid, ct);
        if (!markedUsed)
        {
            logger.LogWarning(
                "Email code verify failed. Purpose={Purpose} Rid={Rid} Category={Category}",
                purpose,
                rid,
                "already_used_race");
            return false;
        }

        logger.LogInformation("Email code verify success. Purpose={Purpose} Rid={Rid}", purpose, rid);
        return true;
    }
}
