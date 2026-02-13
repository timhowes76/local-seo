using System.Security.Cryptography;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ILoginService
{
    Task<(bool ok, string message)> RequestCodeAsync(string email, CancellationToken ct);
    Task<(bool ok, string message)> VerifyCodeAsync(string email, string code, CancellationToken ct);
}

public sealed class LoginService(
    ISqlConnectionFactory connectionFactory,
    ICodeHasher hasher,
    IEmailSender emailSender,
    IOptions<AuthOptions> options,
    ILogger<LoginService> logger) : ILoginService
{
    public async Task<(bool ok, string message)> RequestCodeAsync(string email, CancellationToken ct)
    {
        email = email.Trim().ToLowerInvariant();
        var allowedDomain = options.Value.AllowedDomain.ToLowerInvariant();
        if (!email.EndsWith($"@{allowedDomain}", StringComparison.Ordinal))
            return (false, "Email domain is not allowed.");

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var throttle = await conn.QuerySingleOrDefaultAsync<(DateTime WindowStartUtc, int SendCount)>(new CommandDefinition(
            "SELECT WindowStartUtc, SendCount FROM dbo.LoginThrottle WHERE Email=@Email", new { Email = email }, tx, cancellationToken: ct));

        var now = DateTime.UtcNow;
        if (throttle != default && throttle.WindowStartUtc > now.AddHours(-1) && throttle.SendCount >= options.Value.MaxSendsPerHour)
            return (false, "Too many code requests. Try again later.");

        if (throttle == default)
        {
            await conn.ExecuteAsync(new CommandDefinition("INSERT INTO dbo.LoginThrottle(Email,WindowStartUtc,SendCount) VALUES(@Email,@Now,1)", new { Email = email, Now = now }, tx, cancellationToken: ct));
        }
        else if (throttle.WindowStartUtc <= now.AddHours(-1))
        {
            await conn.ExecuteAsync(new CommandDefinition("UPDATE dbo.LoginThrottle SET WindowStartUtc=@Now, SendCount=1 WHERE Email=@Email", new { Email = email, Now = now }, tx, cancellationToken: ct));
        }
        else
        {
            await conn.ExecuteAsync(new CommandDefinition("UPDATE dbo.LoginThrottle SET SendCount = SendCount + 1 WHERE Email=@Email", new { Email = email }, tx, cancellationToken: ct));
        }

        var code = RandomNumberGenerator.GetInt32(0, 1_000_000).ToString("D6");
        var (hash, salt) = hasher.HashCode(code);
        var expires = now.AddMinutes(options.Value.CodeTtlMinutes);

        await conn.ExecuteAsync(new CommandDefinition(@"INSERT INTO dbo.LoginCode(Email,CodeHash,Salt,ExpiresAtUtc)
VALUES(@Email,@CodeHash,@Salt,@ExpiresAtUtc)", new { Email = email, CodeHash = hash, Salt = salt, ExpiresAtUtc = expires }, tx, cancellationToken: ct));

        await tx.CommitAsync(ct);

        await emailSender.SendLoginCodeAsync(email, code, ct);
        logger.LogInformation("Login code generated for {Email}", email);
        return (true, "A login code was sent.");
    }

    public async Task<(bool ok, string message)> VerifyCodeAsync(string email, string code, CancellationToken ct)
    {
        email = email.Trim().ToLowerInvariant();
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);

        var record = await conn.QuerySingleOrDefaultAsync<LoginCodeRow>(new CommandDefinition(@"
SELECT TOP 1 LoginCodeId, Email, CodeHash, Salt, ExpiresAtUtc, FailedAttempts, IsUsed
FROM dbo.LoginCode
WHERE Email=@Email
ORDER BY CreatedAtUtc DESC", new { Email = email }, cancellationToken: ct));

        if (record is null || record.IsUsed || record.ExpiresAtUtc < DateTime.UtcNow)
            return (false, "Code is invalid or expired.");
        if (record.FailedAttempts >= options.Value.MaxAttempts)
            return (false, "Max attempts reached.");

        if (!hasher.Verify(code.Trim(), record.Salt, record.CodeHash))
        {
            await conn.ExecuteAsync(new CommandDefinition("UPDATE dbo.LoginCode SET FailedAttempts = FailedAttempts + 1 WHERE LoginCodeId=@Id", new { Id = record.LoginCodeId }, cancellationToken: ct));
            return (false, "Code is invalid.");
        }

        await conn.ExecuteAsync(new CommandDefinition("UPDATE dbo.LoginCode SET IsUsed=1 WHERE LoginCodeId=@Id", new { Id = record.LoginCodeId }, cancellationToken: ct));
        return (true, "Login successful.");
    }

    private sealed class LoginCodeRow
    {
        public long LoginCodeId { get; init; }
        public string Email { get; init; } = string.Empty;
        public byte[] CodeHash { get; init; } = Array.Empty<byte>();
        public byte[] Salt { get; init; } = Array.Empty<byte>();
        public DateTime ExpiresAtUtc { get; init; }
        public int FailedAttempts { get; init; }
        public bool IsUsed { get; init; }
    }
}
