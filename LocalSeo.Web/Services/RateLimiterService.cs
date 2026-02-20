using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record RateLimitDecision(bool Allowed, string? ReasonCategory);

public interface IRateLimiterService
{
    Task<RateLimitDecision> CanRequestCodeAsync(string emailNormalized, string? requestedFromIp, CancellationToken ct);
}

public sealed class RateLimiterService(
    IEmailCodeRepository emailCodeRepository,
    IOptions<EmailCodesOptions> options,
    TimeProvider timeProvider) : IRateLimiterService
{
    public async Task<RateLimitDecision> CanRequestCodeAsync(string emailNormalized, string? requestedFromIp, CancellationToken ct)
    {
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var cfg = options.Value;

        var latestForEmail = await emailCodeRepository.GetLatestCreatedAtUtcAsync(emailNormalized, ct);
        if (latestForEmail.HasValue && latestForEmail.Value > nowUtc.AddSeconds(-cfg.CooldownSeconds))
            return new RateLimitDecision(false, "cooldown");

        var emailCount = await emailCodeRepository.CountCreatedInLastHourForEmailAsync(emailNormalized, nowUtc.AddHours(-1), ct);
        if (emailCount >= cfg.MaxPerHourPerEmail)
            return new RateLimitDecision(false, "max_per_hour_email");

        if (!string.IsNullOrWhiteSpace(requestedFromIp))
        {
            var ipCount = await emailCodeRepository.CountCreatedInLastHourForIpAsync(requestedFromIp.Trim(), nowUtc.AddHours(-1), ct);
            if (ipCount >= cfg.MaxPerHourPerIp)
                return new RateLimitDecision(false, "max_per_hour_ip");
        }

        return new RateLimitDecision(true, null);
    }
}
