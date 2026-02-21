using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public sealed record SendGridWebhookIngestionResult(bool Success, int ReceivedCount, int InsertedCount, string? Message);

public interface ISendGridWebhookSignatureValidator
{
    bool IsValid(string? timestampHeader, string? signatureHeader, ReadOnlySpan<byte> payloadUtf8);
}

public interface ISendGridWebhookIngestionService
{
    Task<SendGridWebhookIngestionResult> IngestAsync(string payloadJson, CancellationToken ct);
}

public sealed class SendGridWebhookSignatureValidator(
    IOptions<SendGridOptions> options,
    TimeProvider timeProvider,
    ILogger<SendGridWebhookSignatureValidator> logger) : ISendGridWebhookSignatureValidator
{
    private static readonly TimeSpan MaxTimestampSkew = TimeSpan.FromMinutes(10);

    public bool IsValid(string? timestampHeader, string? signatureHeader, ReadOnlySpan<byte> payloadUtf8)
    {
        var publicKeyValue = (options.Value.EventWebhookPublicKeyPem ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(publicKeyValue))
        {
            logger.LogWarning("SendGrid webhook signature validation failed: public key is not configured.");
            return false;
        }

        var timestamp = (timestampHeader ?? string.Empty).Trim();
        var signature = (signatureHeader ?? string.Empty).Trim();
        if (timestamp.Length == 0 || signature.Length == 0)
            return false;

        if (!long.TryParse(timestamp, out var unixTimestamp))
            return false;

        var nowUnixSeconds = timeProvider.GetUtcNow().ToUnixTimeSeconds();
        if (Math.Abs(nowUnixSeconds - unixTimestamp) > (long)MaxTimestampSkew.TotalSeconds)
            return false;

        byte[] signatureBytes;
        try
        {
            signatureBytes = Convert.FromBase64String(signature);
        }
        catch (FormatException)
        {
            return false;
        }

        var timestampBytes = Encoding.UTF8.GetBytes(timestamp);
        var signedData = new byte[timestampBytes.Length + payloadUtf8.Length];
        Buffer.BlockCopy(timestampBytes, 0, signedData, 0, timestampBytes.Length);
        payloadUtf8.CopyTo(signedData.AsSpan(timestampBytes.Length));

        try
        {
            using var ecdsa = ECDsa.Create();
            if (!TryImportPublicKey(ecdsa, publicKeyValue))
                return false;

            if (ecdsa.VerifyData(signedData, signatureBytes, HashAlgorithmName.SHA256, DSASignatureFormat.Rfc3279DerSequence))
                return true;

            return ecdsa.VerifyData(signedData, signatureBytes, HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
        }
        catch (Exception ex) when (ex is CryptographicException or FormatException)
        {
            logger.LogWarning(ex, "SendGrid webhook signature validation failed.");
            return false;
        }
    }

    private static bool TryImportPublicKey(ECDsa ecdsa, string configuredKey)
    {
        var key = configuredKey.Trim();
        if (key.Contains("BEGIN PUBLIC KEY", StringComparison.OrdinalIgnoreCase))
        {
            ecdsa.ImportFromPem(key);
            return true;
        }

        var bytes = Convert.FromBase64String(key);
        ecdsa.ImportSubjectPublicKeyInfo(bytes, out _);
        return true;
    }
}

public sealed class SendGridWebhookIngestionService(
    IEmailLogRepository emailLogRepository,
    IEmailProviderEventRepository providerEventRepository,
    TimeProvider timeProvider,
    ILogger<SendGridWebhookIngestionService> logger) : ISendGridWebhookIngestionService
{
    private const string ProviderName = "SendGrid";
    private const int MaxPayloadLength = 16 * 1024;

    public async Task<SendGridWebhookIngestionResult> IngestAsync(string payloadJson, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
            return new SendGridWebhookIngestionResult(false, 0, 0, "Payload is empty.");

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return new SendGridWebhookIngestionResult(false, 0, 0, "Payload root must be an array.");

            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
            var received = 0;
            var inserted = 0;

            foreach (var evt in doc.RootElement.EnumerateArray())
            {
                received++;
                var eventType = NormalizeValue(ReadString(evt, "event"), 50) ?? "unknown";
                var providerMessageId = NormalizeValue(ReadString(evt, "sg_message_id"), 200)
                    ?? NormalizeValue(ReadString(evt, "smtp-id"), 200)
                    ?? NormalizeValue(ReadString(evt, "smtp_id"), 200)
                    ?? NormalizeValue(ReadString(evt, "message_id"), 200);

                if (string.IsNullOrWhiteSpace(providerMessageId))
                    continue;

                var eventUtc = ReadUnixTimestampUtc(evt, "timestamp") ?? nowUtc;
                var emailLogId = await emailLogRepository.FindByProviderMessageIdAsync(providerMessageId, ct);
                var payload = Truncate(evt.GetRawText(), MaxPayloadLength);
                var created = await providerEventRepository.InsertIfNotExistsAsync(new EmailProviderEventCreateRequest(
                    EmailLogId: emailLogId,
                    Provider: ProviderName,
                    EventType: eventType,
                    EventUtc: eventUtc,
                    ProviderMessageId: providerMessageId,
                    PayloadJson: payload,
                    CreatedUtc: nowUtc), ct);

                if (!created)
                    continue;

                inserted++;
                if (emailLogId.HasValue)
                    await emailLogRepository.UpdateLastProviderEventAsync(emailLogId.Value, eventType, eventUtc, ct);
            }

            return new SendGridWebhookIngestionResult(true, received, inserted, null);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "SendGrid webhook payload was not valid JSON.");
            return new SendGridWebhookIngestionResult(false, 0, 0, "Payload is not valid JSON.");
        }
    }

    private static string? ReadString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
            return null;
        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null
        };
    }

    private static DateTime? ReadUnixTimestampUtc(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
            return null;

        long timestamp;
        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out timestamp))
            return DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime;
        if (property.ValueKind == JsonValueKind.String && long.TryParse(property.GetString(), out timestamp))
            return DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime;
        return null;
    }

    private static string? NormalizeValue(string? value, int maxLength)
    {
        var trimmed = (value ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;
        return trimmed.Length <= maxLength ? trimmed : trimmed[..maxLength];
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Length <= maxLength ? value : value[..maxLength];
    }
}
