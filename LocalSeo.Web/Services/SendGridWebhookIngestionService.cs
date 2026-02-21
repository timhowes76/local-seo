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
            logger.LogWarning("SendGrid webhook signature validation bypassed because no public key is configured. Configure SendGrid:EventWebhookPublicKeyPem to enforce signature verification.");
            return true;
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
                var emailLogId = ReadEmailLogId(evt);
                var providerMessageId = NormalizeValue(ReadString(evt, "sg_message_id"), 200)
                    ?? NormalizeValue(ReadString(evt, "smtp-id"), 200)
                    ?? NormalizeValue(ReadString(evt, "smtp_id"), 200)
                    ?? NormalizeValue(ReadString(evt, "message_id"), 200)
                    ?? NormalizeValue(ReadString(evt, "sg_event_id"), 200)
                    ?? (emailLogId.HasValue ? $"email-log-{emailLogId.Value}" : null);

                if (string.IsNullOrWhiteSpace(providerMessageId))
                    continue;

                var eventUtc = ReadUnixTimestampUtc(evt, "timestamp") ?? nowUtc;
                if (!emailLogId.HasValue)
                    emailLogId = await emailLogRepository.FindByProviderMessageIdAsync(providerMessageId, ct);
                var payload = Truncate(evt.GetRawText(), MaxPayloadLength);
                var created = await providerEventRepository.InsertIfNotExistsAsync(new EmailProviderEventCreateRequest(
                    EmailLogId: emailLogId,
                    Provider: ProviderName,
                    EventType: eventType,
                    EventUtc: eventUtc,
                    ProviderMessageId: providerMessageId,
                    PayloadJson: payload,
                    CreatedUtc: nowUtc), ct);

                if (created)
                    inserted++;

                // Duplicate provider events can arrive later with EmailLogId available.
                // Keep log state in sync even when event insert is idempotently skipped.
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
        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return null;
        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null
        };
    }

    private static long? ReadEmailLogId(JsonElement element)
    {
        if (TryReadEmailLogIdFromContainer(element, "custom_args", out var customArgId))
            return customArgId;
        if (TryReadEmailLogIdFromContainer(element, "unique_args", out var uniqueArgId))
            return uniqueArgId;
        if (TryReadLongProperty(element, "email_log_id", out var flattenedId) && flattenedId > 0)
            return flattenedId;
        if (TryReadLongProperty(element, "emailLogId", out flattenedId) && flattenedId > 0)
            return flattenedId;
        if (TryReadLongProperty(element, "EmailLogId", out flattenedId) && flattenedId > 0)
            return flattenedId;
        return null;
    }

    private static bool TryReadEmailLogIdFromContainer(JsonElement root, string containerPropertyName, out long emailLogId)
    {
        emailLogId = 0;
        if (!TryGetPropertyIgnoreCase(root, containerPropertyName, out var container) || container.ValueKind != JsonValueKind.Object)
            return false;

        if (TryReadLongProperty(container, "email_log_id", out emailLogId))
            return emailLogId > 0;
        if (TryReadLongProperty(container, "emailLogId", out emailLogId))
            return emailLogId > 0;
        if (TryReadLongProperty(container, "EmailLogId", out emailLogId))
            return emailLogId > 0;
        return false;
    }

    private static bool TryReadLongProperty(JsonElement container, string propertyName, out long value)
    {
        value = 0;
        if (!TryGetPropertyIgnoreCase(container, propertyName, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out value))
            return true;
        if (property.ValueKind == JsonValueKind.String && long.TryParse(property.GetString(), out value))
            return true;
        return false;
    }

    private static DateTime? ReadUnixTimestampUtc(JsonElement element, string propertyName)
    {
        if (!TryGetPropertyIgnoreCase(element, propertyName, out var property))
            return null;

        long timestamp;
        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out timestamp))
            return DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime;
        if (property.ValueKind == JsonValueKind.String && long.TryParse(property.GetString(), out timestamp))
            return DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime;
        return null;
    }

    private static bool TryGetPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement property)
    {
        property = default;
        if (element.ValueKind != JsonValueKind.Object)
            return false;

        if (element.TryGetProperty(propertyName, out property))
            return true;

        foreach (var candidate in element.EnumerateObject())
        {
            if (!string.Equals(candidate.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                continue;
            property = candidate.Value;
            return true;
        }

        return false;
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
