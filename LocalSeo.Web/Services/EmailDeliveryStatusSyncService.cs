using System.Net.Http.Headers;
using System.Text.Json;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IEmailDeliveryStatusSyncService
{
    Task<int> RefreshPendingAsync(IReadOnlyList<EmailLogListRow> rows, CancellationToken ct);
    Task<bool> RefreshPendingAsync(EmailLogDetailsRow row, CancellationToken ct);
}

public sealed class SendGridEmailDeliveryStatusSyncService(
    IHttpClientFactory httpClientFactory,
    IOptions<SendGridOptions> sendGridOptions,
    IEmailLogRepository emailLogRepository,
    TimeProvider timeProvider,
    ILogger<SendGridEmailDeliveryStatusSyncService> logger) : IEmailDeliveryStatusSyncService
{
    private const int MaxLookupsPerRequest = 5;

    public async Task<int> RefreshPendingAsync(IReadOnlyList<EmailLogListRow> rows, CancellationToken ct)
    {
        if (rows.Count == 0)
            return 0;

        var candidates = rows
            .Where(ShouldRefresh)
            .Take(MaxLookupsPerRequest)
            .Select(x => new EmailSyncCandidate(x.Id, x.Status, x.LastProviderEvent, x.SendGridMessageId, x.ToEmail))
            .ToArray();

        return await RefreshPendingInternalAsync(candidates, ct);
    }

    public async Task<bool> RefreshPendingAsync(EmailLogDetailsRow row, CancellationToken ct)
    {
        if (!ShouldRefresh(row.Status, row.LastProviderEvent, row.SendGridMessageId))
            return false;

        var refreshed = await RefreshPendingInternalAsync(
            [new EmailSyncCandidate(row.Id, row.Status, row.LastProviderEvent, row.SendGridMessageId, row.ToEmail)],
            ct);
        return refreshed > 0;
    }

    private async Task<int> RefreshPendingInternalAsync(IReadOnlyList<EmailSyncCandidate> candidates, CancellationToken ct)
    {
        if (candidates.Count == 0)
            return 0;

        var apiKey = (sendGridOptions.Value.ApiKey ?? string.Empty).Trim();
        if (apiKey.Length == 0)
            return 0;

        var refreshed = 0;
        foreach (var candidate in candidates)
        {
            ct.ThrowIfCancellationRequested();
            if (candidate.SendGridMessageId is null)
                continue;

            var lookup = await TryGetMessageStatusAsync(apiKey, candidate.SendGridMessageId, candidate.ToEmail, ct);
            if (lookup is null || lookup.EventType is null)
                continue;

            await emailLogRepository.UpdateLastProviderEventAsync(
                candidate.EmailLogId,
                lookup.EventType,
                lookup.EventUtc ?? timeProvider.GetUtcNow().UtcDateTime,
                ct);
            refreshed++;
        }

        return refreshed;
    }

    private async Task<SendGridMessageStatus?> TryGetMessageStatusAsync(string apiKey, string sendGridMessageId, string? toEmail, CancellationToken ct)
    {
        var client = httpClientFactory.CreateClient();
        var logLookup = await TryGetMessageStatusFromLogsAsync(client, apiKey, sendGridMessageId, ct);
        if (logLookup.Status is not null)
            return logLookup.Status;
        if (!logLookup.NotFound)
            return null;

        return await TryGetMessageStatusFromMessagesAsync(client, apiKey, sendGridMessageId, toEmail, ct);
    }

    private async Task<(SendGridMessageStatus? Status, bool NotFound)> TryGetMessageStatusFromLogsAsync(HttpClient client, string apiKey, string sendGridMessageId, CancellationToken ct)
    {
        var escapedMessageId = Uri.EscapeDataString(sendGridMessageId);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://api.sendgrid.com/v3/logs/{escapedMessageId}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        using var response = await client.SendAsync(request, ct);

        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return (null, true);

        if (!response.IsSuccessStatusCode)
        {
            if ((int)response.StatusCode >= 500 || response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                logger.LogWarning(
                    "SendGrid /v3/logs lookup returned HTTP {StatusCode} for messageId={MessageId}.",
                    (int)response.StatusCode,
                    sendGridMessageId);
            }

            return (null, false);
        }

        var json = await response.Content.ReadAsStringAsync(ct);
        if (string.IsNullOrWhiteSpace(json))
            return (null, false);

        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var status = ReadString(root, "status");
            var latest = ReadLatestEvent(root);

            var eventType = NormalizeEventType(latest.EventType) ?? NormalizeEventType(status);
            if (eventType is null)
                return (null, false);

            return (new SendGridMessageStatus(eventType, latest.EventUtc), false);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "SendGrid /v3/logs lookup returned invalid JSON for messageId={MessageId}.", sendGridMessageId);
            return (null, false);
        }
    }

    private async Task<SendGridMessageStatus?> TryGetMessageStatusFromMessagesAsync(HttpClient client, string apiKey, string sendGridMessageId, string? toEmail, CancellationToken ct)
    {
        var queryExpression = $"msg_id LIKE '{EscapeQueryValue(sendGridMessageId)}%'";
        var escapedQuery = Uri.EscapeDataString(queryExpression);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://api.sendgrid.com/v3/messages?limit=5&query={escapedQuery}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        using var response = await client.SendAsync(request, ct);

        if (!response.IsSuccessStatusCode)
        {
            if ((int)response.StatusCode >= 500 || response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                logger.LogWarning(
                    "SendGrid /v3/messages lookup returned HTTP {StatusCode} for messageId={MessageId}.",
                    (int)response.StatusCode,
                    sendGridMessageId);
            }

            return null;
        }

        var json = await response.Content.ReadAsStringAsync(ct);
        if (string.IsNullOrWhiteSpace(json))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(json);
            return ReadMessageStatusFromMessagesResponse(doc.RootElement, toEmail);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "SendGrid /v3/messages lookup returned invalid JSON for messageId={MessageId}.", sendGridMessageId);
            return null;
        }
    }

    private static bool ShouldRefresh(EmailLogListRow row)
        => ShouldRefresh(row.Status, row.LastProviderEvent, row.SendGridMessageId);

    private static bool ShouldRefresh(string status, string? lastProviderEvent, string? sendGridMessageId)
    {
        if (string.IsNullOrWhiteSpace(sendGridMessageId))
            return false;
        if (string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase))
            return false;
        return !IsTerminalEvent(lastProviderEvent);
    }

    private static bool IsTerminalEvent(string? eventType)
    {
        var value = (eventType ?? string.Empty).Trim().ToLowerInvariant();
        return value is "delivered" or "open" or "click" or "bounce" or "bounced" or "dropped" or "spamreport" or "blocked";
    }

    private static SendGridMessageStatus? ReadMessageStatusFromMessagesResponse(JsonElement root, string? toEmail)
    {
        if (!TryGetPropertyIgnoreCase(root, "messages", out var messages) || messages.ValueKind != JsonValueKind.Array)
            return null;

        SendGridMessageStatus? fallback = null;
        foreach (var message in messages.EnumerateArray())
        {
            var normalized = NormalizeEventType(ReadString(message, "status"));
            if (normalized is null)
                continue;

            var candidate = new SendGridMessageStatus(normalized, ReadIsoTimestampUtc(message, "last_event_time"));
            if (fallback is null)
                fallback = candidate;

            var candidateToEmail = ReadString(message, "to_email");
            if (string.IsNullOrWhiteSpace(toEmail) || string.Equals(candidateToEmail, toEmail, StringComparison.OrdinalIgnoreCase))
                return candidate;
        }

        return fallback;
    }

    private static (string? EventType, DateTime? EventUtc) ReadLatestEvent(JsonElement root)
    {
        if (!TryGetPropertyIgnoreCase(root, "events", out var events) || events.ValueKind != JsonValueKind.Array)
            return (null, null);

        string? latestType = null;
        DateTime? latestUtc = null;

        foreach (var evt in events.EnumerateArray())
        {
            var timestamp = ReadUnixTimestampUtc(evt, "timestamp");
            var eventType = ReadString(evt, "event");
            if (!timestamp.HasValue && eventType is null)
                continue;

            if (!latestUtc.HasValue || (timestamp.HasValue && timestamp.Value >= latestUtc.Value))
            {
                latestUtc = timestamp ?? latestUtc;
                latestType = eventType ?? latestType;
            }
        }

        return (latestType, latestUtc);
    }

    private static string? NormalizeEventType(string? value)
    {
        var normalized = (value ?? string.Empty).Trim().ToLowerInvariant();
        if (normalized.Length == 0)
            return null;

        return normalized switch
        {
            "delivered" => "delivered",
            "open" => "delivered",
            "click" => "delivered",
            "deferred" => "deferred",
            "processed" => "processed",
            "processing" => "processed",
            "sent" => "processed",
            "drop" => "dropped",
            "cancel_drop" => "dropped",
            "dropped" => "dropped",
            "blocked" => "bounce",
            "bounced" => "bounce",
            "bounce" => "bounce",
            "not_delivered" => "bounce",
            "spamreport" => "spamreport",
            _ => null
        };
    }

    private static string EscapeQueryValue(string input)
        => (input ?? string.Empty).Replace("'", "''", StringComparison.Ordinal);

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

    private static DateTime? ReadIsoTimestampUtc(JsonElement element, string propertyName)
    {
        var value = ReadString(element, propertyName);
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return DateTimeOffset.TryParse(value, out var parsed)
            ? parsed.UtcDateTime
            : null;
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

    private sealed record EmailSyncCandidate(long EmailLogId, string Status, string? LastProviderEvent, string? SendGridMessageId, string? ToEmail);
    private sealed record SendGridMessageStatus(string? EventType, DateTime? EventUtc);
}
