using System.Text.Json;
using System.Text.RegularExpressions;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;

namespace LocalSeo.Web.Services;

public interface IZohoLeadSyncService
{
    Task<ZohoLeadSyncResult> CreateLeadForPlaceAsync(string placeId, string localSeoLink, CancellationToken ct);
}

public sealed record ZohoLeadSyncResult(
    bool Success,
    string Message,
    string? ZohoLeadId,
    bool UsedExistingLead);

public sealed class ZohoLeadSyncService(
    ISqlConnectionFactory connectionFactory,
    IZohoCrmClient zohoCrmClient,
    IAdminSettingsService adminSettingsService,
    ILogger<ZohoLeadSyncService> logger) : IZohoLeadSyncService
{
    private static readonly Regex UkPostcodeRegex = new(
        @"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex GenericPostalRegex = new(
        @"\b(\d{4,6}(?:-\d{3,4})?)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public async Task<ZohoLeadSyncResult> CreateLeadForPlaceAsync(string placeId, string localSeoLink, CancellationToken ct)
    {
        var normalizedPlaceId = (placeId ?? string.Empty).Trim();
        if (normalizedPlaceId.Length == 0)
            return new ZohoLeadSyncResult(false, "Place ID is required.", null, false);

        var normalizedLocalSeoLink = NormalizeLocalSeoLink(localSeoLink, normalizedPlaceId);
        var nowUtc = DateTime.UtcNow;
        var place = await LoadPlaceAsync(normalizedPlaceId, ct);
        if (place is null)
            return new ZohoLeadSyncResult(false, "Place not found.", null, false);

        if (place.ZohoLeadCreated && !string.IsNullOrWhiteSpace(place.ZohoLeadId))
        {
            await MarkSyncSuccessAsync(normalizedPlaceId, place.ZohoLeadId, nowUtc, ct);
            return new ZohoLeadSyncResult(
                true,
                $"Zoho lead already exists for this place (Lead ID {place.ZohoLeadId}).",
                place.ZohoLeadId,
                true);
        }

        var settings = await adminSettingsService.GetAsync(ct);
        var websiteHost = NormalizeWebsiteHost(place.WebsiteUri);
        var descriptionEntry = BuildDescriptionEntry(nowUtc);
        var address = ParseAddress(place.FormattedAddress);

        try
        {
            var existingLead = await FindExistingActiveLeadAsync(place, websiteHost, normalizedLocalSeoLink, ct);
            if (existingLead is not null)
            {
                var currentDescription = await GetLeadDescriptionAsync(existingLead.LeadId, ct);
                var description = AppendDescription(currentDescription, descriptionEntry);
                var updatePayload = BuildExistingLeadUpdatePayload(normalizedLocalSeoLink, description);

                using var updateResponse = await zohoCrmClient.UpdateLeadAsync(existingLead.LeadId, updatePayload, ct);
                EnsureMutationSuccess(updateResponse.RootElement, "Zoho lead update failed.");

                await MarkSyncSuccessAsync(normalizedPlaceId, existingLead.LeadId, nowUtc, ct);
                return new ZohoLeadSyncResult(
                    true,
                    $"Linked to existing active Zoho lead {existingLead.LeadId} (matched by {existingLead.MatchReason}).",
                    existingLead.LeadId,
                    true);
            }

            var createPayload = BuildCreateLeadPayload(place, settings, websiteHost, normalizedLocalSeoLink, descriptionEntry, address);
            var duplicateCheckFields = BuildDuplicateCheckFields(place.NationalPhoneNumber, websiteHost);

            using var upsertResponse = await zohoCrmClient.UpsertLeadAsync(createPayload, duplicateCheckFields, ct);
            var mutation = ParseMutationResult(upsertResponse.RootElement, "Zoho lead upsert failed.");
            if (string.IsNullOrWhiteSpace(mutation.LeadId))
                throw new InvalidOperationException("Zoho lead upsert did not return a lead ID.");

            if (string.Equals(mutation.Action, "update", StringComparison.OrdinalIgnoreCase))
            {
                var currentDescription = await GetLeadDescriptionAsync(mutation.LeadId, ct);
                var description = AppendDescription(currentDescription, descriptionEntry);
                var updatePayload = BuildExistingLeadUpdatePayload(normalizedLocalSeoLink, description);
                using var updateResponse = await zohoCrmClient.UpdateLeadAsync(mutation.LeadId, updatePayload, ct);
                EnsureMutationSuccess(updateResponse.RootElement, "Zoho lead post-upsert update failed.");

                await MarkSyncSuccessAsync(normalizedPlaceId, mutation.LeadId, nowUtc, ct);
                return new ZohoLeadSyncResult(
                    true,
                    $"Linked to existing Zoho lead {mutation.LeadId}.",
                    mutation.LeadId,
                    true);
            }

            await MarkSyncSuccessAsync(normalizedPlaceId, mutation.LeadId, nowUtc, ct);
            return new ZohoLeadSyncResult(
                true,
                $"Created Zoho lead {mutation.LeadId}.",
                mutation.LeadId,
                false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await MarkSyncFailureAsync(normalizedPlaceId, nowUtc, ex.Message, ct);
            logger.LogWarning(ex, "Zoho lead sync failed for place {PlaceId}.", normalizedPlaceId);
            return new ZohoLeadSyncResult(false, $"Zoho sync failed: {ex.Message}", null, false);
        }
    }

    private async Task<PlaceLeadSourceRow?> LoadPlaceAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<PlaceLeadSourceRow>(new CommandDefinition(@"
SELECT TOP 1
  PlaceId,
  DisplayName,
  NationalPhoneNumber,
  WebsiteUri,
  FormattedAddress,
  OpeningDate,
  ZohoLeadCreated,
  ZohoLeadId
FROM dbo.Place
WHERE PlaceId = @PlaceId;", new { PlaceId = placeId }, cancellationToken: ct));
    }

    private async Task<ExistingLeadMatch?> FindExistingActiveLeadAsync(
        PlaceLeadSourceRow place,
        string? websiteHost,
        string localSeoLink,
        CancellationToken ct)
    {
        var normalizedCompanyName = NormalizeCompanyName(place.DisplayName);
        if (normalizedCompanyName is null)
            return null;

        var checks = new List<LeadDuplicateCheck>
        {
            new("Phone", place.NationalPhoneNumber, "phone number"),
            new("Company", place.DisplayName, "company name"),
            new("Website", websiteHost, "website host"),
            new("LocalSeoLink", localSeoLink, "LocalSeoLink")
        };

        foreach (var check in checks)
        {
            if (string.IsNullOrWhiteSpace(check.Value))
                continue;

            var criteria = BuildActiveLeadCriteria(check.FieldName, check.Value, place.DisplayName);
            using var searchResponse = await zohoCrmClient.SearchLeadsAsync(criteria, ct);
            if (!TryGetDataArray(searchResponse.RootElement, out var dataArray))
                continue;

            foreach (var lead in dataArray.EnumerateArray())
            {
                var leadId = GetString(lead, "id");
                if (string.IsNullOrWhiteSpace(leadId))
                    continue;
                var leadCompany = NormalizeCompanyName(GetString(lead, "Company"));
                if (!string.Equals(normalizedCompanyName, leadCompany, StringComparison.Ordinal))
                    continue;

                return new ExistingLeadMatch(leadId, check.MatchReason);
            }
        }

        return null;
    }

    private async Task<string?> GetLeadDescriptionAsync(string leadId, CancellationToken ct)
    {
        using var response = await zohoCrmClient.GetLeadByIdAsync(leadId, ct);
        if (!TryGetFirstDataItem(response.RootElement, out var lead))
            return null;

        return GetString(lead, "Description");
    }

    private static IReadOnlyList<string> BuildDuplicateCheckFields(string? phone, string? websiteHost)
    {
        var fields = new List<string> { "Company", "LocalSeoLink" };
        if (!string.IsNullOrWhiteSpace(phone))
            fields.Insert(0, "Phone");
        if (!string.IsNullOrWhiteSpace(websiteHost))
            fields.Add("Website");

        return fields
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static Dictionary<string, object> BuildCreateLeadPayload(
        PlaceLeadSourceRow place,
        AdminSettingsModel settings,
        string? websiteHost,
        string localSeoLink,
        string description,
        ParsedAddress address)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Company"] = CoalesceOrDefault(place.DisplayName, place.PlaceId),
            ["Last_Name"] = "Unknown",
            ["Phone"] = NormalizeNullable(place.NationalPhoneNumber),
            ["Website"] = websiteHost,
            ["Lead_Source"] = "LocalSeo",
            ["Opportunity"] = "Local SEO",
            ["Lead_Status"] = "Not Contacted",
            ["Rating"] = "Active",
            ["Street"] = address.Street1,
            ["Street2"] = address.Street2,
            ["Street3"] = address.Street3,
            ["City"] = address.City,
            ["State"] = address.Province,
            ["Province"] = address.Province,
            ["Zip_Code"] = address.PostalCode,
            ["Postalcode"] = address.PostalCode,
            ["Country"] = address.Country,
            ["Established"] = place.OpeningDate?.ToString("yyyy-MM-dd"),
            ["NextAction"] = NormalizeNullable(settings.ZohoLeadNextAction),
            ["LocalSeoLink"] = localSeoLink,
            ["Description"] = description
        };

        var ownerId = NormalizeNullable(settings.ZohoLeadOwnerId);
        if (!string.IsNullOrWhiteSpace(ownerId))
            payload["Owner"] = new Dictionary<string, object> { ["id"] = ownerId };

        return CompactPayload(payload);
    }

    private static Dictionary<string, object> BuildExistingLeadUpdatePayload(string localSeoLink, string description)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["LocalSeoLink"] = localSeoLink,
            ["Description"] = description,
            ["Opportunity"] = "Local SEO"
        };
        return CompactPayload(payload);
    }

    private static Dictionary<string, object> CompactPayload(IReadOnlyDictionary<string, object?> payload)
    {
        var compact = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var pair in payload)
        {
            if (pair.Value is null)
                continue;
            if (pair.Value is string text && string.IsNullOrWhiteSpace(text))
                continue;

            compact[pair.Key] = pair.Value;
        }

        return compact;
    }

    private async Task MarkSyncSuccessAsync(string placeId, string zohoLeadId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Place
SET
  ZohoLeadCreated = 1,
  ZohoLeadCreatedAtUtc = COALESCE(ZohoLeadCreatedAtUtc, @NowUtc),
  ZohoLeadId = @ZohoLeadId,
  ZohoLastSyncAtUtc = @NowUtc,
  ZohoLastError = NULL
WHERE PlaceId = @PlaceId;", new
        {
            PlaceId = placeId,
            ZohoLeadId = zohoLeadId,
            NowUtc = nowUtc
        }, cancellationToken: ct));
    }

    private async Task MarkSyncFailureAsync(string placeId, DateTime nowUtc, string errorMessage, CancellationToken ct)
    {
        var normalizedError = Truncate(errorMessage, 2000);
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.Place
SET
  ZohoLastSyncAtUtc = @NowUtc,
  ZohoLastError = @ZohoLastError
WHERE PlaceId = @PlaceId;", new
        {
            PlaceId = placeId,
            NowUtc = nowUtc,
            ZohoLastError = normalizedError
        }, cancellationToken: ct));
    }

    private static string BuildDescriptionEntry(DateTime createdAtUtc)
    {
        return $"This lead was created on {createdAtUtc:yyyy-MM-dd HH:mm:ss} UTC from our LocalSeo app. Use the LocalSeoLink above to get detailed information about how this company is doing and how they compare to their competitors.";
    }

    private static string AppendDescription(string? existingDescription, string entry)
    {
        var normalizedEntry = (entry ?? string.Empty).Trim();
        if (normalizedEntry.Length == 0)
            return (existingDescription ?? string.Empty).Trim();

        var normalizedExisting = (existingDescription ?? string.Empty).Trim();
        if (normalizedExisting.Length == 0)
            return normalizedEntry;

        if (normalizedExisting.Contains(normalizedEntry, StringComparison.OrdinalIgnoreCase))
            return normalizedExisting;

        return $"{normalizedExisting}{Environment.NewLine}{Environment.NewLine}{normalizedEntry}";
    }

    private static ParsedAddress ParseAddress(string? formattedAddress)
    {
        var parts = (formattedAddress ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .ToList();
        if (parts.Count == 0)
            return new ParsedAddress(null, null, null, null, null, null, null);

        var country = parts[^1];
        parts.RemoveAt(parts.Count - 1);

        var postalCode = ExtractPostalCode(formattedAddress);
        if (!string.IsNullOrWhiteSpace(postalCode))
        {
            for (var i = parts.Count - 1; i >= 0; i--)
            {
                if (!parts[i].Contains(postalCode, StringComparison.OrdinalIgnoreCase))
                    continue;

                var cleaned = parts[i].Replace(postalCode, string.Empty, StringComparison.OrdinalIgnoreCase).Trim().Trim(',');
                if (cleaned.Length == 0)
                    parts.RemoveAt(i);
                else
                    parts[i] = cleaned;
                break;
            }
        }

        string? city = null;
        string? province = null;
        var streetParts = new List<string>();

        if (parts.Count >= 3)
        {
            province = parts[^1];
            city = parts[^2];
            streetParts = parts.Take(parts.Count - 2).ToList();
        }
        else if (parts.Count == 2)
        {
            city = parts[^1];
            streetParts = new List<string> { parts[0] };
        }
        else if (parts.Count == 1)
        {
            streetParts = new List<string> { parts[0] };
        }

        var street1 = streetParts.Count > 0 ? streetParts[0] : null;
        var street2 = streetParts.Count > 1 ? streetParts[1] : null;
        var street3 = streetParts.Count > 2 ? streetParts[2] : null;

        return new ParsedAddress(street1, street2, street3, city, province, postalCode, country);
    }

    private static string? ExtractPostalCode(string? formattedAddress)
    {
        var text = (formattedAddress ?? string.Empty).Trim();
        if (text.Length == 0)
            return null;

        var ukMatch = UkPostcodeRegex.Match(text);
        if (ukMatch.Success)
            return ukMatch.Groups[1].Value.Trim();

        var genericMatch = GenericPostalRegex.Match(text);
        if (genericMatch.Success)
            return genericMatch.Groups[1].Value.Trim();

        return null;
    }

    private static string BuildActiveLeadCriteria(string fieldName, string value, string? companyName)
    {
        var escapedCompany = EscapeCriteriaValue(companyName ?? string.Empty);
        var escapedValue = EscapeCriteriaValue(value);
        return $"(({fieldName}:equals:{escapedValue})and(Company:equals:{escapedCompany})and(Rating:equals:Active))";
    }

    private static string EscapeCriteriaValue(string value)
    {
        return (value ?? string.Empty)
            .Replace(@"\", @"\\", StringComparison.Ordinal)
            .Replace("(", @"\(", StringComparison.Ordinal)
            .Replace(")", @"\)", StringComparison.Ordinal)
            .Replace(",", @"\,", StringComparison.Ordinal);
    }

    private static ZohoMutationResult ParseMutationResult(JsonElement root, string errorPrefix)
    {
        if (!TryGetFirstDataItem(root, out var item))
            throw new InvalidOperationException($"{errorPrefix}: missing response payload.");

        var status = GetString(item, "status");
        if (!string.Equals(status, "success", StringComparison.OrdinalIgnoreCase))
        {
            var message = GetString(item, "message") ?? "Unknown Zoho error.";
            throw new InvalidOperationException($"{errorPrefix}: {message}");
        }

        var action = GetString(item, "action");
        string? leadId = null;
        if (item.TryGetProperty("details", out var details) && details.ValueKind == JsonValueKind.Object)
            leadId = GetString(details, "id");
        leadId ??= GetString(item, "id");

        return new ZohoMutationResult(leadId, action);
    }

    private static void EnsureMutationSuccess(JsonElement root, string errorPrefix)
    {
        _ = ParseMutationResult(root, errorPrefix);
    }

    private static bool TryGetDataArray(JsonElement root, out JsonElement dataArray)
    {
        dataArray = default;
        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
            return false;
        if (data.GetArrayLength() == 0)
            return false;

        dataArray = data;
        return true;
    }

    private static bool TryGetFirstDataItem(JsonElement root, out JsonElement item)
    {
        item = default;
        if (!TryGetDataArray(root, out var data))
            return false;

        item = data[0];
        return true;
    }

    private static string? GetString(JsonElement node, string propertyName)
    {
        if (!node.TryGetProperty(propertyName, out var property))
            return null;
        if (property.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        return property.ValueKind == JsonValueKind.String
            ? property.GetString()
            : property.ToString();
    }

    private static string NormalizeLocalSeoLink(string localSeoLink, string placeId)
    {
        var normalized = (localSeoLink ?? string.Empty).Trim();
        if (normalized.Length > 0)
            return normalized;

        return $"/places/{Uri.EscapeDataString(placeId)}";
    }

    private static string? NormalizeWebsiteHost(string? websiteUri)
    {
        var normalized = (websiteUri ?? string.Empty).Trim();
        if (normalized.Length == 0)
            return null;

        if (!Uri.TryCreate(normalized, UriKind.Absolute, out var uri) &&
            !Uri.TryCreate($"https://{normalized}", UriKind.Absolute, out uri))
        {
            return null;
        }

        var host = (uri.Host ?? string.Empty).Trim().TrimEnd('.');
        if (host.StartsWith("www.", StringComparison.OrdinalIgnoreCase))
            host = host[4..];
        return host.Length == 0 ? null : host.ToLowerInvariant();
    }

    private static string? NormalizeNullable(string? value)
    {
        var trimmed = (value ?? string.Empty).Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string CoalesceOrDefault(string? value, string fallback)
    {
        var normalizedValue = NormalizeNullable(value);
        if (!string.IsNullOrWhiteSpace(normalizedValue))
            return normalizedValue;

        return NormalizeNullable(fallback) ?? "Unknown";
    }

    private static string? NormalizeCompanyName(string? value)
    {
        var trimmed = NormalizeNullable(value);
        if (trimmed is null)
            return null;

        var collapsed = Regex.Replace(trimmed, @"\s+", " ").Trim();
        return collapsed.Length == 0 ? null : collapsed.ToUpperInvariant();
    }

    private static string Truncate(string? value, int maxLength)
    {
        var normalized = (value ?? string.Empty).Trim();
        if (normalized.Length <= maxLength)
            return normalized;
        return normalized[..maxLength];
    }

    private sealed record PlaceLeadSourceRow(
        string PlaceId,
        string? DisplayName,
        string? NationalPhoneNumber,
        string? WebsiteUri,
        string? FormattedAddress,
        DateTime? OpeningDate,
        bool ZohoLeadCreated,
        string? ZohoLeadId);

    private sealed record ExistingLeadMatch(string LeadId, string MatchReason);
    private sealed record LeadDuplicateCheck(string FieldName, string? Value, string MatchReason);
    private sealed record ParsedAddress(
        string? Street1,
        string? Street2,
        string? Street3,
        string? City,
        string? Province,
        string? PostalCode,
        string? Country);
    private sealed record ZohoMutationResult(string? LeadId, string? Action);
}
