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
    bool UsedExistingLead,
    bool RequiresZohoTokenRefresh);

public sealed class ZohoLeadSyncService(
    ISqlConnectionFactory connectionFactory,
    IZohoCrmClient zohoCrmClient,
    IAdminSettingsService adminSettingsService,
    IHttpClientFactory httpClientFactory,
    IWebHostEnvironment webHostEnvironment,
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
            return new ZohoLeadSyncResult(false, "Place ID is required.", null, false, false);

        var nowUtc = DateTime.UtcNow;
        var place = await LoadPlaceAsync(normalizedPlaceId, ct);
        if (place is null)
            return new ZohoLeadSyncResult(false, "Place not found.", null, false, false);

        var settings = await adminSettingsService.GetAsync(ct);
        var normalizedLocalSeoLink = BuildLocalSeoLink(settings.SiteUrl, normalizedPlaceId, localSeoLink);
        var fieldResolution = await ResolveLeadFieldMapAsync(place.ZohoLeadId, ct);
        var leadFieldMap = fieldResolution.Map;
        logger.LogInformation(
            "Zoho lead field map resolved. CompanyNumber={CompanyNumberField}, Established={EstablishedField}, NextAction={NextActionField}, LocalSeoLink={LocalSeoLinkField}, LinkedIn={LinkedInField}, Facebook={FacebookField}, Instagram={InstagramField}, YouTube={YouTubeField}, TikTok={TikTokField}, Pinterest={PinterestField}, XTwitter={XTwitterField}, Bluesky={BlueskyField}, CustomerOrPartner={CustomerOrPartnerField}",
            leadFieldMap.CompanyNumberApiName,
            leadFieldMap.EstablishedApiName,
            leadFieldMap.NextActionApiName,
            leadFieldMap.LocalSeoLinkApiName,
            leadFieldMap.LinkedInApiName,
            leadFieldMap.FacebookApiName,
            leadFieldMap.InstagramApiName,
            leadFieldMap.YouTubeApiName,
            leadFieldMap.TikTokApiName,
            leadFieldMap.PinterestApiName,
            leadFieldMap.XTwitterApiName,
            leadFieldMap.BlueskyApiName,
            leadFieldMap.CustomerOrPartnerApiName);
        if (!HasAnyRequestedFieldMapping(leadFieldMap))
        {
            var mappingDiagnostic = $"Zoho field metadata lookup did not resolve any requested lead fields for mapping. {fieldResolution.Diagnostic}";
            var needsReconnect = mappingDiagnostic.Contains("OAUTH_SCOPE_MISMATCH", StringComparison.OrdinalIgnoreCase)
                || mappingDiagnostic.Contains("oauth scope", StringComparison.OrdinalIgnoreCase)
                || mappingDiagnostic.Contains("invalid oauth scope", StringComparison.OrdinalIgnoreCase);
            return new ZohoLeadSyncResult(
                false,
                needsReconnect
                    ? "Zoho scope permissions are missing. Reconnect Zoho to grant required CRM scopes, then try again."
                    : mappingDiagnostic,
                null,
                false,
                needsReconnect);
        }
        var websiteHost = NormalizeWebsiteHost(place.WebsiteUri);
        var descriptionEntry = BuildDescriptionEntry(nowUtc);
        var address = ParseAddress(place.FormattedAddress);

        try
        {
            if (!string.IsNullOrWhiteSpace(place.ZohoLeadId))
            {
                try
                {
                    var currentDescription = await GetLeadDescriptionAsync(place.ZohoLeadId, ct);
                    var description = AppendDescription(currentDescription, descriptionEntry);
                    var directUpdatePayload = BuildExistingLeadUpdatePayload(place, settings, leadFieldMap, normalizedLocalSeoLink, description);
                    using var directUpdateResponse = await zohoCrmClient.UpdateLeadAsync(place.ZohoLeadId, directUpdatePayload, ct);
                    EnsureMutationSuccess(directUpdateResponse.RootElement, "Zoho lead direct update failed.");
                    await UploadLeadPhotoFromLogoAsync(place, place.ZohoLeadId, ct);
                    await MarkSyncSuccessAsync(normalizedPlaceId, place.ZohoLeadId, nowUtc, ct);
                    return new ZohoLeadSyncResult(
                        true,
                        $"Updated existing Zoho lead {place.ZohoLeadId}.",
                        place.ZohoLeadId,
                        true,
                        false);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Direct update for Zoho lead {LeadId} failed; falling back to duplicate search/upsert.", place.ZohoLeadId);
                }
            }

            var existingLead = await FindExistingActiveLeadAsync(place, websiteHost, normalizedLocalSeoLink, leadFieldMap, ct);
            if (existingLead is not null)
            {
                var currentDescription = await GetLeadDescriptionAsync(existingLead.LeadId, ct);
                var description = AppendDescription(currentDescription, descriptionEntry);
                var updatePayload = BuildExistingLeadUpdatePayload(place, settings, leadFieldMap, normalizedLocalSeoLink, description);

                using var updateResponse = await zohoCrmClient.UpdateLeadAsync(existingLead.LeadId, updatePayload, ct);
                EnsureMutationSuccess(updateResponse.RootElement, "Zoho lead update failed.");
                await UploadLeadPhotoFromLogoAsync(place, existingLead.LeadId, ct);

                await MarkSyncSuccessAsync(normalizedPlaceId, existingLead.LeadId, nowUtc, ct);
                return new ZohoLeadSyncResult(
                    true,
                    $"Linked to existing active Zoho lead {existingLead.LeadId} (matched by {existingLead.MatchReason}).",
                    existingLead.LeadId,
                    true,
                    false);
            }

            var createPayload = BuildCreateLeadPayload(place, settings, leadFieldMap, websiteHost, normalizedLocalSeoLink, descriptionEntry, address);
            var duplicateCheckFields = BuildDuplicateCheckFields(place.NationalPhoneNumber, websiteHost, leadFieldMap.LocalSeoLinkApiName);

            using var upsertResponse = await zohoCrmClient.UpsertLeadAsync(createPayload, duplicateCheckFields, ct);
            var mutation = ParseMutationResult(upsertResponse.RootElement, "Zoho lead upsert failed.");
            if (string.IsNullOrWhiteSpace(mutation.LeadId))
                throw new InvalidOperationException("Zoho lead upsert did not return a lead ID.");

            if (string.Equals(mutation.Action, "update", StringComparison.OrdinalIgnoreCase))
            {
                var currentDescription = await GetLeadDescriptionAsync(mutation.LeadId, ct);
                var description = AppendDescription(currentDescription, descriptionEntry);
                var updatePayload = BuildExistingLeadUpdatePayload(place, settings, leadFieldMap, normalizedLocalSeoLink, description);
                using var updateResponse = await zohoCrmClient.UpdateLeadAsync(mutation.LeadId, updatePayload, ct);
                EnsureMutationSuccess(updateResponse.RootElement, "Zoho lead post-upsert update failed.");
                await UploadLeadPhotoFromLogoAsync(place, mutation.LeadId, ct);

                await MarkSyncSuccessAsync(normalizedPlaceId, mutation.LeadId, nowUtc, ct);
                return new ZohoLeadSyncResult(
                    true,
                    $"Linked to existing Zoho lead {mutation.LeadId}.",
                    mutation.LeadId,
                    true,
                    false);
            }

            await UploadLeadPhotoFromLogoAsync(place, mutation.LeadId, ct);
            await MarkSyncSuccessAsync(normalizedPlaceId, mutation.LeadId, nowUtc, ct);
            return new ZohoLeadSyncResult(
                true,
                $"Created Zoho lead {mutation.LeadId}.",
                mutation.LeadId,
                false,
                false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            await MarkSyncFailureAsync(normalizedPlaceId, nowUtc, ex.Message, ct);
            logger.LogWarning(ex, "Zoho lead sync failed for place {PlaceId}.", normalizedPlaceId);
            var requiresTokenRefresh = IsLikelyZohoTokenError(ex);
            return new ZohoLeadSyncResult(
                false,
                requiresTokenRefresh ? "Zoho authentication failed and needs to be refreshed." : $"Zoho sync failed: {ex.Message}",
                null,
                false,
                requiresTokenRefresh);
        }
    }

    private static bool IsLikelyZohoTokenError(Exception ex)
    {
        var message = new System.Text.StringBuilder();
        var cursor = ex;
        while (cursor is not null)
        {
            if (!string.IsNullOrWhiteSpace(cursor.Message))
            {
                message.Append(' ');
                message.Append(cursor.Message);
            }
            cursor = cursor.InnerException;
        }

        var text = message.ToString().ToLowerInvariant();
        if (text.Length == 0)
            return false;

        return text.Contains("invalid token", StringComparison.Ordinal)
            || text.Contains("refresh token", StringComparison.Ordinal)
            || text.Contains("oauth", StringComparison.Ordinal)
            || text.Contains("unauthorized", StringComparison.Ordinal)
            || text.Contains("http 401", StringComparison.Ordinal)
            || text.Contains("token refresh", StringComparison.Ordinal)
            || text.Contains("/integrations/zoho/connect", StringComparison.Ordinal)
            || text.Contains("tokens are missing", StringComparison.Ordinal);
    }

    private async Task<PlaceLeadSourceRow?> LoadPlaceAsync(string placeId, CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<PlaceLeadSourceRow>(new CommandDefinition(@"
SELECT TOP 1
  p.PlaceId,
  p.DisplayName,
  p.NationalPhoneNumber,
  p.WebsiteUri,
  p.FormattedAddress,
  p.OpeningDate,
  p.LogoUrl,
  p.MainPhotoUrl,
  p.FacebookUrl,
  p.InstagramUrl,
  p.LinkedInUrl,
  p.XUrl,
  p.YouTubeUrl,
  p.TikTokUrl,
  p.PinterestUrl,
  p.BlueskyUrl,
  p.ZohoLeadCreated,
  p.ZohoLeadId,
  pf.CompanyNumber,
  pf.DateOfCreation AS FinancialDateOfCreation
FROM dbo.Place p
LEFT JOIN dbo.PlacesFinancial pf ON pf.PlaceId = p.PlaceId
WHERE p.PlaceId = @PlaceId;", new { PlaceId = placeId }, cancellationToken: ct));
    }

    private async Task<ExistingLeadMatch?> FindExistingActiveLeadAsync(
        PlaceLeadSourceRow place,
        string? websiteHost,
        string localSeoLink,
        ZohoLeadFieldMap fieldMap,
        CancellationToken ct)
    {
        var normalizedCompanyName = NormalizeCompanyName(place.DisplayName);
        if (normalizedCompanyName is null)
            return null;

        var checks = new List<LeadDuplicateCheck>
        {
            new("Phone", place.NationalPhoneNumber, "phone number"),
            new("Company", place.DisplayName, "company name"),
            new("Website", websiteHost, "website host")
        };
        if (!string.IsNullOrWhiteSpace(fieldMap.LocalSeoLinkApiName))
            checks.Add(new(fieldMap.LocalSeoLinkApiName!, localSeoLink, "LocalSeoLink"));

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

    private static IReadOnlyList<string> BuildDuplicateCheckFields(string? phone, string? websiteHost, string? localSeoLinkApiName)
    {
        var fields = new List<string> { "Company" };
        var normalizedLocalSeoField = NormalizeNullable(localSeoLinkApiName);
        if (normalizedLocalSeoField is not null)
            fields.Add(normalizedLocalSeoField);
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
        ZohoLeadFieldMap fieldMap,
        string? websiteHost,
        string localSeoLink,
        string description,
        ParsedAddress address)
    {
        var established = ToZohoDateString(place.FinancialDateOfCreation ?? place.OpeningDate);
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
            ["Description"] = description,
        };
        AddMappedValue(payload, fieldMap.CompanyNumberApiName, NormalizeNullable(place.CompanyNumber));
        AddMappedValue(payload, fieldMap.EstablishedApiName, established);
        AddMappedValue(payload, fieldMap.NextActionApiName, NormalizeNullable(settings.ZohoLeadNextAction));
        AddMappedValue(payload, fieldMap.LocalSeoLinkApiName, localSeoLink);
        AddMappedValue(payload, fieldMap.LinkedInApiName, NormalizeNullable(place.LinkedInUrl));
        AddMappedValue(payload, fieldMap.FacebookApiName, NormalizeNullable(place.FacebookUrl));
        AddMappedValue(payload, fieldMap.InstagramApiName, NormalizeNullable(place.InstagramUrl));
        AddMappedValue(payload, fieldMap.YouTubeApiName, NormalizeNullable(place.YouTubeUrl));
        AddMappedValue(payload, fieldMap.TikTokApiName, NormalizeNullable(place.TikTokUrl));
        AddMappedValue(payload, fieldMap.PinterestApiName, NormalizeNullable(place.PinterestUrl));
        AddMappedValue(payload, fieldMap.XTwitterApiName, NormalizeNullable(place.XUrl));
        AddMappedValue(payload, fieldMap.BlueskyApiName, NormalizeNullable(place.BlueskyUrl));
        AddMappedValue(payload, fieldMap.LogoApiName, NormalizeNullable(place.LogoUrl));
        AddMappedValue(payload, fieldMap.MainPhotoApiName, NormalizeNullable(place.MainPhotoUrl));
        AddMappedValue(payload, fieldMap.CustomerOrPartnerApiName, "Customer");

        var ownerId = NormalizeNullable(settings.ZohoLeadOwnerId);
        if (!string.IsNullOrWhiteSpace(ownerId))
            payload["Owner"] = new Dictionary<string, object> { ["id"] = ownerId };

        return CompactPayload(payload);
    }

    private static Dictionary<string, object> BuildExistingLeadUpdatePayload(
        PlaceLeadSourceRow place,
        AdminSettingsModel settings,
        ZohoLeadFieldMap fieldMap,
        string localSeoLink,
        string description)
    {
        var address = ParseAddress(place.FormattedAddress);
        var websiteHost = NormalizeWebsiteHost(place.WebsiteUri);
        var established = ToZohoDateString(place.FinancialDateOfCreation ?? place.OpeningDate);
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Description"] = description,
            ["Opportunity"] = "Local SEO",
            ["Phone"] = NormalizeNullable(place.NationalPhoneNumber),
            ["Website"] = websiteHost,
            ["Street"] = address.Street1,
            ["Street2"] = address.Street2,
            ["Street3"] = address.Street3,
            ["City"] = address.City,
            ["State"] = address.Province,
            ["Province"] = address.Province,
            ["Zip_Code"] = address.PostalCode,
            ["Postalcode"] = address.PostalCode,
            ["Country"] = address.Country
        };
        AddMappedValue(payload, fieldMap.CompanyNumberApiName, NormalizeNullable(place.CompanyNumber));
        AddMappedValue(payload, fieldMap.EstablishedApiName, established);
        AddMappedValue(payload, fieldMap.LocalSeoLinkApiName, localSeoLink);
        AddMappedValue(payload, fieldMap.NextActionApiName, NormalizeNullable(settings.ZohoLeadNextAction));
        AddMappedValue(payload, fieldMap.LinkedInApiName, NormalizeNullable(place.LinkedInUrl));
        AddMappedValue(payload, fieldMap.FacebookApiName, NormalizeNullable(place.FacebookUrl));
        AddMappedValue(payload, fieldMap.InstagramApiName, NormalizeNullable(place.InstagramUrl));
        AddMappedValue(payload, fieldMap.YouTubeApiName, NormalizeNullable(place.YouTubeUrl));
        AddMappedValue(payload, fieldMap.TikTokApiName, NormalizeNullable(place.TikTokUrl));
        AddMappedValue(payload, fieldMap.PinterestApiName, NormalizeNullable(place.PinterestUrl));
        AddMappedValue(payload, fieldMap.XTwitterApiName, NormalizeNullable(place.XUrl));
        AddMappedValue(payload, fieldMap.BlueskyApiName, NormalizeNullable(place.BlueskyUrl));
        AddMappedValue(payload, fieldMap.LogoApiName, NormalizeNullable(place.LogoUrl));
        AddMappedValue(payload, fieldMap.MainPhotoApiName, NormalizeNullable(place.MainPhotoUrl));
        AddMappedValue(payload, fieldMap.CustomerOrPartnerApiName, "Customer");
        return CompactPayload(payload);
    }

    private async Task<ZohoLeadFieldResolution> ResolveLeadFieldMapAsync(string? knownLeadId, CancellationToken ct)
    {
        Exception? metadataException = null;
        try
        {
            var metadata = new List<ZohoLeadFieldMetadata>();
            const int perPage = 200;
            const int maxPages = 25;

            for (var page = 1; page <= maxPages; page++)
            {
                using var response = await zohoCrmClient.GetLeadFieldsAsync(page, perPage, ct);
                var apiError = TryGetZohoApiError(response.RootElement);
                if (!string.IsNullOrWhiteSpace(apiError))
                    throw new InvalidOperationException($"Zoho fields endpoint returned an API error: {apiError}");
                var pageFields = ExtractLeadFieldMetadata(response.RootElement);
                metadata.AddRange(pageFields);

                if (!HasMoreRecords(response.RootElement) || pageFields.Count == 0)
                    break;
            }

            var map = ParseLeadFieldMap(metadata);
            var sampleApiNames = metadata
                .Select(x => x.ApiName)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Take(20)
                .ToArray();
            var diagnostic = sampleApiNames.Length > 0
                ? $"Zoho returned {metadata.Count} lead fields. Sample API names: {string.Join(", ", sampleApiNames)}."
                : $"Zoho returned {metadata.Count} lead fields with no API names.";
            if (HasAnyRequestedFieldMapping(map))
                return new ZohoLeadFieldResolution(map, diagnostic);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            metadataException = ex;
            logger.LogWarning(ex, "Could not resolve Zoho Leads field metadata. Falling back to lead-schema inference.");
        }

        try
        {
            var inferredMetadata = await InferLeadFieldMetadataFromLeadSchemaAsync(knownLeadId, ct);
            var inferredMap = ParseLeadFieldMap(inferredMetadata);
            var sampleApiNames = inferredMetadata
                .Select(x => x.ApiName)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Take(20)
                .ToArray();
            var diagnostic = sampleApiNames.Length > 0
                ? $"Inferred from lead schema with {inferredMetadata.Count} fields. Sample API names: {string.Join(", ", sampleApiNames)}."
                : "Lead schema inference returned no fields.";
            if (HasAnyRequestedFieldMapping(inferredMap))
                return new ZohoLeadFieldResolution(inferredMap, diagnostic);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Could not infer Zoho lead fields from lead schema fallback.");
            if (metadataException is null)
                metadataException = ex;
        }

        var metadataMessage = metadataException?.Message ?? "No additional Zoho error details were returned.";
        return new ZohoLeadFieldResolution(
            ZohoLeadFieldMap.Default,
            $"Metadata lookup and schema inference did not resolve any target fields. Last error: {metadataMessage}");
    }

    private async Task<IReadOnlyList<ZohoLeadFieldMetadata>> InferLeadFieldMetadataFromLeadSchemaAsync(string? knownLeadId, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(knownLeadId))
        {
            using var byId = await zohoCrmClient.GetLeadByIdAsync(knownLeadId.Trim(), ct);
            var fromKnownLead = ExtractLeadSchemaMetadata(byId.RootElement);
            if (fromKnownLead.Count > 0)
                return fromKnownLead;
        }

        using var ping = await zohoCrmClient.PingAsync(ct);
        return ExtractLeadSchemaMetadata(ping.RootElement);
    }

    private static IReadOnlyList<ZohoLeadFieldMetadata> ExtractLeadSchemaMetadata(JsonElement root)
    {
        if (!TryGetFirstDataItem(root, out var item) || item.ValueKind != JsonValueKind.Object)
            return [];

        var metadata = new List<ZohoLeadFieldMetadata>();
        foreach (var prop in item.EnumerateObject())
        {
            if (string.IsNullOrWhiteSpace(prop.Name))
                continue;

            metadata.Add(new ZohoLeadFieldMetadata(prop.Name, prop.Name, prop.Name));
        }

        return metadata;
    }

    private static IReadOnlyList<ZohoLeadFieldMetadata> ExtractLeadFieldMetadata(JsonElement root)
    {
        if (!root.TryGetProperty("fields", out var fieldsNode) || fieldsNode.ValueKind != JsonValueKind.Array)
            return [];

        var metadata = new List<ZohoLeadFieldMetadata>();
        foreach (var field in fieldsNode.EnumerateArray())
        {
            var label = GetString(field, "field_label");
            var displayLabel = GetString(field, "display_label");
            var apiName = GetString(field, "api_name");
            if (string.IsNullOrWhiteSpace(apiName))
                continue;

            metadata.Add(new ZohoLeadFieldMetadata(label, displayLabel, apiName.Trim()));
        }

        return metadata;
    }

    private static bool HasMoreRecords(JsonElement root)
    {
        if (!root.TryGetProperty("info", out var info) || info.ValueKind != JsonValueKind.Object)
            return false;
        if (!info.TryGetProperty("more_records", out var moreRecords))
            return false;
        return moreRecords.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(moreRecords.GetString(), out var parsed) && parsed,
            _ => false
        };
    }

    private static string? TryGetZohoApiError(JsonElement root)
    {
        var code = GetString(root, "code");
        var message = GetString(root, "message");
        if (!string.IsNullOrWhiteSpace(code) || !string.IsNullOrWhiteSpace(message))
            return $"{code ?? "unknown_code"}: {message ?? "unknown_message"}";

        if (root.TryGetProperty("details", out var details) && details.ValueKind == JsonValueKind.Object)
        {
            var detailsMessage = details.ToString();
            if (!string.IsNullOrWhiteSpace(detailsMessage))
                return detailsMessage;
        }

        return null;
    }

    private static ZohoLeadFieldMap ParseLeadFieldMap(IReadOnlyList<ZohoLeadFieldMetadata> fields)
    {
        if (fields.Count == 0)
            return ZohoLeadFieldMap.Default;

        var byLabel = new Dictionary<string, string>(StringComparer.Ordinal);
        var byApiName = new Dictionary<string, string>(StringComparer.Ordinal);
        var searchable = new List<KeyValuePair<string, string>>();
        foreach (var field in fields)
        {
            var normalizedApi = NormalizeFieldToken(field.ApiName);
            if (normalizedApi.Length > 0 && !byApiName.ContainsKey(normalizedApi))
            {
                byApiName[normalizedApi] = field.ApiName;
                searchable.Add(new KeyValuePair<string, string>(normalizedApi, field.ApiName));
            }

            var normalizedLabel = NormalizeFieldToken(field.FieldLabel);
            if (normalizedLabel.Length > 0 && !byLabel.ContainsKey(normalizedLabel))
            {
                byLabel[normalizedLabel] = field.ApiName;
                searchable.Add(new KeyValuePair<string, string>(normalizedLabel, field.ApiName));
            }

            var normalizedDisplayLabel = NormalizeFieldToken(field.DisplayLabel);
            if (normalizedDisplayLabel.Length > 0 && !byLabel.ContainsKey(normalizedDisplayLabel))
            {
                byLabel[normalizedDisplayLabel] = field.ApiName;
                searchable.Add(new KeyValuePair<string, string>(normalizedDisplayLabel, field.ApiName));
            }
        }

        var companyNumber = ResolveApiNameOptional(byLabel, byApiName, "Company Number", "CompanyNumber", "Company_Number")
            ?? ResolveApiNameByRequiredTokens(searchable, "company", "number");
        var established = ResolveApiNameOptional(byLabel, byApiName, "Established", "Date Established", "Date_Established")
            ?? ResolveApiNameByRequiredTokens(searchable, "established")
            ?? ResolveApiNameByRequiredTokens(searchable, "date", "incorporated");
        var nextAction = ResolveApiNameOptional(byLabel, byApiName, "Next Action", "Next_Action", "NextAction")
            ?? ResolveApiNameByRequiredTokens(searchable, "next", "action");
        var linkedIn = ResolveApiNameOptional(byLabel, byApiName, "LinkedIn", "Company_LinkedIn", "LinkedIn_URL", "LinkedIn_Link")
            ?? ResolveApiNameByRequiredTokens(searchable, "linkedin");
        var facebook = ResolveApiNameOptional(byLabel, byApiName, "Facebook", "Company_Facebook", "Facebook_Page", "Facebook_URL", "Facebook_Link")
            ?? ResolveApiNameByRequiredTokens(searchable, "facebook");
        var instagram = ResolveApiNameOptional(byLabel, byApiName, "Instagram")
            ?? ResolveApiNameByRequiredTokens(searchable, "instagram");
        var youTube = ResolveApiNameOptional(byLabel, byApiName, "YouTube")
            ?? ResolveApiNameByRequiredTokens(searchable, "youtube");
        var tikTok = ResolveApiNameOptional(byLabel, byApiName, "TikTok")
            ?? ResolveApiNameByRequiredTokens(searchable, "tiktok");
        var pinterest = ResolveApiNameOptional(byLabel, byApiName, "Pinterest")
            ?? ResolveApiNameByRequiredTokens(searchable, "pinterest");
        var xTwitter = ResolveApiNameOptional(byLabel, byApiName, "X (Twitter)", "X Twitter", "X_Twitter", "Twitter")
            ?? ResolveApiNameByRequiredTokens(searchable, "twitter")
            ?? ResolveApiNameByRequiredTokens(searchable, "xtwitter");
        var bluesky = ResolveApiNameOptional(byLabel, byApiName, "Bluesky")
            ?? ResolveApiNameByRequiredTokens(searchable, "bluesky");
        var customerOrPartner = ResolveApiNameOptional(byLabel, byApiName, "Customer or Partner?", "Customer or Partner", "Customer_or_Partner", "Customer_or_Partner_")
            ?? ResolveApiNameByRequiredTokens(searchable, "customer", "partner");
        var localSeoLink = ResolveApiNameOptional(byLabel, byApiName, "LocalSeoLink", "Local Seo Link", "LocalSEO Link")
            ?? ResolveApiNameByRequiredTokens(searchable, "local", "seo", "link");
        var logo = ResolveApiNameOptional(byLabel, byApiName, "Logo", "Logo Url", "Company Logo", "Logo_URL")
            ?? ResolveApiNameByRequiredTokens(searchable, "logo");
        var mainPhoto = ResolveApiNameOptional(byLabel, byApiName, "Main Photo", "Main Photo Url", "Main_Image", "MainImage")
            ?? ResolveApiNameByRequiredTokens(searchable, "main", "photo");

        return new ZohoLeadFieldMap(
            companyNumber,
            established,
            nextAction,
            linkedIn,
            facebook,
            instagram,
            youTube,
            tikTok,
            pinterest,
            xTwitter,
            bluesky,
            customerOrPartner,
            localSeoLink,
            logo,
            mainPhoto);
    }

    private static string? ResolveApiNameOptional(
        IReadOnlyDictionary<string, string> byLabel,
        IReadOnlyDictionary<string, string> byApiName,
        params string[] candidates)
    {
        foreach (var candidate in candidates)
        {
            var normalized = NormalizeFieldToken(candidate);
            if (normalized.Length == 0)
                continue;
            if (byApiName.TryGetValue(normalized, out var apiFromName))
                return apiFromName;
            if (byLabel.TryGetValue(normalized, out var apiFromLabel))
                return apiFromLabel;
        }

        return null;
    }

    private static string? ResolveApiNameByRequiredTokens(
        IEnumerable<KeyValuePair<string, string>> searchable,
        params string[] requiredTokens)
    {
        var normalizedTokens = requiredTokens
            .Select(NormalizeFieldToken)
            .Where(x => x.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (normalizedTokens.Length == 0)
            return null;

        foreach (var pair in searchable)
        {
            if (normalizedTokens.All(token => pair.Key.Contains(token, StringComparison.Ordinal)))
                return pair.Value;
        }

        return null;
    }

    private static string NormalizeFieldToken(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var chars = value
            .Trim()
            .ToLowerInvariant()
            .Where(char.IsLetterOrDigit)
            .ToArray();
        return new string(chars);
    }

    private static bool HasAnyRequestedFieldMapping(ZohoLeadFieldMap fieldMap)
    {
        return !string.IsNullOrWhiteSpace(fieldMap.CompanyNumberApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.EstablishedApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.NextActionApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.CustomerOrPartnerApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.LocalSeoLinkApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.LinkedInApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.FacebookApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.InstagramApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.YouTubeApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.TikTokApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.PinterestApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.XTwitterApiName)
            || !string.IsNullOrWhiteSpace(fieldMap.BlueskyApiName);
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

    private static void AddMappedValue(IDictionary<string, object?> payload, string? apiName, object? value)
    {
        var normalizedApiName = NormalizeNullable(apiName);
        if (normalizedApiName is null)
            return;
        if (value is null)
            return;
        if (value is string text && string.IsNullOrWhiteSpace(text))
            return;

        payload[normalizedApiName] = value;
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

    private async Task UploadLeadPhotoFromLogoAsync(PlaceLeadSourceRow place, string leadId, CancellationToken ct)
    {
        var photo = await TryBuildLeadPhotoPayloadAsync(place.LogoUrl, ct);
        if (photo is null)
            return;

        try
        {
            await zohoCrmClient.UploadLeadPhotoAsync(leadId, photo.Content, photo.FileName, photo.ContentType, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Zoho lead photo upload failed for PlaceId={PlaceId} LeadId={LeadId}.", place.PlaceId, leadId);
        }
    }

    private async Task<LeadPhotoPayload?> TryBuildLeadPhotoPayloadAsync(string? logoUrl, CancellationToken ct)
    {
        var normalizedLogoUrl = NormalizeNullable(logoUrl);
        if (normalizedLogoUrl is null)
            return null;

        var localAssetPayload = await TryBuildLeadPhotoPayloadFromLocalAssetAsync(normalizedLogoUrl, ct);
        if (localAssetPayload is not null)
            return localAssetPayload;

        if (!Uri.TryCreate(normalizedLogoUrl, UriKind.Absolute, out var logoUri))
            return null;
        if (logoUri.Scheme != Uri.UriSchemeHttp && logoUri.Scheme != Uri.UriSchemeHttps)
            return null;

        var client = httpClientFactory.CreateClient();
        using var response = await client.GetAsync(logoUri, ct);
        if (!response.IsSuccessStatusCode)
        {
            logger.LogWarning(
                "Logo download failed for lead photo upload. Place logo URL={LogoUrl} StatusCode={StatusCode}",
                normalizedLogoUrl,
                (int)response.StatusCode);
            return null;
        }

        var contentType = response.Content.Headers.ContentType?.MediaType;
        if (string.IsNullOrWhiteSpace(contentType) || !contentType.StartsWith("image/", StringComparison.OrdinalIgnoreCase))
            contentType = "image/jpeg";

        var content = await response.Content.ReadAsByteArrayAsync(ct);
        if (content.Length == 0)
            return null;

        const int maxBytes = 8 * 1024 * 1024;
        if (content.Length > maxBytes)
        {
            logger.LogWarning("Skipping lead photo upload because logo exceeds max size. Bytes={ByteCount}", content.Length);
            return null;
        }

        var extension = GetImageExtension(contentType, logoUri.AbsolutePath);
        return new LeadPhotoPayload(content, $"company-logo{extension}", contentType);
    }

    private async Task<LeadPhotoPayload?> TryBuildLeadPhotoPayloadFromLocalAssetAsync(string logoUrl, CancellationToken ct)
    {
        var normalizedLocalPath = NormalizeLocalAssetPath(logoUrl);
        if (normalizedLocalPath is null)
            return null;

        var webRootPath = webHostEnvironment.WebRootPath;
        if (string.IsNullOrWhiteSpace(webRootPath) && !string.IsNullOrWhiteSpace(webHostEnvironment.ContentRootPath))
            webRootPath = Path.Combine(webHostEnvironment.ContentRootPath, "wwwroot");
        if (string.IsNullOrWhiteSpace(webRootPath))
            return null;

        var fullPath = Path.GetFullPath(Path.Combine(webRootPath, normalizedLocalPath.Replace('/', Path.DirectorySeparatorChar)));
        var fullWebRootPath = Path.GetFullPath(webRootPath);
        if (!fullPath.StartsWith(fullWebRootPath, StringComparison.OrdinalIgnoreCase))
            return null;
        if (!File.Exists(fullPath))
            return null;

        var content = await File.ReadAllBytesAsync(fullPath, ct);
        if (content.Length == 0)
            return null;

        const int maxBytes = 8 * 1024 * 1024;
        if (content.Length > maxBytes)
        {
            logger.LogWarning("Skipping lead photo upload because local logo exceeds max size. Path={Path} Bytes={ByteCount}", fullPath, content.Length);
            return null;
        }

        var extension = Path.GetExtension(fullPath);
        var contentType = GetContentTypeFromExtension(extension);
        return new LeadPhotoPayload(content, $"company-logo{NormalizeExtension(extension)}", contentType);
    }

    private static string? NormalizeLocalAssetPath(string value)
    {
        var normalized = (value ?? string.Empty).Trim().Replace('\\', '/');
        if (normalized.Length == 0)
            return null;
        if (normalized.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (normalized.StartsWith("~/", StringComparison.Ordinal))
            normalized = normalized[2..];
        if (normalized.StartsWith("/", StringComparison.Ordinal))
            normalized = normalized[1..];
        if (normalized.StartsWith("wwwroot/", StringComparison.OrdinalIgnoreCase))
            normalized = normalized["wwwroot/".Length..];

        if (normalized.StartsWith("site-assets/", StringComparison.OrdinalIgnoreCase))
            return normalized;

        return null;
    }

    private static string GetContentTypeFromExtension(string? extension)
    {
        var normalized = NormalizeExtension(extension);
        return normalized switch
        {
            ".png" => "image/png",
            ".gif" => "image/gif",
            ".webp" => "image/webp",
            ".svg" => "image/svg+xml",
            ".bmp" => "image/bmp",
            ".tif" => "image/tiff",
            ".tiff" => "image/tiff",
            ".jpg" => "image/jpeg",
            ".jpeg" => "image/jpeg",
            _ => "image/jpeg"
        };
    }

    private static string NormalizeExtension(string? extension)
    {
        var normalized = (extension ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(normalized))
            return ".jpg";
        if (!normalized.StartsWith(".", StringComparison.Ordinal))
            normalized = "." + normalized;
        if (normalized.Length > 10)
            return ".jpg";
        return normalized;
    }

    private static string BuildLocalSeoLink(string? siteUrl, string placeId, string? fallbackLocalSeoLink)
    {
        var normalizedSiteUrl = NormalizeNullable(siteUrl);
        if (normalizedSiteUrl is not null
            && Uri.TryCreate(normalizedSiteUrl, UriKind.Absolute, out var siteUri)
            && (siteUri.Scheme == Uri.UriSchemeHttp || siteUri.Scheme == Uri.UriSchemeHttps))
        {
            var baseUri = siteUri.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
                ? new Uri(siteUri.AbsoluteUri, UriKind.Absolute)
                : new Uri($"{siteUri.AbsoluteUri}/", UriKind.Absolute);
            var relative = $"places/{Uri.EscapeDataString(placeId)}";
            return new Uri(baseUri, relative).ToString();
        }

        return NormalizeLocalSeoLink(fallbackLocalSeoLink ?? string.Empty, placeId);
    }

    private static string NormalizeLocalSeoLink(string localSeoLink, string placeId)
    {
        var normalized = (localSeoLink ?? string.Empty).Trim();
        if (normalized.Length > 0)
            return normalized;

        return $"/places/{Uri.EscapeDataString(placeId)}";
    }

    private static string GetImageExtension(string contentType, string? absolutePath)
    {
        var normalized = (contentType ?? string.Empty).Trim().ToLowerInvariant();
        if (normalized == "image/png")
            return ".png";
        if (normalized == "image/gif")
            return ".gif";
        if (normalized == "image/webp")
            return ".webp";
        if (normalized == "image/svg+xml")
            return ".svg";
        if (normalized == "image/bmp")
            return ".bmp";
        if (normalized == "image/tiff")
            return ".tif";
        if (normalized is "image/jpg" or "image/jpeg")
            return ".jpg";

        var extension = Path.GetExtension(absolutePath ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(extension)
            && extension.Length <= 10
            && extension.StartsWith(".", StringComparison.Ordinal))
        {
            return extension.ToLowerInvariant();
        }

        return ".jpg";
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

    private static string? ToZohoDateString(DateTime? value)
    {
        if (!value.HasValue)
            return null;
        return value.Value.ToString("yyyy-MM-dd");
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

    private sealed class PlaceLeadSourceRow
    {
        public string PlaceId { get; set; } = string.Empty;
        public string? DisplayName { get; set; }
        public string? NationalPhoneNumber { get; set; }
        public string? WebsiteUri { get; set; }
        public string? FormattedAddress { get; set; }
        public DateTime? OpeningDate { get; set; }
        public string? LogoUrl { get; set; }
        public string? MainPhotoUrl { get; set; }
        public string? FacebookUrl { get; set; }
        public string? InstagramUrl { get; set; }
        public string? LinkedInUrl { get; set; }
        public string? XUrl { get; set; }
        public string? YouTubeUrl { get; set; }
        public string? TikTokUrl { get; set; }
        public string? PinterestUrl { get; set; }
        public string? BlueskyUrl { get; set; }
        public bool ZohoLeadCreated { get; set; }
        public string? ZohoLeadId { get; set; }
        public string? CompanyNumber { get; set; }
        public DateTime? FinancialDateOfCreation { get; set; }
    }

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
    private sealed record LeadPhotoPayload(
        byte[] Content,
        string FileName,
        string ContentType);
    private sealed record ZohoLeadFieldMetadata(
        string? FieldLabel,
        string? DisplayLabel,
        string ApiName);
    private sealed record ZohoLeadFieldResolution(
        ZohoLeadFieldMap Map,
        string Diagnostic);
    private sealed record ZohoLeadFieldMap(
        string? CompanyNumberApiName,
        string? EstablishedApiName,
        string? NextActionApiName,
        string? LinkedInApiName,
        string? FacebookApiName,
        string? InstagramApiName,
        string? YouTubeApiName,
        string? TikTokApiName,
        string? PinterestApiName,
        string? XTwitterApiName,
        string? BlueskyApiName,
        string? CustomerOrPartnerApiName,
        string? LocalSeoLinkApiName,
        string? LogoApiName,
        string? MainPhotoApiName)
    {
        public static ZohoLeadFieldMap Default { get; } = new(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
    private sealed record ZohoMutationResult(string? LeadId, string? Action);
}
