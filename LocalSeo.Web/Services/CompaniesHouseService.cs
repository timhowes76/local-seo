using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Diagnostics;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ICompaniesHouseService
{
    Task<IReadOnlyList<CompaniesHouseCompanySearchResult>> SearchCompaniesAsync(string? query, string? location, CancellationToken ct);
    Task<CompaniesHouseCompanyProfile?> GetCompanyProfileAsync(string companyNumber, CancellationToken ct);
    Task<IReadOnlyList<CompaniesHouseOfficer>> GetCompanyOfficersAsync(string companyNumber, CancellationToken ct);
    Task<IReadOnlyList<CompaniesHousePersonWithSignificantControl>> GetCompanyPersonsWithSignificantControlAsync(string companyNumber, string? placeId, CancellationToken ct);
}

public sealed class CompaniesHouseService(
    HttpClient httpClient,
    IOptions<CompaniesHouseOptions> options,
    ILogger<CompaniesHouseService> logger) : ICompaniesHouseService
{
    public async Task<IReadOnlyList<CompaniesHouseCompanySearchResult>> SearchCompaniesAsync(string? query, string? location, CancellationToken ct)
    {
        var normalizedQuery = Normalize(query);
        var normalizedLocation = Normalize(location);
        if (normalizedQuery is null && normalizedLocation is null)
            return [];

        var primaryQuery = normalizedQuery ?? normalizedLocation!;
        var merged = new Dictionary<string, CompaniesHouseCompanySearchResult>(StringComparer.OrdinalIgnoreCase);

        async Task QueryAndMergeAsync(string searchTerm)
        {
            var body = await SendGetAsync($"/search/companies?q={Uri.EscapeDataString(searchTerm)}", ct);
            if (body is null)
                return;

            foreach (var result in ParseCompanySearchResults(body))
            {
                if (!merged.ContainsKey(result.CompanyNumber))
                    merged[result.CompanyNumber] = result;
            }
        }

        await QueryAndMergeAsync(primaryQuery);
        if (normalizedQuery is not null && normalizedLocation is not null)
        {
            var combinedQuery = $"{normalizedQuery} {normalizedLocation}";
            if (!string.Equals(combinedQuery, primaryQuery, StringComparison.OrdinalIgnoreCase))
                await QueryAndMergeAsync(combinedQuery);
        }

        var results = merged.Values.ToList();
        if (normalizedLocation is null)
            return results;

        var locationTokens = normalizedLocation
            .Split([' ', ',', '.', ';', ':', '/', '\\', '-', '(', ')'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(token => token.Length >= 2)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        return results
            .OrderByDescending(x => ScoreLocationMatch(x, normalizedLocation, locationTokens))
            .ThenBy(x => x.Title, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static IReadOnlyList<CompaniesHouseCompanySearchResult> ParseCompanySearchResults(string body)
    {
        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("items", out var items) || items.ValueKind != JsonValueKind.Array)
            return [];

        var results = new List<CompaniesHouseCompanySearchResult>();
        foreach (var item in items.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
                continue;

            var companyNumber = Normalize(GetString(item, "company_number"));
            if (companyNumber is null)
                continue;

            var title = Normalize(GetString(item, "title")) ?? companyNumber;
            var address = Normalize(GetString(item, "address_snippet"))
                ?? BuildAddressFromObject(item);
            var dateOfCreation = ParseDate(GetString(item, "date_of_creation"));
            var companyType = Normalize(GetString(item, "company_type"));

            results.Add(new CompaniesHouseCompanySearchResult(
                title,
                address ?? "N/A",
                dateOfCreation,
                companyNumber,
                companyType));
        }

        return results;
    }

    private static int ScoreLocationMatch(
        CompaniesHouseCompanySearchResult result,
        string normalizedLocation,
        IReadOnlyList<string> locationTokens)
    {
        var address = Normalize(result.Address) ?? string.Empty;
        var title = Normalize(result.Title) ?? string.Empty;
        var score = 0;

        if (ContainsOrdinalIgnoreCase(address, normalizedLocation))
            score += 200;
        if (ContainsOrdinalIgnoreCase(title, normalizedLocation))
            score += 80;

        foreach (var token in locationTokens)
        {
            if (ContainsOrdinalIgnoreCase(address, token))
                score += 20;
            if (ContainsOrdinalIgnoreCase(title, token))
                score += 6;
        }

        return score;
    }

    private static bool ContainsOrdinalIgnoreCase(string haystack, string needle)
    {
        if (haystack.Length == 0 || needle.Length == 0)
            return false;
        return haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public async Task<CompaniesHouseCompanyProfile?> GetCompanyProfileAsync(string companyNumber, CancellationToken ct)
    {
        var normalizedCompanyNumber = Normalize(companyNumber);
        if (normalizedCompanyNumber is null)
            throw new InvalidOperationException("Company number is required.");

        var body = await SendGetAsync($"/company/{Uri.EscapeDataString(normalizedCompanyNumber)}", ct, returnNullOnNotFound: true);
        if (body is null)
            return null;

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        DateTime? lastAccountsFiled = null;
        DateTime? nextAccountsDue = null;
        if (root.TryGetProperty("accounts", out var accounts) && accounts.ValueKind == JsonValueKind.Object)
        {
            nextAccountsDue = GetDate(accounts, "next_due");
            if (!nextAccountsDue.HasValue
                && accounts.TryGetProperty("next_accounts", out var nextAccounts)
                && nextAccounts.ValueKind == JsonValueKind.Object)
            {
                nextAccountsDue = GetDate(nextAccounts, "due_on");
            }

            if (accounts.TryGetProperty("last_accounts", out var lastAccounts) && lastAccounts.ValueKind == JsonValueKind.Object)
            {
                lastAccountsFiled = GetDate(lastAccounts, "made_up_to")
                    ?? GetDate(lastAccounts, "period_end_on");
            }
        }

        return new CompaniesHouseCompanyProfile(
            normalizedCompanyNumber,
            GetDate(root, "date_of_creation"),
            Normalize(GetString(root, "type")),
            lastAccountsFiled,
            nextAccountsDue,
            Normalize(GetString(root, "company_status")),
            GetBool(root, "has_been_liquidated"),
            GetBool(root, "has_charges"),
            GetBool(root, "has_insolvency_history"));
    }

    public async Task<IReadOnlyList<CompaniesHouseOfficer>> GetCompanyOfficersAsync(string companyNumber, CancellationToken ct)
    {
        var normalizedCompanyNumber = Normalize(companyNumber);
        if (normalizedCompanyNumber is null)
            throw new InvalidOperationException("Company number is required.");

        var encodedCompanyNumber = Uri.EscapeDataString(normalizedCompanyNumber);
        const int pageSize = 100;
        var startIndex = 0;
        var officers = new List<CompaniesHouseOfficer>();

        while (true)
        {
            var body = await SendGetAsync(
                $"/company/{encodedCompanyNumber}/officers?items_per_page={pageSize}&start_index={startIndex}",
                ct,
                returnNullOnNotFound: true);
            if (body is null)
                return [];

            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (!root.TryGetProperty("items", out var items) || items.ValueKind != JsonValueKind.Array)
                break;

            var pageItemCount = 0;
            foreach (var item in items.EnumerateArray())
            {
                pageItemCount++;
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                var parsed = ParseOfficer(item);
                if (parsed is null)
                    continue;

                officers.Add(parsed);
            }

            if (pageItemCount <= 0)
                break;

            var totalResults = GetInt(root, "total_results");
            startIndex += pageItemCount;

            if (totalResults.HasValue && startIndex >= totalResults.Value)
                break;
            if (pageItemCount < pageSize)
                break;
            if (startIndex > 5000)
                break;
        }

        return officers;
    }

    public async Task<IReadOnlyList<CompaniesHousePersonWithSignificantControl>> GetCompanyPersonsWithSignificantControlAsync(string companyNumber, string? placeId, CancellationToken ct)
    {
        var normalizedCompanyNumber = Normalize(companyNumber);
        if (normalizedCompanyNumber is null)
            throw new InvalidOperationException("Company number is required.");

        var encodedCompanyNumber = Uri.EscapeDataString(normalizedCompanyNumber);
        const int pageSize = 100;
        var startIndex = 0;
        var items = new List<CompaniesHousePersonWithSignificantControl>();
        var callStopwatch = Stopwatch.StartNew();
        var lastStatusCode = 200;
        try
        {
            while (true)
            {
                var response = await SendGetDetailedAsync(
                    $"/company/{encodedCompanyNumber}/persons-with-significant-control?items_per_page={pageSize}&start_index={startIndex}",
                    ct,
                    returnNullOnNotFound: true);
                lastStatusCode = (int)response.StatusCode;
                if (response.Body is null)
                {
                    logger.LogInformation(
                        "Companies House PSC fetch returned no content. PlaceId={PlaceId} CompanyNumber={CompanyNumber} HttpStatus={HttpStatus} DurationMs={DurationMs}",
                        placeId,
                        normalizedCompanyNumber,
                        lastStatusCode,
                        callStopwatch.ElapsedMilliseconds);
                    return [];
                }

                using var doc = JsonDocument.Parse(response.Body);
                var root = doc.RootElement;
                if (!root.TryGetProperty("items", out var pscItems) || pscItems.ValueKind != JsonValueKind.Array)
                    break;

                var rootEtag = Normalize(GetString(root, "etag"));
                var pageItemCount = 0;
                foreach (var item in pscItems.EnumerateArray())
                {
                    pageItemCount++;
                    if (item.ValueKind != JsonValueKind.Object)
                        continue;

                    var parsed = ParsePscItem(normalizedCompanyNumber, item, rootEtag);
                    if (parsed is null)
                        continue;

                    items.Add(parsed);
                }

                if (pageItemCount <= 0)
                    break;

                var totalResults = GetInt(root, "total_results");
                startIndex += pageItemCount;

                if (totalResults.HasValue && startIndex >= totalResults.Value)
                    break;
                if (pageItemCount < pageSize)
                    break;
                if (startIndex > 5000)
                    break;
            }

            logger.LogInformation(
                "Companies House PSC fetch completed. PlaceId={PlaceId} CompanyNumber={CompanyNumber} HttpStatus={HttpStatus} DurationMs={DurationMs} ItemCount={ItemCount}",
                placeId,
                normalizedCompanyNumber,
                lastStatusCode,
                callStopwatch.ElapsedMilliseconds,
                items.Count);
            return items;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(
                ex,
                "Companies House PSC fetch failed. PlaceId={PlaceId} CompanyNumber={CompanyNumber} LastHttpStatus={HttpStatus} DurationMs={DurationMs}",
                placeId,
                normalizedCompanyNumber,
                lastStatusCode,
                callStopwatch.ElapsedMilliseconds);
            throw;
        }
    }

    private static string NormalizeBaseUrl(string? baseUrl)
    {
        var normalized = (baseUrl ?? string.Empty).Trim().TrimEnd('/');
        if (normalized.Length == 0)
            return "https://api.company-information.service.gov.uk";
        return normalized;
    }

    private async Task<string?> SendGetAsync(string pathAndQuery, CancellationToken ct, bool returnNullOnNotFound = false)
    {
        var response = await SendGetDetailedAsync(pathAndQuery, ct, returnNullOnNotFound);
        return response.Body;
    }

    private async Task<CompaniesHouseGetResponse> SendGetDetailedAsync(string pathAndQuery, CancellationToken ct, bool returnNullOnNotFound = false)
    {
        var cfg = options.Value;
        var apiKey = (cfg.ApiKey ?? string.Empty).Trim();
        if (apiKey.Length == 0)
            throw new InvalidOperationException("Companies House API key is not configured.");

        var baseUrl = NormalizeBaseUrl(cfg.BaseUrl);
        var requestUrl = $"{baseUrl}/{pathAndQuery.TrimStart('/')}";

        using var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
        var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var stopwatch = Stopwatch.StartNew();
        using var response = await httpClient.SendAsync(request, ct);
        stopwatch.Stop();
        var body = await response.Content.ReadAsStringAsync(ct);

        logger.LogDebug(
            "Companies House GET {PathAndQuery} responded {HttpStatus} in {DurationMs} ms.",
            pathAndQuery,
            (int)response.StatusCode,
            stopwatch.ElapsedMilliseconds);

        if (returnNullOnNotFound && response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return new CompaniesHouseGetResponse(response.StatusCode, null, stopwatch.Elapsed);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Companies House request failed with HTTP {(int)response.StatusCode}.");

        return new CompaniesHouseGetResponse(response.StatusCode, body, stopwatch.Elapsed);
    }

    private static string? GetString(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
            return null;

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private static DateTime? ParseDate(string? value)
    {
        var normalized = Normalize(value);
        if (normalized is null)
            return null;

        if (DateTime.TryParseExact(normalized, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            return parsed.Date;

        return null;
    }

    private static DateTime? GetDate(JsonElement obj, string propertyName)
    {
        var raw = GetString(obj, propertyName);
        return ParseDate(raw);
    }

    private static int? GetInt(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
            return null;

        if (value.TryGetInt32(out var intValue))
            return intValue;
        if (int.TryParse(value.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            return parsed;

        return null;
    }

    private static bool GetBool(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
            return false;

        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(value.GetString(), out var parsed) && parsed,
            JsonValueKind.Number => value.TryGetInt32(out var intValue) && intValue != 0,
            _ => false
        };
    }

    private static CompaniesHouseOfficer? ParseOfficer(JsonElement item)
    {
        var rawName = Normalize(GetString(item, "name"));
        var (firstNames, lastName) = ParseOfficerName(rawName);
        var countryOfResidence = ToNameCase(Normalize(GetString(item, "country_of_residence")));
        var dateOfBirth = ParseOfficerDateOfBirth(item);
        var nationality = ToNameCase(Normalize(GetString(item, "nationality")));
        var role = ToNameCase(Normalize(GetString(item, "officer_role")));
        var appointed = ParseDate(GetString(item, "appointed_on"));
        var resigned = ParseDate(GetString(item, "resigned_on"));

        if (firstNames is null
            && lastName is null
            && countryOfResidence is null
            && !dateOfBirth.HasValue
            && nationality is null
            && role is null
            && !appointed.HasValue
            && !resigned.HasValue)
        {
            return null;
        }

        return new CompaniesHouseOfficer(
            firstNames,
            lastName,
            countryOfResidence,
            dateOfBirth,
            nationality,
            role,
            appointed,
            resigned);
    }

    private static CompaniesHousePersonWithSignificantControl? ParsePscItem(string companyNumber, JsonElement item, string? rootEtag)
    {
        var kind = Normalize(GetString(item, "kind"));
        var linkSelf = Normalize(GetPscSelfLink(item));
        var pscId = ExtractPscIdFromSelfLink(linkSelf);

        var rawName = Normalize(GetString(item, "name"));
        var (firstNamesFromRaw, lastNameFromRaw) = ParseOfficerName(rawName);
        var firstNames = Normalize(ParsePscFirstNames(item)) ?? firstNamesFromRaw;
        var lastName = Normalize(ParsePscLastName(item)) ?? lastNameFromRaw;
        if (firstNames is not null)
            firstNames = ToNameCase(firstNames);
        if (lastName is not null)
            lastName = ToNameCase(lastName);

        var countryOfResidence = ToNameCase(Normalize(GetString(item, "country_of_residence")));
        var nationality = ToNameCase(Normalize(GetString(item, "nationality")));
        var (birthMonth, birthYear) = ParsePscMonthYearOfBirth(item);
        var notifiedOn = ParseDate(GetString(item, "notified_on"));
        var ceasedOn = ParseDate(GetString(item, "ceased_on"));
        var sourceEtag = Normalize(GetString(item, "etag")) ?? rootEtag;
        var retrievedUtc = DateTime.UtcNow;
        var rawJson = item.GetRawText();
        var natureCodes = ParseNatureCodes(item);

        if (kind is null
            && linkSelf is null
            && pscId is null
            && rawName is null
            && firstNames is null
            && lastName is null
            && countryOfResidence is null
            && nationality is null
            && !birthMonth.HasValue
            && !birthYear.HasValue
            && !notifiedOn.HasValue
            && !ceasedOn.HasValue
            && sourceEtag is null
            && natureCodes.Count == 0)
        {
            return null;
        }

        return new CompaniesHousePersonWithSignificantControl(
            companyNumber,
            kind,
            linkSelf,
            pscId,
            rawName,
            firstNames,
            lastName,
            countryOfResidence,
            nationality,
            birthMonth,
            birthYear,
            notifiedOn,
            ceasedOn,
            sourceEtag,
            retrievedUtc,
            rawJson,
            natureCodes);
    }

    private static (string? FirstNames, string? LastName) ParseOfficerName(string? name)
    {
        var normalized = Normalize(name);
        if (normalized is null)
            return (null, null);

        var commaIndex = normalized.IndexOf(',');
        if (commaIndex < 0)
        {
            var tokens = normalized
                .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (tokens.Length == 0)
                return (null, null);
            if (tokens.Length == 1)
                return (ToNameCase(tokens[0]), null);

            var lastNameToken = tokens[^1];
            var firstNameTokens = tokens[..^1];
            return (ToNameCase(string.Join(" ", firstNameTokens)), ToNameCase(lastNameToken));
        }

        var left = normalized[..commaIndex];
        var right = normalized[(commaIndex + 1)..];
        return (ToNameCase(left), ToNameCase(right));
    }

    private static DateTime? ParseOfficerDateOfBirth(JsonElement item)
    {
        if (!item.TryGetProperty("date_of_birth", out var dateOfBirth) || dateOfBirth.ValueKind != JsonValueKind.Object)
            return null;

        var year = GetInt(dateOfBirth, "year");
        if (!year.HasValue || year.Value < 1 || year.Value > 9999)
            return null;

        var month = GetInt(dateOfBirth, "month") ?? 1;
        month = Math.Clamp(month, 1, 12);
        return new DateTime(year.Value, month, 1);
    }

    private static (byte? Month, int? Year) ParsePscMonthYearOfBirth(JsonElement item)
    {
        if (!item.TryGetProperty("date_of_birth", out var dateOfBirth) || dateOfBirth.ValueKind != JsonValueKind.Object)
            return (null, null);

        var year = GetInt(dateOfBirth, "year");
        if (year is < 1 or > 9999)
            year = null;

        var month = GetInt(dateOfBirth, "month");
        if (month is < 1 or > 12)
            month = null;

        return (month.HasValue ? (byte?)month.Value : null, year);
    }

    private static string? ParsePscFirstNames(JsonElement item)
    {
        if (!item.TryGetProperty("name_elements", out var nameElements) || nameElements.ValueKind != JsonValueKind.Object)
            return null;

        var tokens = new List<string>();
        var forename = Normalize(GetString(nameElements, "forename"));
        if (forename is not null)
            tokens.Add(forename);
        var middleName = Normalize(GetString(nameElements, "middle_name"));
        if (middleName is not null)
            tokens.Add(middleName);
        var otherForenames = Normalize(GetString(nameElements, "other_forenames"));
        if (otherForenames is not null)
            tokens.Add(otherForenames);

        if (tokens.Count == 0)
            return null;
        return string.Join(" ", tokens);
    }

    private static string? ParsePscLastName(JsonElement item)
    {
        if (!item.TryGetProperty("name_elements", out var nameElements) || nameElements.ValueKind != JsonValueKind.Object)
            return null;
        return Normalize(GetString(nameElements, "surname"));
    }

    private static string? GetPscSelfLink(JsonElement item)
    {
        if (!item.TryGetProperty("links", out var links) || links.ValueKind != JsonValueKind.Object)
            return null;
        return GetString(links, "self");
    }

    private static string? ExtractPscIdFromSelfLink(string? selfLink)
    {
        var normalized = Normalize(selfLink);
        if (normalized is null)
            return null;

        var withoutQuery = normalized.Split('?', 2)[0].TrimEnd('/');
        var lastSlashIndex = withoutQuery.LastIndexOf('/');
        if (lastSlashIndex < 0 || lastSlashIndex >= withoutQuery.Length - 1)
            return null;
        return withoutQuery[(lastSlashIndex + 1)..];
    }

    private static IReadOnlyList<string> ParseNatureCodes(JsonElement item)
    {
        if (!item.TryGetProperty("natures_of_control", out var natures) || natures.ValueKind != JsonValueKind.Array)
            return [];

        var values = new List<string>();
        foreach (var nature in natures.EnumerateArray())
        {
            if (nature.ValueKind != JsonValueKind.String)
                continue;

            var normalized = Normalize(nature.GetString());
            if (normalized is null)
                continue;

            values.Add(normalized);
        }

        return values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string? ToNameCase(string? value)
    {
        var normalized = Normalize(value);
        if (normalized is null)
            return null;

        var lower = normalized.ToLowerInvariant();
        return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(lower);
    }

    private static string? BuildAddressFromObject(JsonElement item)
    {
        if (!item.TryGetProperty("address", out var address) || address.ValueKind != JsonValueKind.Object)
            return null;

        var parts = new[]
        {
            GetString(address, "premises"),
            GetString(address, "address_line_1"),
            GetString(address, "address_line_2"),
            GetString(address, "locality"),
            GetString(address, "region"),
            GetString(address, "postal_code"),
            GetString(address, "country")
        };

        var normalizedParts = parts
            .Select(Normalize)
            .Where(x => x is not null)
            .Cast<string>()
            .ToList();
        if (normalizedParts.Count == 0)
            return null;

        return string.Join(", ", normalizedParts);
    }

    private static string? Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Trim();
    }

    private sealed record CompaniesHouseGetResponse(
        System.Net.HttpStatusCode StatusCode,
        string? Body,
        TimeSpan Duration);
}
