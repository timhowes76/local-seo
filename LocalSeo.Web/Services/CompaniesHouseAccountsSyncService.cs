using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Options;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface ICompaniesHouseAccountsSyncService
{
    Task<PlaceFinancialAccountsSyncResult> SyncAccountsAsync(string placeId, string companyNumber, CancellationToken ct);
}

public sealed record PlaceFinancialAccountsSyncResult(
    int FilingItemsSeen,
    int Inserted,
    int Updated,
    int Downloaded,
    int FailedDownloads);

public sealed class CompaniesHouseAccountsSyncService(
    ISqlConnectionFactory connectionFactory,
    IWebHostEnvironment webHostEnvironment,
    IOptions<CompaniesHouseOptions> companiesHouseOptions,
    HttpClient httpClient,
    ILogger<CompaniesHouseAccountsSyncService> logger) : ICompaniesHouseAccountsSyncService
{
    private const int PageSize = 100;
    private const int MaxRetryAttempts = 3;

    public async Task<PlaceFinancialAccountsSyncResult> SyncAccountsAsync(string placeId, string companyNumber, CancellationToken ct)
    {
        var normalizedPlaceId = Normalize(placeId);
        if (normalizedPlaceId is null)
            throw new InvalidOperationException("PlaceId is required.");

        var normalizedCompanyNumber = Normalize(companyNumber);
        if (normalizedCompanyNumber is null)
            throw new InvalidOperationException("Company number is required.");

        var filings = await FetchAccountsFilingsAsync(normalizedCompanyNumber, ct);
        var filingItemsSeen = filings.Count;
        var inserted = 0;
        var updated = 0;
        var downloaded = 0;
        var failedDownloads = 0;

        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var placeExists = await conn.ExecuteScalarAsync<int>(new CommandDefinition(@"
SELECT COUNT(1)
FROM dbo.Place
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct));
        if (placeExists <= 0)
            return new PlaceFinancialAccountsSyncResult(filingItemsSeen, inserted, updated, downloaded, failedDownloads);

        var existingRows = (await conn.QueryAsync<ExistingAccountRow>(new CommandDefinition(@"
SELECT
  Id,
  DocumentId,
  LocalRelativePath,
  ContentType,
  OriginalFileName,
  FileSizeBytes
FROM dbo.PlaceFinancialAccounts
WHERE PlaceId = @PlaceId;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct)))
            .ToDictionary(x => x.DocumentId, StringComparer.OrdinalIgnoreCase);

        foreach (var filing in filings)
        {
            if (string.IsNullOrWhiteSpace(filing.DocumentId))
                continue;

            try
            {
                existingRows.TryGetValue(filing.DocumentId, out var existing);
                var shouldDownload = existing is null || !HasLocalFile(existing.LocalRelativePath);
                DownloadedAccountFile? downloadedFile = null;

                if (shouldDownload)
                {
                    downloadedFile = await DownloadAccountFileAsync(
                        normalizedCompanyNumber,
                        filing.DocumentId,
                        filing.MadeUpDate,
                        filing.FilingDate,
                        ct);
                    if (downloadedFile is null)
                    {
                        failedDownloads++;
                        if (existing is null)
                        {
                            logger.LogWarning(
                                "Skipping new accounts row because download failed. PlaceId={PlaceId} CompanyNumber={CompanyNumber} DocumentId={DocumentId}",
                                normalizedPlaceId,
                                normalizedCompanyNumber,
                                filing.DocumentId);
                            continue;
                        }
                    }
                    else
                    {
                        downloaded++;
                    }
                }

                var persistedContentType = downloadedFile?.ContentType ?? existing?.ContentType;
                var persistedOriginalFileName = downloadedFile?.OriginalFileName ?? existing?.OriginalFileName;
                var persistedLocalRelativePath = downloadedFile?.LocalRelativePath ?? existing?.LocalRelativePath;
                var persistedFileSizeBytes = downloadedFile?.FileSizeBytes ?? existing?.FileSizeBytes;
                if (persistedLocalRelativePath is null)
                    continue;

                if (existing is null)
                {
                    await conn.ExecuteAsync(new CommandDefinition(@"
INSERT INTO dbo.PlaceFinancialAccounts(
  PlaceId,
  CompanyNumber,
  TransactionId,
  FilingDate,
  MadeUpDate,
  AccountsType,
  DocumentId,
  DocumentMetadataUrl,
  ContentType,
  OriginalFileName,
  LocalRelativePath,
  FileSizeBytes,
  RetrievedUtc,
  IsLatest,
  RawJson
)
VALUES(
  @PlaceId,
  @CompanyNumber,
  @TransactionId,
  @FilingDate,
  @MadeUpDate,
  @AccountsType,
  @DocumentId,
  @DocumentMetadataUrl,
  @ContentType,
  @OriginalFileName,
  @LocalRelativePath,
  @FileSizeBytes,
  @RetrievedUtc,
  0,
  @RawJson
);", new
                    {
                        PlaceId = normalizedPlaceId,
                        CompanyNumber = normalizedCompanyNumber,
                        filing.TransactionId,
                        filing.FilingDate,
                        filing.MadeUpDate,
                        filing.AccountsType,
                        filing.DocumentId,
                        filing.DocumentMetadataUrl,
                        ContentType = persistedContentType,
                        OriginalFileName = persistedOriginalFileName,
                        LocalRelativePath = persistedLocalRelativePath,
                        FileSizeBytes = persistedFileSizeBytes,
                        RetrievedUtc = DateTime.UtcNow,
                        filing.RawJson
                    }, cancellationToken: ct));
                    inserted++;
                }
                else
                {
                    await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceFinancialAccounts
SET
  CompanyNumber = @CompanyNumber,
  TransactionId = @TransactionId,
  FilingDate = @FilingDate,
  MadeUpDate = @MadeUpDate,
  AccountsType = @AccountsType,
  DocumentMetadataUrl = @DocumentMetadataUrl,
  ContentType = @ContentType,
  OriginalFileName = @OriginalFileName,
  LocalRelativePath = @LocalRelativePath,
  FileSizeBytes = @FileSizeBytes,
  RetrievedUtc = @RetrievedUtc,
  RawJson = @RawJson
WHERE Id = @Id;", new
                    {
                        existing.Id,
                        CompanyNumber = normalizedCompanyNumber,
                        filing.TransactionId,
                        filing.FilingDate,
                        filing.MadeUpDate,
                        filing.AccountsType,
                        filing.DocumentMetadataUrl,
                        ContentType = persistedContentType,
                        OriginalFileName = persistedOriginalFileName,
                        LocalRelativePath = persistedLocalRelativePath,
                        FileSizeBytes = persistedFileSizeBytes,
                        RetrievedUtc = DateTime.UtcNow,
                        filing.RawJson
                    }, cancellationToken: ct));
                    updated++;
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(
                    ex,
                    "Failed to process account filing item. PlaceId={PlaceId} CompanyNumber={CompanyNumber} DocumentId={DocumentId}",
                    normalizedPlaceId,
                    normalizedCompanyNumber,
                    filing.DocumentId);
            }
        }

        await conn.ExecuteAsync(new CommandDefinition(@"
UPDATE dbo.PlaceFinancialAccounts
SET IsLatest = 0
WHERE PlaceId = @PlaceId;

;WITH ranked AS (
  SELECT
    Id,
    ROW_NUMBER() OVER (
      ORDER BY
        CASE WHEN MadeUpDate IS NULL THEN 1 ELSE 0 END,
        MadeUpDate DESC,
        CASE WHEN FilingDate IS NULL THEN 1 ELSE 0 END,
        FilingDate DESC,
        RetrievedUtc DESC,
        Id DESC
    ) AS rn
  FROM dbo.PlaceFinancialAccounts
  WHERE PlaceId = @PlaceId
)
UPDATE a
SET IsLatest = 1
FROM dbo.PlaceFinancialAccounts a
JOIN ranked r ON r.Id = a.Id
WHERE r.rn = 1;", new { PlaceId = normalizedPlaceId }, cancellationToken: ct));

        return new PlaceFinancialAccountsSyncResult(filingItemsSeen, inserted, updated, downloaded, failedDownloads);
    }

    private async Task<IReadOnlyList<AccountsFilingItem>> FetchAccountsFilingsAsync(string companyNumber, CancellationToken ct)
    {
        var allItems = new List<AccountsFilingItem>();
        var encodedCompanyNumber = Uri.EscapeDataString(companyNumber);
        var baseUrl = NormalizeBaseUrl(companiesHouseOptions.Value.BaseUrl);

        var startIndex = 0;
        while (true)
        {
            var requestUrl = $"{baseUrl}/company/{encodedCompanyNumber}/filing-history?category=accounts&items_per_page={PageSize}&start_index={startIndex}";
            using var response = await SendWithRetryAsync(
                () => BuildAuthenticatedRequest(requestUrl, "application/json"),
                ct);

            if (response.StatusCode == HttpStatusCode.NotFound)
                return [];

            if (!response.IsSuccessStatusCode)
            {
                logger.LogWarning(
                    "Filing history request failed for company {CompanyNumber}. Status={StatusCode}",
                    companyNumber,
                    (int)response.StatusCode);
                return allItems;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(ct);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
            var root = doc.RootElement;
            if (!root.TryGetProperty("items", out var itemsNode) || itemsNode.ValueKind != JsonValueKind.Array)
                break;

            var pageItemCount = 0;
            foreach (var item in itemsNode.EnumerateArray())
            {
                pageItemCount++;
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                var category = Normalize(GetString(item, "category"));
                if (!string.Equals(category, "accounts", StringComparison.OrdinalIgnoreCase))
                    continue;

                var documentMetadataUrl = ExtractDocumentMetadataUrl(item);
                var documentId = ExtractDocumentId(documentMetadataUrl);
                if (documentMetadataUrl is null || documentId is null)
                    continue;

                allItems.Add(new AccountsFilingItem(
                    Normalize(GetString(item, "transaction_id")),
                    ParseDate(GetString(item, "date")),
                    ParseMadeUpDate(item),
                    HumanizeDescription(Normalize(GetString(item, "description")) ?? Normalize(GetString(item, "type"))),
                    documentId,
                    documentMetadataUrl,
                    item.GetRawText()));
            }

            if (pageItemCount <= 0)
                break;

            var totalCount = GetInt(root, "total_count") ?? GetInt(root, "total_results");
            startIndex += pageItemCount;
            if (totalCount.HasValue && startIndex >= totalCount.Value)
                break;
            if (pageItemCount < PageSize)
                break;
            if (startIndex > 10000)
                break;
        }

        return allItems
            .GroupBy(x => x.DocumentId, StringComparer.OrdinalIgnoreCase)
            .Select(g => g
                .OrderByDescending(x => x.MadeUpDate ?? DateTime.MinValue)
                .ThenByDescending(x => x.FilingDate ?? DateTime.MinValue)
                .First())
            .OrderByDescending(x => x.MadeUpDate.HasValue)
            .ThenByDescending(x => x.MadeUpDate)
            .ThenByDescending(x => x.FilingDate)
            .ToList();
    }

    private async Task<DownloadedAccountFile?> DownloadAccountFileAsync(
        string companyNumber,
        string documentId,
        DateTime? madeUpDate,
        DateTime? filingDate,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(webHostEnvironment.WebRootPath))
        {
            logger.LogWarning(
                "Accounts file download skipped because WebRootPath is unavailable. CompanyNumber={CompanyNumber} DocumentId={DocumentId}",
                companyNumber,
                documentId);
            return null;
        }

        var pdfResult = await DownloadDocumentContentAsync(documentId, "application/pdf", ct);
        if (pdfResult is null)
            return null;

        DocumentDownloadResult? content = pdfResult;
        if (content.StatusCode == HttpStatusCode.NotAcceptable)
            content = await DownloadDocumentContentAsync(documentId, "application/xhtml+xml", ct);

        if (content is null || !content.Success || content.StatusCode == HttpStatusCode.NotAcceptable)
        {
            logger.LogWarning(
                "Document download failed for DocumentId={DocumentId}. Status={StatusCode}",
                documentId,
                content?.StatusCode);
            return null;
        }

        var companyDirName = SanitizePathSegment(companyNumber);
        var accountRootAbsolute = Path.Combine(webHostEnvironment.WebRootPath, "site-assets", "accounts", companyDirName);
        Directory.CreateDirectory(accountRootAbsolute);

        var datePart = madeUpDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            ?? filingDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            ?? "unknown-date";
        var extension = GetFileExtension(content.ContentType);
        var safeDocumentId = SanitizePathSegment(documentId);
        var fileName = $"{datePart}_{safeDocumentId}{extension}";
        var absoluteFilePath = Path.Combine(accountRootAbsolute, fileName);

        var tempFilePath = absoluteFilePath + ".tmp";
        await using (var output = File.Create(tempFilePath))
        {
            await output.WriteAsync(content.ContentBytes, ct);
        }

        if (File.Exists(absoluteFilePath))
            File.Delete(absoluteFilePath);
        File.Move(tempFilePath, absoluteFilePath);

        var fileInfo = new FileInfo(absoluteFilePath);
        var relativePath = $"/site-assets/accounts/{companyDirName}/{fileName}";
        return new DownloadedAccountFile(
            content.ContentType,
            fileName,
            relativePath,
            fileInfo.Length);
    }

    private async Task<DocumentDownloadResult?> DownloadDocumentContentAsync(string documentId, string acceptType, CancellationToken ct)
    {
        var url = $"https://document-api.company-information.service.gov.uk/document/{Uri.EscapeDataString(documentId)}/content";
        var response = await SendWithRetryAsync(
            () => BuildAuthenticatedRequest(url, acceptType),
            ct);

        if (!response.IsSuccessStatusCode && response.StatusCode != HttpStatusCode.NotAcceptable)
        {
            logger.LogWarning(
                "Document API request failed for DocumentId={DocumentId}. Accept={Accept} Status={StatusCode}",
                documentId,
                acceptType,
                (int)response.StatusCode);
            response.Dispose();
            return null;
        }

        if (response.StatusCode == HttpStatusCode.NotAcceptable)
        {
            response.Dispose();
            return new DocumentDownloadResult(false, response.StatusCode, acceptType, Array.Empty<byte>());
        }

        var contentType = Normalize(response.Content.Headers.ContentType?.MediaType) ?? acceptType;
        var bytes = await response.Content.ReadAsByteArrayAsync(ct);
        response.Dispose();
        return new DocumentDownloadResult(true, response.StatusCode, contentType, bytes);
    }

    private async Task<HttpResponseMessage> SendWithRetryAsync(Func<HttpRequestMessage> requestFactory, CancellationToken ct)
    {
        for (var attempt = 1; attempt <= MaxRetryAttempts; attempt++)
        {
            try
            {
                using var request = requestFactory();
                var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
                if (attempt < MaxRetryAttempts && IsTransientStatusCode(response.StatusCode))
                {
                    response.Dispose();
                    await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), ct);
                    continue;
                }

                return response;
            }
            catch (HttpRequestException ex) when (attempt < MaxRetryAttempts)
            {
                logger.LogWarning(ex, "Transient HTTP error during Companies House request attempt {Attempt}. Retrying.", attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), ct);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested && attempt < MaxRetryAttempts)
            {
                logger.LogWarning(ex, "HTTP timeout during Companies House request attempt {Attempt}. Retrying.", attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), ct);
            }
        }

        using var finalRequest = requestFactory();
        return await httpClient.SendAsync(finalRequest, HttpCompletionOption.ResponseHeadersRead, ct);
    }

    private HttpRequestMessage BuildAuthenticatedRequest(string requestUrl, string acceptType)
    {
        var apiKey = Normalize(companiesHouseOptions.Value.ApiKey);
        if (apiKey is null)
            throw new InvalidOperationException("Companies House API key is not configured.");

        var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
        var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        request.Headers.Accept.Clear();
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(acceptType));
        return request;
    }

    private bool HasLocalFile(string? localRelativePath)
    {
        if (string.IsNullOrWhiteSpace(localRelativePath))
            return false;
        if (string.IsNullOrWhiteSpace(webHostEnvironment.WebRootPath))
            return false;

        var normalizedRelativePath = localRelativePath.Trim().TrimStart('/');
        var absolutePath = Path.Combine(
            webHostEnvironment.WebRootPath,
            normalizedRelativePath.Replace('/', Path.DirectorySeparatorChar));
        return File.Exists(absolutePath);
    }

    private static string? ExtractDocumentMetadataUrl(JsonElement filingItem)
    {
        if (!filingItem.TryGetProperty("links", out var links) || links.ValueKind != JsonValueKind.Object)
            return null;
        return Normalize(GetString(links, "document_metadata"));
    }

    private static string? ExtractDocumentId(string? documentMetadataUrl)
    {
        var normalized = Normalize(documentMetadataUrl);
        if (normalized is null)
            return null;

        if (!Uri.TryCreate(normalized, UriKind.Absolute, out var uri)
            && !Uri.TryCreate($"https://api.company-information.service.gov.uk{(normalized.StartsWith("/") ? string.Empty : "/")}{normalized}", UriKind.Absolute, out uri))
        {
            return null;
        }

        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < segments.Length - 1; i++)
        {
            if (!string.Equals(segments[i], "document", StringComparison.OrdinalIgnoreCase))
                continue;

            var candidate = Normalize(segments[i + 1]);
            if (candidate is not null)
                return candidate;
        }

        return null;
    }

    private static DateTime? ParseMadeUpDate(JsonElement filingItem)
    {
        if (!filingItem.TryGetProperty("description_values", out var values) || values.ValueKind != JsonValueKind.Object)
            return null;
        return ParseDate(GetString(values, "made_up_date"));
    }

    private static string NormalizeBaseUrl(string? baseUrl)
    {
        var normalized = Normalize(baseUrl);
        if (normalized is null)
            return "https://api.company-information.service.gov.uk";
        return normalized.TrimEnd('/');
    }

    private static string? HumanizeDescription(string? value)
    {
        var normalized = Normalize(value);
        if (normalized is null)
            return null;

        var tokens = normalized
            .Replace('_', ' ')
            .Replace('-', ' ')
            .Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(token => char.ToUpperInvariant(token[0]) + token[1..].ToLowerInvariant());
        return string.Join(' ', tokens);
    }

    private static int? GetInt(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
            return null;
        if (value.TryGetInt32(out var number))
            return number;
        return int.TryParse(value.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static DateTime? ParseDate(string? value)
    {
        var normalized = Normalize(value);
        if (normalized is null)
            return null;
        return DateTime.TryParseExact(normalized, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)
            ? parsed.Date
            : null;
    }

    private static string? GetString(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value) || value.ValueKind == JsonValueKind.Null)
            return null;
        return value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : value.ToString();
    }

    private static string GetFileExtension(string? contentType)
    {
        var normalized = Normalize(contentType)?.ToLowerInvariant();
        return normalized switch
        {
            "application/pdf" => ".pdf",
            "application/xhtml+xml" => ".xhtml",
            _ => ".bin"
        };
    }

    private static bool IsTransientStatusCode(HttpStatusCode statusCode)
    {
        var numeric = (int)statusCode;
        if (statusCode == HttpStatusCode.RequestTimeout)
            return true;
        if (statusCode == (HttpStatusCode)429)
            return true;
        return numeric >= 500;
    }

    private static string SanitizePathSegment(string value)
    {
        var trimmed = Normalize(value) ?? "unknown";
        var chars = trimmed
            .Select(ch => char.IsLetterOrDigit(ch) || ch is '-' or '_' ? ch : '_')
            .ToArray();
        var sanitized = new string(chars).Trim('_');
        return sanitized.Length == 0 ? "unknown" : sanitized;
    }

    private static string? Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Trim();
    }

    private sealed record AccountsFilingItem(
        string? TransactionId,
        DateTime? FilingDate,
        DateTime? MadeUpDate,
        string? AccountsType,
        string DocumentId,
        string DocumentMetadataUrl,
        string RawJson);

    private sealed record ExistingAccountRow(
        long Id,
        string DocumentId,
        string LocalRelativePath,
        string? ContentType,
        string? OriginalFileName,
        long? FileSizeBytes);

    private sealed record DownloadedAccountFile(
        string? ContentType,
        string OriginalFileName,
        string LocalRelativePath,
        long FileSizeBytes);

    private sealed record DocumentDownloadResult(
        bool Success,
        HttpStatusCode StatusCode,
        string? ContentType,
        byte[] ContentBytes);
}
