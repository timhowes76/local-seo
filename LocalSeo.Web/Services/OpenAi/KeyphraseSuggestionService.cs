using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Dapper;
using LocalSeo.Web.Data;
using LocalSeo.Web.Models;
using LocalSeo.Web.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LocalSeo.Web.Services;

public interface IKeyphraseSuggestionService
{
    Task<KeyphraseSuggestionResponse> SuggestAsync(
        string categoryId,
        int countyId,
        int townId,
        int maxSuggestions,
        CancellationToken ct);
}

public static class KeyphraseSuggestionErrorCodes
{
    public const string OpenAiNotConfigured = "openai_not_configured";
    public const string OpenAiUnavailable = "openai_unavailable";
}

public sealed class KeyphraseSuggestionException(string errorCode, string message) : Exception(message)
{
    public string ErrorCode { get; } = errorCode;
}

public sealed class KeyphraseSuggestionService(
    ISqlConnectionFactory connectionFactory,
    IAdminSettingsService adminSettingsService,
    IHttpClientFactory httpClientFactory,
    IMemoryCache memoryCache,
    IOptions<OpenAiOptions> openAiOptions,
    ILogger<KeyphraseSuggestionService> logger) : IKeyphraseSuggestionService
{
    private const string SystemPrompt = "You generate local SEO keyword suggestions for a given service category and town in the UK. You must follow the provided JSON schema exactly.";
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task<KeyphraseSuggestionResponse> SuggestAsync(
        string categoryId,
        int countyId,
        int townId,
        int maxSuggestions,
        CancellationToken ct)
    {
        var normalizedCategoryId = (categoryId ?? string.Empty).Trim();
        var normalizedMaxSuggestions = Math.Clamp(maxSuggestions, 5, 100);
        if (string.IsNullOrWhiteSpace(normalizedCategoryId))
            throw new InvalidOperationException("Category is required.");
        if (countyId <= 0)
            throw new InvalidOperationException("County is required.");
        if (townId <= 0)
            throw new InvalidOperationException("Town is required.");

        var context = await GetContextAsync(normalizedCategoryId, townId, ct);
        if (context.CountyId != countyId)
            throw new InvalidOperationException("Town does not belong to the selected county.");

        var mainKeyword = KeyphraseSuggestionRules.BuildMainKeyword(context.CategoryDisplayName, context.TownName);
        var cacheKey = BuildCacheKey(normalizedCategoryId, townId);
        if (memoryCache.TryGetValue<KeyphraseSuggestionResponse>(cacheKey, out var cached) && cached is not null)
        {
            return new KeyphraseSuggestionResponse
            {
                MainKeyword = mainKeyword,
                RequiredLocationName = context.TownName,
                Suggestions = cached.Suggestions.Take(normalizedMaxSuggestions).ToList()
            };
        }

        var settings = await adminSettingsService.GetAsync(ct);
        var apiKey = ResolveApiKey(settings);
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            throw new KeyphraseSuggestionException(
                KeyphraseSuggestionErrorCodes.OpenAiNotConfigured,
                "Admin must configure OpenAI API key.");
        }

        var model = ResolveModel(settings);
        var timeoutSeconds = ResolveTimeoutSeconds(settings);
        var rawResponse = await RequestOpenAiSuggestionsAsync(
            apiKey,
            model,
            timeoutSeconds,
            context.CategoryDisplayName,
            context.TownName,
            mainKeyword,
            normalizedMaxSuggestions,
            ct);

        var filtered = KeyphraseSuggestionRules.NormalizeAndFilterSuggestions(
            rawResponse.Suggestions,
            context.TownName,
            mainKeyword,
            normalizedMaxSuggestions)
            .ToList();

        var result = new KeyphraseSuggestionResponse
        {
            MainKeyword = mainKeyword,
            RequiredLocationName = context.TownName,
            Suggestions = filtered
        };

        memoryCache.Set(cacheKey, result, TimeSpan.FromDays(30));
        return result;
    }

    private async Task<OpenAiStructuredSuggestionResponse> RequestOpenAiSuggestionsAsync(
        string apiKey,
        string model,
        int timeoutSeconds,
        string categoryDisplayName,
        string townName,
        string mainKeyword,
        int maxSuggestions,
        CancellationToken ct)
    {
        var payload = BuildRequestPayload(model, categoryDisplayName, townName, mainKeyword, maxSuggestions);
        var requestBody = JsonSerializer.Serialize(payload);
        var endpoint = ResolveEndpoint();
        var client = httpClientFactory.CreateClient();

        for (var attempt = 0; attempt <= 2; attempt++)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            linkedCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = new StringContent(requestBody, Encoding.UTF8, "application/json")
                };
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                using var response = await client.SendAsync(request, linkedCts.Token);
                var body = await response.Content.ReadAsStringAsync(linkedCts.Token);
                if (response.IsSuccessStatusCode)
                    return ParseStructuredResponse(body);

                if (IsTransient(response.StatusCode) && attempt < 2)
                {
                    logger.LogWarning(
                        "OpenAI suggestions transient failure. Status={StatusCode}, Attempt={Attempt}.",
                        (int)response.StatusCode,
                        attempt + 1);
                    await Task.Delay(GetRetryDelay(attempt), ct);
                    continue;
                }

                logger.LogWarning(
                    "OpenAI suggestions request failed. Status={StatusCode}, Attempt={Attempt}.",
                    (int)response.StatusCode,
                    attempt + 1);

                if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                {
                    throw new KeyphraseSuggestionException(
                        KeyphraseSuggestionErrorCodes.OpenAiUnavailable,
                        "OpenAI credentials are invalid. Update the API key in Admin settings.");
                }

                throw new KeyphraseSuggestionException(
                    KeyphraseSuggestionErrorCodes.OpenAiUnavailable,
                    "Unable to generate suggestions right now. Please try again.");
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attempt < 2)
            {
                logger.LogWarning(
                    "OpenAI suggestions request timed out. Attempt={Attempt}.",
                    attempt + 1);
                await Task.Delay(GetRetryDelay(attempt), ct);
            }
            catch (HttpRequestException ex) when (attempt < 2)
            {
                logger.LogWarning(
                    ex,
                    "OpenAI suggestions request failed due to network error. Attempt={Attempt}.",
                    attempt + 1);
                await Task.Delay(GetRetryDelay(attempt), ct);
            }
            catch (JsonException ex)
            {
                logger.LogWarning(ex, "OpenAI suggestions response parsing failed.");
                throw new KeyphraseSuggestionException(
                    KeyphraseSuggestionErrorCodes.OpenAiUnavailable,
                    "Unable to process AI suggestions right now. Please try again.");
            }
        }

        throw new KeyphraseSuggestionException(
            KeyphraseSuggestionErrorCodes.OpenAiUnavailable,
            "Unable to generate suggestions right now. Please try again.");
    }

    private static object BuildRequestPayload(
        string model,
        string categoryDisplayName,
        string townName,
        string mainKeyword,
        int maxSuggestions)
    {
        var userMessage = $"""
categoryDisplayName: {categoryDisplayName}
townName: {townName}
mainKeyword: {mainKeyword}
maxSuggestions: {maxSuggestions}

Return up to maxSuggestions suggestions. Mix of:
- Modifiers: intent/preference qualifiers applied to category (best, affordable, emergency, same day, near me BUT still must include townName, etc.)
- Adjacent: closely related services customers also consider.
All keywords MUST contain the exact townName token somewhere. Do NOT include mainKeyword. Avoid duplicates and trivial word-order shuffles. Use natural English phrasing. Output strictly according to schema.
""";

        return new
        {
            model,
            input = new object[]
            {
                new
                {
                    role = "system",
                    content = new object[]
                    {
                        new { type = "input_text", text = SystemPrompt }
                    }
                },
                new
                {
                    role = "user",
                    content = new object[]
                    {
                        new { type = "input_text", text = userMessage }
                    }
                }
            },
            text = new
            {
                format = new
                {
                    type = "json_schema",
                    name = "keyphrase_suggestion_response",
                    strict = true,
                    schema = new
                    {
                        type = "object",
                        additionalProperties = false,
                        properties = new
                        {
                            mainKeyword = new { type = "string" },
                            requiredLocationName = new { type = "string" },
                            suggestions = new
                            {
                                type = "array",
                                maxItems = maxSuggestions,
                                items = new
                                {
                                    type = "object",
                                    additionalProperties = false,
                                    properties = new
                                    {
                                        keyword = new { type = "string", maxLength = 255 },
                                        keywordType = new { type = "integer", @enum = new[] { 3, 4 } },
                                        confidence = new { type = "number", minimum = 0, maximum = 1 }
                                    },
                                    required = new[] { "keyword", "keywordType", "confidence" }
                                }
                            }
                        },
                        required = new[] { "mainKeyword", "requiredLocationName", "suggestions" }
                    }
                }
            }
        };
    }

    private static OpenAiStructuredSuggestionResponse ParseStructuredResponse(string responseBody)
    {
        using var doc = JsonDocument.Parse(responseBody);
        var payloadText = ExtractPayloadText(doc.RootElement);
        if (string.IsNullOrWhiteSpace(payloadText))
            throw new JsonException("Structured output payload was empty.");

        var normalizedPayload = StripCodeFence(payloadText);
        var parsed = JsonSerializer.Deserialize<OpenAiStructuredSuggestionResponse>(normalizedPayload, JsonOptions);
        if (parsed is null)
            throw new JsonException("Structured output payload was invalid.");
        parsed.Suggestions ??= [];
        return parsed;
    }

    private static string? ExtractPayloadText(JsonElement root)
    {
        if (root.TryGetProperty("output_text", out var outputTextNode) && outputTextNode.ValueKind == JsonValueKind.String)
            return outputTextNode.GetString();

        if (!root.TryGetProperty("output", out var outputNode) || outputNode.ValueKind != JsonValueKind.Array)
            return null;

        foreach (var outputItem in outputNode.EnumerateArray())
        {
            if (!outputItem.TryGetProperty("content", out var contentNode) || contentNode.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var contentItem in contentNode.EnumerateArray())
            {
                if (contentItem.TryGetProperty("type", out var typeNode)
                    && typeNode.ValueKind == JsonValueKind.String
                    && string.Equals(typeNode.GetString(), "output_text", StringComparison.OrdinalIgnoreCase)
                    && contentItem.TryGetProperty("text", out var textNode)
                    && textNode.ValueKind == JsonValueKind.String)
                {
                    return textNode.GetString();
                }

                if (contentItem.TryGetProperty("json", out var jsonNode)
                    && (jsonNode.ValueKind == JsonValueKind.Object || jsonNode.ValueKind == JsonValueKind.Array))
                {
                    return jsonNode.GetRawText();
                }
            }
        }

        return null;
    }

    private static string StripCodeFence(string value)
    {
        var trimmed = value.Trim();
        if (!trimmed.StartsWith("```", StringComparison.Ordinal) || !trimmed.EndsWith("```", StringComparison.Ordinal))
            return trimmed;

        var firstLineBreak = trimmed.IndexOf('\n');
        if (firstLineBreak < 0)
            return trimmed.Trim('`');

        return trimmed[(firstLineBreak + 1)..^3].Trim();
    }

    private async Task<SuggestionContextRow> GetContextAsync(string categoryId, int townId, CancellationToken ct)
    {
        await using var conn = (SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<SuggestionContextRow>(new CommandDefinition(
            """
SELECT TOP 1
  c.CategoryId,
  c.DisplayName AS CategoryDisplayName,
  t.TownId,
  t.Name AS TownName,
  t.CountyId
FROM dbo.GoogleBusinessProfileCategory c
CROSS JOIN dbo.GbTown t
WHERE c.CategoryId = @CategoryId
  AND t.TownId = @TownId;
""",
            new { CategoryId = categoryId, TownId = townId },
            cancellationToken: ct));

        if (row is null)
            throw new InvalidOperationException("Category or town was not found.");

        return row;
    }

    private string ResolveApiKey(AdminSettingsModel settings)
    {
        var stored = (settings.OpenAiApiKey ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(stored))
            return stored;
        return (openAiOptions.Value.ApiKey ?? string.Empty).Trim();
    }

    private string ResolveModel(AdminSettingsModel settings)
    {
        var model = (settings.OpenAiModel ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(model))
            return model;

        var configured = (openAiOptions.Value.DefaultModel ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        return "gpt-4.1-mini";
    }

    private int ResolveTimeoutSeconds(AdminSettingsModel settings)
    {
        var configuredTimeout = settings.OpenAiTimeoutSeconds > 0
            ? settings.OpenAiTimeoutSeconds
            : openAiOptions.Value.TimeoutSeconds;
        return Math.Clamp(configuredTimeout, 5, 120);
    }

    private string ResolveEndpoint()
    {
        var endpoint = (openAiOptions.Value.ApiBaseUrl ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(endpoint))
            return "https://api.openai.com/v1/responses";
        return endpoint;
    }

    private static bool IsTransient(HttpStatusCode statusCode)
        => statusCode == HttpStatusCode.TooManyRequests || (int)statusCode >= 500;

    private static TimeSpan GetRetryDelay(int attempt)
    {
        var baseMilliseconds = attempt switch
        {
            0 => 500,
            1 => 1000,
            _ => 1500
        };
        return TimeSpan.FromMilliseconds(baseMilliseconds);
    }

    private static string BuildCacheKey(string categoryId, int townId)
        => $"keyphrase-suggestions:{categoryId.ToLowerInvariant()}:{townId.ToString(CultureInfo.InvariantCulture)}";

    private sealed class OpenAiStructuredSuggestionResponse
    {
        public string MainKeyword { get; set; } = string.Empty;
        public string RequiredLocationName { get; set; } = string.Empty;
        public List<KeyphraseSuggestionItem>? Suggestions { get; set; }
    }

    private sealed record SuggestionContextRow(
        string CategoryId,
        string CategoryDisplayName,
        long TownId,
        string TownName,
        long CountyId);
}
