namespace LocalSeo.Web.Models;

public sealed class KeyphraseSuggestionResponse
{
    public string MainKeyword { get; set; } = string.Empty;
    public string RequiredLocationName { get; set; } = string.Empty;
    public List<KeyphraseSuggestionItem> Suggestions { get; set; } = [];
}

public sealed class KeyphraseSuggestionItem
{
    public string Keyword { get; set; } = string.Empty;
    public int KeywordType { get; set; }
    public decimal Confidence { get; set; }
}

public sealed class SuggestKeyphrasesRequest
{
    public string CategoryId { get; set; } = string.Empty;
    public int CountyId { get; set; }
    public int TownId { get; set; }
}

public sealed class AddBulkKeyphrasesRequest
{
    public string CategoryId { get; set; } = string.Empty;
    public int CountyId { get; set; }
    public int TownId { get; set; }
    public List<AddBulkKeyphraseItem> Items { get; set; } = [];
}

public sealed class AddBulkKeyphraseItem
{
    public string Keyword { get; set; } = string.Empty;
    public int KeywordType { get; set; }
}

public sealed class AddBulkKeyphrasesResponse
{
    public int AddedCount { get; set; }
    public int SkippedCount { get; set; }
    public int ErrorCount { get; set; }
    public List<AddBulkKeyphraseItemResult> Results { get; set; } = [];
}

public sealed class AddBulkKeyphraseItemResult
{
    public string Keyword { get; set; } = string.Empty;
    public int KeywordType { get; set; }
    public string Status { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}

public sealed class AddBulkKeyphraseJobStartResponse
{
    public string JobId { get; set; } = string.Empty;
    public int TotalCount { get; set; }
}

public sealed class AddBulkKeyphraseJobStatusResponse
{
    public string JobId { get; set; } = string.Empty;
    public int TotalCount { get; set; }
    public int CompletedCount { get; set; }
    public int AddedCount { get; set; }
    public int SkippedCount { get; set; }
    public int ErrorCount { get; set; }
    public bool IsCompleted { get; set; }
    public bool IsFailed { get; set; }
    public string Message { get; set; } = string.Empty;
    public List<AddBulkKeyphraseItemResult> Results { get; set; } = [];
}

public sealed class OtherLocationKeyphrasesRequest
{
    public string CategoryId { get; set; } = string.Empty;
    public int CountyId { get; set; }
    public int TownId { get; set; }
}

public sealed class OtherLocationKeyphraseDetailsRequest
{
    public string CategoryId { get; set; } = string.Empty;
    public int CountyId { get; set; }
    public int TownId { get; set; }
    public int SourceTownId { get; set; }
}

public sealed class CopyFromOtherLocationRequest
{
    public string CategoryId { get; set; } = string.Empty;
    public int CountyId { get; set; }
    public int TownId { get; set; }
    public int SourceTownId { get; set; }
}

public sealed class OtherLocationKeyphraseSourceSummary
{
    public long LocationId { get; set; }
    public string LocationName { get; set; } = string.Empty;
    public string CountyName { get; set; } = string.Empty;
    public int KeywordCount { get; set; }
    public DateTime? LastUpdatedUtc { get; set; }
}
