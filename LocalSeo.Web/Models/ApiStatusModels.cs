namespace LocalSeo.Web.Models;

public sealed record ApiStatusWidgetModel(
    int DefinitionId,
    string Key,
    string DisplayName,
    string Category,
    bool IsEnabled,
    Services.ApiHealthStatus Status,
    DateTime? CheckedUtc,
    int? LatencyMs,
    string? Message);

public sealed class ApiStatusDetailsViewModel
{
    public IReadOnlyList<ApiStatusWidgetModel> Rows { get; init; } = [];
    public IReadOnlyList<string> CategoryOptions { get; init; } = [];
    public string? SelectedCategory { get; init; }
    public string? Search { get; init; }
}

public sealed class AdminApiStatusDefinitionRowModel
{
    public int Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public bool IsEnabled { get; set; }
    public int IntervalSeconds { get; set; }
    public int TimeoutSeconds { get; set; }
    public int? DegradedThresholdMs { get; set; }
}

public sealed class AdminApiStatusDefinitionsViewModel
{
    public List<AdminApiStatusDefinitionRowModel> Rows { get; set; } = [];
}

