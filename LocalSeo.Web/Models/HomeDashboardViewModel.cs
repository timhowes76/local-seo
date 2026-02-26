namespace LocalSeo.Web.Models;

public sealed class HomeDashboardViewModel
{
    public decimal? DataForSeoBalanceUsd { get; set; }
    public string? DataForSeoBalanceDisplay { get; set; }
    public string? AccountError { get; set; }
    public DateTime RetrievedAtUtc { get; set; }
    public IReadOnlyList<DataForSeoApiStatusRow> ApiStatuses { get; set; } = [];
    public IReadOnlyList<ExternalApiHealthWidgetModel> ExternalApiHealthWidgets { get; set; } = [];
    public DateTime ApiStatusRetrievedAtUtc { get; set; }
    public IReadOnlyList<ApiStatusWidgetModel> ApiStatusWidgets { get; set; } = [];
}

public sealed record DataForSeoApiStatusRow(
    string ServiceName,
    string StatusLabel,
    string StatusDescription,
    int? StatusCode,
    string? RawMessage);

public sealed record ExternalApiHealthWidgetModel(
    string Name,
    bool HasData,
    bool IsUp,
    bool IsDegraded,
    DateTime? CheckedAtUtc,
    int? LatencyMs,
    string EndpointCalled,
    int? HttpStatusCode,
    string? LastError);
