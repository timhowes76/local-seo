namespace LocalSeo.Web.Models;

public sealed class HomeDashboardViewModel
{
    public decimal? DataForSeoBalanceUsd { get; init; }
    public string? DataForSeoBalanceDisplay { get; init; }
    public string? AccountError { get; init; }
    public DateTime RetrievedAtUtc { get; init; }
    public IReadOnlyList<DataForSeoApiStatusRow> ApiStatuses { get; init; } = [];
}

public sealed record DataForSeoApiStatusRow(
    string ServiceName,
    string StatusLabel,
    string StatusDescription,
    int? StatusCode,
    string? RawMessage);
