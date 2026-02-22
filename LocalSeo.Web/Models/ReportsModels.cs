namespace LocalSeo.Web.Models;

public enum FirstContactReportVariant
{
    ClientFacing = 1,
    Internal = 2
}

public enum StrengthLevel
{
    Unknown = 0,
    Weak = 1,
    Ok = 2,
    Strong = 3
}

public sealed record ReportVariantSummary(
    FirstContactReportVariant Variant,
    string Label,
    string? ExistingPdfPath);

public sealed class FirstContactReportAvailability
{
    public string PlaceId { get; init; } = string.Empty;
    public long RunId { get; init; }
    public bool IsAvailable { get; init; }
    public string Message { get; init; } = string.Empty;
    public string? BusinessName { get; init; }
    public IReadOnlyList<ReportVariantSummary> Variants { get; init; } = [];
}

public sealed class FirstContactReportViewModel
{
    public string PlaceId { get; init; } = string.Empty;
    public long RunId { get; init; }
    public FirstContactReportVariant Variant { get; init; }
    public string VariantLabel { get; init; } = string.Empty;
    public bool IsClientFacing => Variant == FirstContactReportVariant.ClientFacing;
    public bool ShowRawMetrics { get; init; }
    public int Version { get; init; } = 1;

    public string LogoPath { get; init; } = "/assets/images/logo.svg";
    public string ReportTitle { get; init; } = "Local Visibility Snapshot";
    public string Strapline { get; init; } = "Successful websites driven by marketing expertise";
    public string BusinessName { get; init; } = string.Empty;
    public string TownName { get; init; } = string.Empty;
    public string PrimaryCategory { get; init; } = string.Empty;
    public DateTime RunDateUtc { get; init; }

    public int CurrentPosition { get; init; }
    public string PositionCommentary { get; init; } = "Top 3 captures most attention; position 4+ sees a sharp drop.";

    public FirstContactOpportunityEstimate MoveToPosition3 { get; init; } = new();
    public FirstContactOpportunityEstimate MoveToPosition1 { get; init; } = new();
    public string WhyThisMatters { get; init; } = string.Empty;
    public int EstimatedMonthlySearchDemand { get; init; }
    public decimal ConservativeMultiplier { get; init; }
    public decimal UpsideMultiplier { get; init; }

    public IReadOnlyList<FirstContactCompetitorSummary> Competitors { get; init; } = [];
    public FirstContactChartModel ReviewChart { get; init; } = new();
    public IReadOnlyList<FirstContactStrengthSignal> StrengthSignals { get; init; } = [];
    public string ProfileStrengthImpactText { get; init; } = "Stronger profiles tend to win more impressions in the top results.";

    public IReadOnlyList<string> Outcomes { get; init; } = [];
    public string AboutKontrolit { get; init; } = string.Empty;
    public string CtaLine { get; init; } = string.Empty;
    public string Disclaimer { get; init; } = "Estimates are based on an independent CTR model and observed search demand, not a guarantee of results.";

    public string BrandPrimary { get; init; } = "#0f2a5f";
    public string BrandAccent { get; init; } = "#5b7cfa";
    public string BrandDark { get; init; } = "#0f172a";
    public string BrandLight { get; init; } = "#f6f8fc";

    public string? ExistingPdfPath { get; init; }
}

public sealed class FirstContactOpportunityEstimate
{
    public int Conservative { get; init; }
    public int Expected { get; init; }
    public int Upside { get; init; }
}

public sealed class FirstContactChartModel
{
    public string GranularityLabel { get; init; } = "Monthly";
    public IReadOnlyList<string> Labels { get; init; } = [];
    public IReadOnlyList<FirstContactChartSeries> Series { get; init; } = [];
}

public sealed class FirstContactChartSeries
{
    public string Name { get; init; } = string.Empty;
    public bool IsYou { get; init; }
    public IReadOnlyList<int> Values { get; init; } = [];
}

public sealed class FirstContactStrengthSignal
{
    public string Key { get; init; } = string.Empty;
    public string Label { get; init; } = string.Empty;
    public StrengthLevel Strength { get; init; }
    public string Impact { get; init; } = string.Empty;
    public string? RawValue { get; init; }
}

public sealed class FirstContactCompetitorSummary
{
    public string PlaceId { get; init; } = string.Empty;
    public string DisplayName { get; init; } = string.Empty;
    public int RankPosition { get; init; }
}

public sealed class ReportCookie
{
    public string Name { get; init; } = string.Empty;
    public string Value { get; init; } = string.Empty;
    public string Domain { get; init; } = string.Empty;
    public string Path { get; init; } = "/";
    public bool HttpOnly { get; init; }
    public bool Secure { get; init; }
    public DateTime? ExpiresUtc { get; init; }
    public string SameSite { get; init; } = "Lax";
}

public sealed class FirstContactPdfGenerationRequest
{
    public string PlaceId { get; init; } = string.Empty;
    public long RunId { get; init; }
    public FirstContactReportVariant Variant { get; init; }
    public string ReportUrl { get; init; } = string.Empty;
    public IReadOnlyList<ReportCookie> Cookies { get; init; } = [];
    public int? CreatedByUserId { get; init; }
}

public sealed class FirstContactPdfGenerationResult
{
    public bool Success { get; init; }
    public string Message { get; init; } = string.Empty;
    public string? DownloadUrl { get; init; }
    public long? ReportId { get; init; }
}

public sealed class PlaceRunReportRecord
{
    public long ReportId { get; init; }
    public string PlaceId { get; init; } = string.Empty;
    public long RunId { get; init; }
    public string ReportType { get; init; } = string.Empty;
    public string Variant { get; init; } = string.Empty;
    public int Version { get; init; }
    public string? HtmlSnapshotPath { get; init; }
    public string? PdfPath { get; init; }
    public DateTime CreatedUtc { get; init; }
    public int? CreatedByUserId { get; init; }
    public string? ContentHash { get; init; }
}
