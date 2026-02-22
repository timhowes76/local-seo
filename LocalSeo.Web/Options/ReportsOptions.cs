namespace LocalSeo.Web.Options;

public sealed class ReportsOptions
{
    public decimal ConservativeMultiplier { get; set; } = 0.75m;
    public decimal UpsideMultiplier { get; set; } = 1.25m;
    public string PdfOutputRelativeDirectory { get; set; } = "/site-assets/reports";
    public int FirstContactVersion { get; set; } = 1;
}
