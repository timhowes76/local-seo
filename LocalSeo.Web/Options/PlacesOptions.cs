namespace LocalSeo.Web.Options;

public sealed class PlacesOptions
{
    public int DefaultRadiusMeters { get; set; } = 5000;
    public int DefaultResultLimit { get; set; } = 20;
    public string ReviewsProvider { get; set; } = "None";
}
