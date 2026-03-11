namespace LocalSeo.Web.Services;

internal static class GeoDistanceCalculator
{
    public static decimal? DistanceKm(decimal? originLat, decimal? originLng, decimal? targetLat, decimal? targetLng)
    {
        if (!originLat.HasValue || !originLng.HasValue || !targetLat.HasValue || !targetLng.HasValue)
            return null;

        const double earthRadiusKm = 6371d;
        var lat1 = (double)originLat.Value * Math.PI / 180d;
        var lat2 = (double)targetLat.Value * Math.PI / 180d;
        var deltaLat = lat2 - lat1;
        var deltaLng = ((double)targetLng.Value - (double)originLng.Value) * Math.PI / 180d;
        var sinLat = Math.Sin(deltaLat / 2d);
        var sinLng = Math.Sin(deltaLng / 2d);
        var a = (sinLat * sinLat) + (Math.Cos(lat1) * Math.Cos(lat2) * sinLng * sinLng);
        var c = 2d * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1d - a));
        return (decimal)(earthRadiusKm * c);
    }
}
