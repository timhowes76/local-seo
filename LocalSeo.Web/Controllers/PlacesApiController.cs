using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
[ApiController]
public class PlacesApiController(IReviewVelocityService reviewVelocityService) : ControllerBase
{
    private static readonly int[] AllowedTakeOptions = [25, 50, 100, 500, 1000];

    [HttpGet("/api/places")]
    public async Task<IActionResult> GetPlaces([FromQuery] string? sort, [FromQuery] string? direction, [FromQuery] string? placeName, [FromQuery] string? keyword, [FromQuery] string? location, [FromQuery] int? take, CancellationToken ct)
    {
        var normalizedTake = NormalizeTake(take);
        var rows = await reviewVelocityService.GetPlaceVelocityListAsync(sort, direction, placeName, keyword, location, normalizedTake, ct);
        return Ok(rows);
    }

    [HttpGet("/api/places/{placeId}/review-velocity")]
    public async Task<IActionResult> GetReviewVelocity(string placeId, CancellationToken ct)
    {
        var details = await reviewVelocityService.GetPlaceReviewVelocityAsync(placeId, ct);
        if (details is null)
            return NotFound();

        return Ok(details);
    }

    private static int NormalizeTake(int? value)
    {
        if (!value.HasValue)
            return 100;

        return AllowedTakeOptions.Contains(value.Value) ? value.Value : 100;
    }
}
