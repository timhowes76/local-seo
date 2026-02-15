using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
[ApiController]
public class PlacesApiController(IReviewVelocityService reviewVelocityService) : ControllerBase
{
    [HttpGet("/api/places")]
    public async Task<IActionResult> GetPlaces([FromQuery] string? sort, [FromQuery] string? direction, [FromQuery] string? keyword, [FromQuery] string? location, CancellationToken ct)
    {
        var rows = await reviewVelocityService.GetPlaceVelocityListAsync(sort, direction, keyword, location, ct);
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
}
