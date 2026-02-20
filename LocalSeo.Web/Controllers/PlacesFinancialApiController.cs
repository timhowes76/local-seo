using System.Globalization;
using LocalSeo.Web.Models;
using LocalSeo.Web.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LocalSeo.Web.Controllers;

[Authorize(Policy = "StaffOnly")]
[ApiController]
public sealed class PlacesFinancialApiController(
    ICompaniesHouseService companiesHouseService,
    ISearchIngestionService ingestionService,
    ICompaniesHouseAccountsSyncService accountsSyncService,
    ILogger<PlacesFinancialApiController> logger) : ControllerBase
{
    [HttpGet("/api/companies-house/search")]
    public async Task<IActionResult> SearchCompanies([FromQuery] string? q, [FromQuery] string? location, CancellationToken ct)
    {
        var normalizedQuery = Normalize(q);
        var normalizedLocation = Normalize(location);
        if (normalizedQuery is null && normalizedLocation is null)
            return BadRequest(new { message = "Company name or location is required." });

        var results = await companiesHouseService.SearchCompaniesAsync(normalizedQuery, normalizedLocation, ct);
        return Ok(results);
    }

    [HttpPost("/api/places/{placeId}/financial")]
    public async Task<IActionResult> SaveFinancialInfo(string placeId, [FromBody] SavePlaceFinancialRequest? request, CancellationToken ct)
    {
        if (request is null)
            return BadRequest(new { message = "Request body is required." });

        var companyNumber = (request.CompanyNumber ?? string.Empty).Trim();
        if (companyNumber.Length == 0)
            return BadRequest(new { message = "Company number is required." });

        DateTime? dateOfCreation = null;
        var rawDate = (request.DateOfCreation ?? string.Empty).Trim();
        if (rawDate.Length > 0)
        {
            if (!DateTime.TryParseExact(rawDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
                return BadRequest(new { message = "DateOfCreation must be in yyyy-MM-dd format." });

            dateOfCreation = parsed.Date;
        }

        var profile = await companiesHouseService.GetCompanyProfileAsync(companyNumber, ct);
        if (profile is null)
            return NotFound(new { message = $"Company '{companyNumber}' was not found in Companies House." });
        var officers = await companiesHouseService.GetCompanyOfficersAsync(companyNumber, ct);
        var pscs = await companiesHouseService.GetCompanyPersonsWithSignificantControlAsync(companyNumber, placeId, ct);

        var upsert = new PlaceFinancialInfoUpsert(
            dateOfCreation ?? profile.DateOfCreation,
            companyNumber,
            request.CompanyType ?? profile.CompanyType,
            profile.LastAccountsFiled,
            profile.NextAccountsDue,
            profile.CompanyStatus,
            profile.HasLiquidated,
            profile.HasCharges,
            profile.HasInsolvencyHistory,
            officers.Select(MapOfficer).ToList(),
            pscs.Select(MapPsc).ToList());

        var saved = await ingestionService.SavePlaceFinancialAsync(placeId, upsert, ct);
        if (!saved)
            return NotFound();

        try
        {
            await accountsSyncService.SyncAccountsAsync(placeId, companyNumber, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(
                ex,
                "Accounts sync failed after financial save. PlaceId={PlaceId} CompanyNumber={CompanyNumber}",
                placeId,
                companyNumber);
        }

        return Ok(new { message = "Financial info saved." });
    }

    [HttpPost("/api/places/{placeId}/financial/refresh")]
    public async Task<IActionResult> RefreshFinancialInfo(string placeId, [FromBody] RefreshPlaceFinancialRequest? request, CancellationToken ct)
    {
        var existing = await ingestionService.GetPlaceFinancialAsync(placeId, ct);
        if (existing is null)
            return NotFound(new { message = "No financial record exists for this place." });

        var requestedCompanyNumber = Normalize(request?.CompanyNumber);
        var companyNumber = requestedCompanyNumber ?? existing.CompanyNumber;
        if (companyNumber is null)
            return BadRequest(new { message = "Company number is required." });

        var profile = await companiesHouseService.GetCompanyProfileAsync(companyNumber, ct);
        if (profile is null)
            return NotFound(new { message = $"Company '{companyNumber}' was not found in Companies House." });
        var officers = await companiesHouseService.GetCompanyOfficersAsync(companyNumber, ct);
        var pscs = await companiesHouseService.GetCompanyPersonsWithSignificantControlAsync(companyNumber, placeId, ct);

        var upsert = new PlaceFinancialInfoUpsert(
            profile.DateOfCreation ?? existing.DateOfCreation,
            companyNumber,
            profile.CompanyType ?? existing.CompanyType,
            profile.LastAccountsFiled,
            profile.NextAccountsDue,
            profile.CompanyStatus,
            profile.HasLiquidated,
            profile.HasCharges,
            profile.HasInsolvencyHistory,
            officers.Select(MapOfficer).ToList(),
            pscs.Select(MapPsc).ToList());

        var saved = await ingestionService.SavePlaceFinancialAsync(placeId, upsert, ct);
        if (!saved)
            return NotFound();

        try
        {
            await accountsSyncService.SyncAccountsAsync(placeId, companyNumber, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning(
                ex,
                "Accounts sync failed after financial refresh. PlaceId={PlaceId} CompanyNumber={CompanyNumber}",
                placeId,
                companyNumber);
        }

        return Ok(new { message = "Financial info refreshed." });
    }

    public sealed class SavePlaceFinancialRequest
    {
        public string? DateOfCreation { get; set; }
        public string? CompanyNumber { get; set; }
        public string? CompanyType { get; set; }
    }

    public sealed class RefreshPlaceFinancialRequest
    {
        public string? CompanyNumber { get; set; }
    }

    private static string? Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
        return value.Trim();
    }

    private static PlaceFinancialOfficerUpsert MapOfficer(CompaniesHouseOfficer officer)
    {
        return new PlaceFinancialOfficerUpsert(
            officer.FirstNames,
            officer.LastName,
            officer.CountryOfResidence,
            officer.DateOfBirth,
            officer.Nationality,
            officer.Role,
            officer.Appointed,
            officer.Resigned);
    }

    private static PlaceFinancialPersonOfSignificantControlUpsert MapPsc(CompaniesHousePersonWithSignificantControl psc)
    {
        return new PlaceFinancialPersonOfSignificantControlUpsert(
            psc.CompanyNumber,
            psc.PscItemKind,
            psc.PscLinkSelf,
            psc.PscId,
            psc.NameRaw,
            psc.FirstNames,
            psc.LastName,
            psc.CountryOfResidence,
            psc.Nationality,
            psc.BirthMonth,
            psc.BirthYear,
            psc.NotifiedOn,
            psc.CeasedOn,
            psc.SourceEtag,
            psc.RetrievedUtc,
            psc.RawJson,
            psc.NatureCodes);
    }
}
