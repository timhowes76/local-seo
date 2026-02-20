namespace LocalSeo.Web.Models;

public sealed record CompaniesHouseCompanySearchResult(
    string Title,
    string Address,
    DateTime? DateOfCreation,
    string CompanyNumber,
    string? CompanyType);

public sealed record CompaniesHouseCompanyProfile(
    string CompanyNumber,
    DateTime? DateOfCreation,
    string? CompanyType,
    DateTime? LastAccountsFiled,
    DateTime? NextAccountsDue,
    string? CompanyStatus,
    bool HasLiquidated,
    bool HasCharges,
    bool HasInsolvencyHistory);

public sealed record CompaniesHouseOfficer(
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    DateTime? DateOfBirth,
    string? Nationality,
    string? Role,
    DateTime? Appointed,
    DateTime? Resigned);

public sealed record CompaniesHousePersonWithSignificantControl(
    string CompanyNumber,
    string? PscItemKind,
    string? PscLinkSelf,
    string? PscId,
    string? NameRaw,
    string? FirstNames,
    string? LastName,
    string? CountryOfResidence,
    string? Nationality,
    byte? BirthMonth,
    int? BirthYear,
    DateTime? NotifiedOn,
    DateTime? CeasedOn,
    string? SourceEtag,
    DateTime RetrievedUtc,
    string? RawJson,
    IReadOnlyList<string> NatureCodes);
