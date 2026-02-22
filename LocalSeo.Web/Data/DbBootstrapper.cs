using Dapper;

namespace LocalSeo.Web.Data;

public sealed class DbBootstrapper(ISqlConnectionFactory connectionFactory, ILogger<DbBootstrapper> logger)
{
    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await using var conn = (Microsoft.Data.SqlClient.SqlConnection)await connectionFactory.OpenConnectionAsync(ct);
        var sql = @"
IF OBJECT_ID('dbo.SearchRun','U') IS NULL
BEGIN
  CREATE TABLE dbo.SearchRun(
    SearchRunId bigint IDENTITY(1,1) PRIMARY KEY,
    SeedKeyword nvarchar(200) NOT NULL,
    LocationName nvarchar(200) NOT NULL,
    CenterLat decimal(9,6) NULL,
    CenterLng decimal(9,6) NULL,
    RadiusMeters int NULL,
    ResultLimit int NOT NULL,
    FetchDetailedData bit NOT NULL CONSTRAINT DF_SearchRun_FetchDetailedData DEFAULT(0),
    FetchGoogleReviews bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleReviews DEFAULT(0),
    FetchGoogleUpdates bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleUpdates DEFAULT(0),
    FetchGoogleQuestionsAndAnswers bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleQuestionsAndAnswers DEFAULT(0),
    FetchGoogleSocialProfiles bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleSocialProfiles DEFAULT(0),
    [Status] nvarchar(20) NOT NULL CONSTRAINT DF_SearchRun_Status DEFAULT(N'Completed'),
    TotalApiCalls int NULL,
    CompletedApiCalls int NULL,
    PercentComplete int NULL,
    StartedUtc datetime2(0) NULL,
    LastUpdatedUtc datetime2(0) NULL,
    CompletedUtc datetime2(0) NULL,
    ErrorMessage nvarchar(max) NULL,
    RanAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SearchRun_RanAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.SearchRun', 'FetchDetailedData') IS NULL
  ALTER TABLE dbo.SearchRun ADD FetchDetailedData bit NOT NULL CONSTRAINT DF_SearchRun_FetchDetailedData_Alt DEFAULT(0);
IF COL_LENGTH('dbo.SearchRun', 'FetchGoogleReviews') IS NULL
  ALTER TABLE dbo.SearchRun ADD FetchGoogleReviews bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleReviews_Alt DEFAULT(0);
IF COL_LENGTH('dbo.SearchRun', 'FetchGoogleUpdates') IS NULL
  ALTER TABLE dbo.SearchRun ADD FetchGoogleUpdates bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleUpdates_Alt DEFAULT(0);
IF COL_LENGTH('dbo.SearchRun', 'FetchGoogleQuestionsAndAnswers') IS NULL
  ALTER TABLE dbo.SearchRun ADD FetchGoogleQuestionsAndAnswers bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleQuestionsAndAnswers_Alt DEFAULT(0);
IF COL_LENGTH('dbo.SearchRun', 'FetchGoogleSocialProfiles') IS NULL
  ALTER TABLE dbo.SearchRun ADD FetchGoogleSocialProfiles bit NOT NULL CONSTRAINT DF_SearchRun_FetchGoogleSocialProfiles_Alt DEFAULT(0);
IF COL_LENGTH('dbo.SearchRun', 'Status') IS NULL
  ALTER TABLE dbo.SearchRun ADD [Status] nvarchar(20) NOT NULL CONSTRAINT DF_SearchRun_Status_Alt DEFAULT(N'Completed');
IF COL_LENGTH('dbo.SearchRun', 'TotalApiCalls') IS NULL
  ALTER TABLE dbo.SearchRun ADD TotalApiCalls int NULL;
IF COL_LENGTH('dbo.SearchRun', 'CompletedApiCalls') IS NULL
  ALTER TABLE dbo.SearchRun ADD CompletedApiCalls int NULL;
IF COL_LENGTH('dbo.SearchRun', 'PercentComplete') IS NULL
  ALTER TABLE dbo.SearchRun ADD PercentComplete int NULL;
IF COL_LENGTH('dbo.SearchRun', 'StartedUtc') IS NULL
  ALTER TABLE dbo.SearchRun ADD StartedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.SearchRun', 'LastUpdatedUtc') IS NULL
  ALTER TABLE dbo.SearchRun ADD LastUpdatedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.SearchRun', 'CompletedUtc') IS NULL
  ALTER TABLE dbo.SearchRun ADD CompletedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.SearchRun', 'ErrorMessage') IS NULL
  ALTER TABLE dbo.SearchRun ADD ErrorMessage nvarchar(max) NULL;
IF COL_LENGTH('dbo.SearchRun', 'Status') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE dbo.SearchRun
  SET [Status] = N''Completed''
  WHERE [Status] IS NULL OR LTRIM(RTRIM([Status])) = N'''';');
END;
IF COL_LENGTH('dbo.SearchRun', 'LastUpdatedUtc') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE dbo.SearchRun
  SET LastUpdatedUtc = COALESCE(LastUpdatedUtc, RanAtUtc)
  WHERE LastUpdatedUtc IS NULL;');
END;
IF COL_LENGTH('dbo.SearchRun', 'StartedUtc') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE dbo.SearchRun
  SET StartedUtc = COALESCE(StartedUtc, RanAtUtc)
  WHERE StartedUtc IS NULL;');
END;
IF COL_LENGTH('dbo.SearchRun', 'CompletedUtc') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE dbo.SearchRun
  SET CompletedUtc = COALESCE(CompletedUtc, RanAtUtc)
  WHERE CompletedUtc IS NULL
    AND [Status] IN (N''Completed'', N''Failed'');');
END;
IF OBJECT_ID('dbo.Place','U') IS NULL
BEGIN
  CREATE TABLE dbo.Place(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY,
    DisplayName nvarchar(300) NULL,
    LogoUrl nvarchar(1500) NULL,
    MainPhotoUrl nvarchar(1500) NULL,
    PrimaryType nvarchar(80) NULL,
    PrimaryCategory nvarchar(200) NULL,
    TypesCsv nvarchar(1500) NULL,
    FormattedAddress nvarchar(500) NULL,
    Lat decimal(9,6) NULL,
    Lng decimal(9,6) NULL,
    NationalPhoneNumber nvarchar(50) NULL,
    WebsiteUri nvarchar(500) NULL,
    FacebookUrl nvarchar(1500) NULL,
    InstagramUrl nvarchar(1500) NULL,
    LinkedInUrl nvarchar(1500) NULL,
    XUrl nvarchar(1500) NULL,
    YouTubeUrl nvarchar(1500) NULL,
    TikTokUrl nvarchar(1500) NULL,
    PinterestUrl nvarchar(1500) NULL,
    BlueskyUrl nvarchar(1500) NULL,
    Description nvarchar(750) NULL,
    PhotoCount int NULL,
    QuestionAnswerCount int NULL,
    IsServiceAreaBusiness bit NULL,
    BusinessStatus nvarchar(50) NULL,
    SearchLocationName nvarchar(200) NULL,
    RegularOpeningHoursJson nvarchar(max) NULL,
    OpeningDate datetime2(0) NULL,
    SocialProfilesJson nvarchar(max) NULL,
    ServiceAreasJson nvarchar(max) NULL,
    OtherCategoriesJson nvarchar(max) NULL,
    PlaceTopicsJson nvarchar(max) NULL,
    ZohoLeadCreated bit NOT NULL CONSTRAINT DF_Place_ZohoLeadCreated DEFAULT(0),
    ZohoLeadCreatedAtUtc datetime2(0) NULL,
    ZohoLeadId nvarchar(64) NULL,
    ZohoLastSyncAtUtc datetime2(0) NULL,
    ZohoLastError nvarchar(2000) NULL,
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_Place_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.Place', 'LogoUrl') IS NULL
  ALTER TABLE dbo.Place ADD LogoUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'MainPhotoUrl') IS NULL
  ALTER TABLE dbo.Place ADD MainPhotoUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'PrimaryCategory') IS NULL
  ALTER TABLE dbo.Place ADD PrimaryCategory nvarchar(200) NULL;
IF COL_LENGTH('dbo.Place', 'NationalPhoneNumber') IS NULL
  ALTER TABLE dbo.Place ADD NationalPhoneNumber nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'WebsiteUri') IS NULL
  ALTER TABLE dbo.Place ADD WebsiteUri nvarchar(500) NULL;
IF COL_LENGTH('dbo.Place', 'FacebookUrl') IS NULL
  ALTER TABLE dbo.Place ADD FacebookUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'InstagramUrl') IS NULL
  ALTER TABLE dbo.Place ADD InstagramUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'LinkedInUrl') IS NULL
  ALTER TABLE dbo.Place ADD LinkedInUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'XUrl') IS NULL
  ALTER TABLE dbo.Place ADD XUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'YouTubeUrl') IS NULL
  ALTER TABLE dbo.Place ADD YouTubeUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'TikTokUrl') IS NULL
  ALTER TABLE dbo.Place ADD TikTokUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'PinterestUrl') IS NULL
  ALTER TABLE dbo.Place ADD PinterestUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'BlueskyUrl') IS NULL
  ALTER TABLE dbo.Place ADD BlueskyUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.Place', 'Description') IS NULL
  ALTER TABLE dbo.Place ADD Description nvarchar(750) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('dbo.Place') AND name='Description' AND max_length <> 1500)
BEGIN
  UPDATE dbo.Place
  SET Description = LEFT(Description, 750)
  WHERE Description IS NOT NULL
    AND LEN(Description) > 750;
  ALTER TABLE dbo.Place ALTER COLUMN Description nvarchar(750) NULL;
END;
IF COL_LENGTH('dbo.Place', 'PhotoCount') IS NULL
  ALTER TABLE dbo.Place ADD PhotoCount int NULL;
IF COL_LENGTH('dbo.Place', 'QuestionAnswerCount') IS NULL
  ALTER TABLE dbo.Place ADD QuestionAnswerCount int NULL;
IF COL_LENGTH('dbo.Place', 'IsServiceAreaBusiness') IS NULL
  ALTER TABLE dbo.Place ADD IsServiceAreaBusiness bit NULL;
IF COL_LENGTH('dbo.Place', 'BusinessStatus') IS NULL
  ALTER TABLE dbo.Place ADD BusinessStatus nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'SearchLocationName') IS NULL
  ALTER TABLE dbo.Place ADD SearchLocationName nvarchar(200) NULL;
IF COL_LENGTH('dbo.Place', 'RegularOpeningHoursJson') IS NULL
  ALTER TABLE dbo.Place ADD RegularOpeningHoursJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'OpeningDate') IS NULL
  ALTER TABLE dbo.Place ADD OpeningDate datetime2(0) NULL;
IF COL_LENGTH('dbo.Place', 'SocialProfilesJson') IS NULL
  ALTER TABLE dbo.Place ADD SocialProfilesJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'ServiceAreasJson') IS NULL
  ALTER TABLE dbo.Place ADD ServiceAreasJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'OtherCategoriesJson') IS NULL
  ALTER TABLE dbo.Place ADD OtherCategoriesJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'PlaceTopicsJson') IS NULL
  ALTER TABLE dbo.Place ADD PlaceTopicsJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'ZohoLeadCreated') IS NULL
  ALTER TABLE dbo.Place ADD ZohoLeadCreated bit NOT NULL CONSTRAINT DF_Place_ZohoLeadCreated_Alt DEFAULT(0);
IF COL_LENGTH('dbo.Place', 'ZohoLeadCreatedAtUtc') IS NULL
  ALTER TABLE dbo.Place ADD ZohoLeadCreatedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.Place', 'ZohoLeadId') IS NULL
  ALTER TABLE dbo.Place ADD ZohoLeadId nvarchar(64) NULL;
IF COL_LENGTH('dbo.Place', 'ZohoLastSyncAtUtc') IS NULL
  ALTER TABLE dbo.Place ADD ZohoLastSyncAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.Place', 'ZohoLastError') IS NULL
  ALTER TABLE dbo.Place ADD ZohoLastError nvarchar(2000) NULL;
IF OBJECT_ID('dbo.PlacesFinancial','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlacesFinancial(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    DateOfCreation date NULL,
    CompanyNumber nvarchar(32) NOT NULL,
    CompanyType nvarchar(80) NULL,
    LastAccountsFiled datetime2(0) NULL,
    NextAccountsDue datetime2(0) NULL,
    CompanyStatus nvarchar(80) NULL,
    HasLiquidated bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasLiquidated DEFAULT(0),
    HasCharges bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasCharges DEFAULT(0),
    HasInsolvencyHistory bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasInsolvencyHistory DEFAULT(0),
    LastUpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlacesFinancial_LastUpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.PlacesFinancial', 'DateOfCreation') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD DateOfCreation date NULL;
IF COL_LENGTH('dbo.PlacesFinancial', 'CompanyNumber') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD CompanyNumber nvarchar(32) NOT NULL CONSTRAINT DF_PlacesFinancial_CompanyNumber_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlacesFinancial', 'CompanyType') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD CompanyType nvarchar(80) NULL;
IF COL_LENGTH('dbo.PlacesFinancial', 'LastAccountsFiled') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD LastAccountsFiled datetime2(0) NULL;
IF COL_LENGTH('dbo.PlacesFinancial', 'NextAccountsDue') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD NextAccountsDue datetime2(0) NULL;
IF COL_LENGTH('dbo.PlacesFinancial', 'CompanyStatus') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD CompanyStatus nvarchar(80) NULL;
IF COL_LENGTH('dbo.PlacesFinancial', 'HasLiquidated') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD HasLiquidated bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasLiquidated_Alt DEFAULT(0);
IF COL_LENGTH('dbo.PlacesFinancial', 'HasCharges') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD HasCharges bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasCharges_Alt DEFAULT(0);
IF COL_LENGTH('dbo.PlacesFinancial', 'HasInsolvencyHistory') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD HasInsolvencyHistory bit NOT NULL CONSTRAINT DF_PlacesFinancial_HasInsolvencyHistory_Alt DEFAULT(0);
IF COL_LENGTH('dbo.PlacesFinancial', 'LastUpdatedUtc') IS NULL
  ALTER TABLE dbo.PlacesFinancial ADD LastUpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlacesFinancial_LastUpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF OBJECT_ID('dbo.PlacesFinancialOfficers','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlacesFinancialOfficers(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    FirstNames nvarchar(200) NULL,
    LastName nvarchar(200) NULL,
    CountryOfResidence nvarchar(100) NULL,
    DateOfBirth date NULL,
    Nationality nvarchar(100) NULL,
    Role nvarchar(80) NULL,
    Appointed date NULL,
    Resigned date NULL
  );
END;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'FirstNames') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD FirstNames nvarchar(200) NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'LastName') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD LastName nvarchar(200) NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'CountryOfResidence') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD CountryOfResidence nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'DateOfBirth') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD DateOfBirth date NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'Nationality') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD Nationality nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'Role') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD Role nvarchar(80) NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'Appointed') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD Appointed date NULL;
IF COL_LENGTH('dbo.PlacesFinancialOfficers', 'Resigned') IS NULL
  ALTER TABLE dbo.PlacesFinancialOfficers ADD Resigned date NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlacesFinancialOfficers_PlaceId' AND object_id=OBJECT_ID('dbo.PlacesFinancialOfficers'))
  CREATE INDEX IX_PlacesFinancialOfficers_PlaceId ON dbo.PlacesFinancialOfficers(PlaceId, Resigned, Appointed);
IF OBJECT_ID('dbo.PlaceFinancialPersonsOfSignificantControl','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceFinancialPersonsOfSignificantControl(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    CompanyNumber nvarchar(32) NOT NULL,
    PscItemKind nvarchar(120) NULL,
    PscLinkSelf nvarchar(500) NULL,
    PscId nvarchar(120) NULL,
    NameRaw nvarchar(300) NULL,
    FirstNames nvarchar(150) NULL,
    LastName nvarchar(150) NULL,
    CountryOfResidence nvarchar(100) NULL,
    Nationality nvarchar(100) NULL,
    BirthMonth tinyint NULL,
    BirthYear int NULL,
    NotifiedOn date NULL,
    CeasedOn date NULL,
    SourceEtag nvarchar(100) NULL,
    RetrievedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceFinancialPSC_RetrievedUtc DEFAULT SYSUTCDATETIME(),
    RawJson nvarchar(max) NULL
  );
END;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'PlaceId') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD PlaceId nvarchar(128) NOT NULL CONSTRAINT DF_PlaceFinancialPSC_PlaceId_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'CompanyNumber') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD CompanyNumber nvarchar(32) NOT NULL CONSTRAINT DF_PlaceFinancialPSC_CompanyNumber_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'PscItemKind') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD PscItemKind nvarchar(120) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'PscLinkSelf') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD PscLinkSelf nvarchar(500) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'PscId') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD PscId nvarchar(120) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'NameRaw') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD NameRaw nvarchar(300) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'FirstNames') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD FirstNames nvarchar(150) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'LastName') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD LastName nvarchar(150) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'CountryOfResidence') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD CountryOfResidence nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'Nationality') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD Nationality nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'BirthMonth') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD BirthMonth tinyint NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'BirthYear') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD BirthYear int NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'NotifiedOn') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD NotifiedOn date NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'CeasedOn') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD CeasedOn date NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'SourceEtag') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD SourceEtag nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'RetrievedUtc') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD RetrievedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceFinancialPSC_RetrievedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceFinancialPersonsOfSignificantControl', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceFinancialPersonsOfSignificantControl ADD RawJson nvarchar(max) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialPSC_PlaceId' AND object_id=OBJECT_ID('dbo.PlaceFinancialPersonsOfSignificantControl'))
  CREATE INDEX IX_PlaceFinancialPSC_PlaceId ON dbo.PlaceFinancialPersonsOfSignificantControl(PlaceId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialPSC_CompanyNumber' AND object_id=OBJECT_ID('dbo.PlaceFinancialPersonsOfSignificantControl'))
  CREATE INDEX IX_PlaceFinancialPSC_CompanyNumber ON dbo.PlaceFinancialPersonsOfSignificantControl(CompanyNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceFinancialPSC_Place_Kind_PscId' AND object_id=OBJECT_ID('dbo.PlaceFinancialPersonsOfSignificantControl'))
  CREATE UNIQUE INDEX UX_PlaceFinancialPSC_Place_Kind_PscId
    ON dbo.PlaceFinancialPersonsOfSignificantControl(PlaceId, PscItemKind, PscId)
    WHERE PscId IS NOT NULL;
IF OBJECT_ID('dbo.PlaceFinancialPSC_NatureOfControl','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceFinancialPSC_NatureOfControl(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PSCId bigint NOT NULL,
    NatureCode nvarchar(200) NOT NULL
  );
END;
IF COL_LENGTH('dbo.PlaceFinancialPSC_NatureOfControl', 'PSCId') IS NULL
  ALTER TABLE dbo.PlaceFinancialPSC_NatureOfControl ADD PSCId bigint NOT NULL CONSTRAINT DF_PlaceFinancialPSCNature_PSCId_Alt DEFAULT(0);
IF COL_LENGTH('dbo.PlaceFinancialPSC_NatureOfControl', 'NatureCode') IS NULL
  ALTER TABLE dbo.PlaceFinancialPSC_NatureOfControl ADD NatureCode nvarchar(200) NOT NULL CONSTRAINT DF_PlaceFinancialPSCNature_Code_Alt DEFAULT N'';
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.PlaceFinancialPSC_NatureOfControl')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.PlaceFinancialPersonsOfSignificantControl')
    AND pc.name = 'PSCId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.PlaceFinancialPSC_NatureOfControl WITH CHECK
    ADD CONSTRAINT FK_PlaceFinancialPSCNature_PSC FOREIGN KEY (PSCId)
    REFERENCES dbo.PlaceFinancialPersonsOfSignificantControl(Id) ON DELETE CASCADE;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialPSCNature_PSCId' AND object_id=OBJECT_ID('dbo.PlaceFinancialPSC_NatureOfControl'))
  CREATE INDEX IX_PlaceFinancialPSCNature_PSCId ON dbo.PlaceFinancialPSC_NatureOfControl(PSCId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceFinancialPSCNature_PSCId_NatureCode' AND object_id=OBJECT_ID('dbo.PlaceFinancialPSC_NatureOfControl'))
  CREATE UNIQUE INDEX UX_PlaceFinancialPSCNature_PSCId_NatureCode ON dbo.PlaceFinancialPSC_NatureOfControl(PSCId, NatureCode);
IF OBJECT_ID('dbo.PlaceFinancialAccounts','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceFinancialAccounts(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    CompanyNumber nvarchar(32) NOT NULL,
    TransactionId nvarchar(64) NULL,
    FilingDate date NULL,
    MadeUpDate date NULL,
    AccountsType nvarchar(100) NULL,
    DocumentId nvarchar(128) NOT NULL,
    DocumentMetadataUrl nvarchar(500) NOT NULL,
    ContentType nvarchar(100) NULL,
    OriginalFileName nvarchar(260) NULL,
    LocalRelativePath nvarchar(500) NOT NULL,
    FileSizeBytes bigint NULL,
    RetrievedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_RetrievedUtc DEFAULT SYSUTCDATETIME(),
    IsLatest bit NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_IsLatest DEFAULT(0),
    RawJson nvarchar(max) NULL
  );
END;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'PlaceId') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD PlaceId nvarchar(128) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_PlaceId_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'CompanyNumber') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD CompanyNumber nvarchar(32) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_CompanyNumber_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'TransactionId') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD TransactionId nvarchar(64) NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'FilingDate') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD FilingDate date NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'MadeUpDate') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD MadeUpDate date NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'AccountsType') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD AccountsType nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'DocumentId') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD DocumentId nvarchar(128) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_DocumentId_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'DocumentMetadataUrl') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD DocumentMetadataUrl nvarchar(500) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_DocumentMetadataUrl_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'ContentType') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD ContentType nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'OriginalFileName') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD OriginalFileName nvarchar(260) NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'LocalRelativePath') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD LocalRelativePath nvarchar(500) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_LocalRelativePath_Alt DEFAULT N'';
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'FileSizeBytes') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD FileSizeBytes bigint NULL;
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'RetrievedUtc') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD RetrievedUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_RetrievedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'IsLatest') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD IsLatest bit NOT NULL CONSTRAINT DF_PlaceFinancialAccounts_IsLatest_Alt DEFAULT(0);
IF COL_LENGTH('dbo.PlaceFinancialAccounts', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceFinancialAccounts ADD RawJson nvarchar(max) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialAccounts_PlaceId' AND object_id=OBJECT_ID('dbo.PlaceFinancialAccounts'))
  CREATE INDEX IX_PlaceFinancialAccounts_PlaceId ON dbo.PlaceFinancialAccounts(PlaceId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialAccounts_CompanyNumber' AND object_id=OBJECT_ID('dbo.PlaceFinancialAccounts'))
  CREATE INDEX IX_PlaceFinancialAccounts_CompanyNumber ON dbo.PlaceFinancialAccounts(CompanyNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceFinancialAccounts_DocumentId' AND object_id=OBJECT_ID('dbo.PlaceFinancialAccounts'))
  CREATE INDEX IX_PlaceFinancialAccounts_DocumentId ON dbo.PlaceFinancialAccounts(DocumentId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceFinancialAccounts_Place_DocumentId' AND object_id=OBJECT_ID('dbo.PlaceFinancialAccounts'))
  CREATE UNIQUE INDEX UX_PlaceFinancialAccounts_Place_DocumentId ON dbo.PlaceFinancialAccounts(PlaceId, DocumentId);
IF OBJECT_ID('dbo.PlaceSnapshot','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceSnapshot(
    PlaceSnapshotId bigint IDENTITY(1,1) PRIMARY KEY,
    SearchRunId bigint NOT NULL FOREIGN KEY REFERENCES dbo.SearchRun(SearchRunId),
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    RankPosition int NOT NULL,
    Rating decimal(3,2) NULL,
    UserRatingCount int NULL,
    CapturedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceSnapshot_CapturedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceSnapshot_Place_CapturedAt' AND object_id=OBJECT_ID('dbo.PlaceSnapshot'))
  CREATE INDEX IX_PlaceSnapshot_Place_CapturedAt ON dbo.PlaceSnapshot(PlaceId, CapturedAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceSnapshot_SearchRun_Rank' AND object_id=OBJECT_ID('dbo.PlaceSnapshot'))
  CREATE INDEX IX_PlaceSnapshot_SearchRun_Rank ON dbo.PlaceSnapshot(SearchRunId, RankPosition);
IF OBJECT_ID('dbo.PlaceReview','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceReview(
    PlaceReviewId bigint IDENTITY(1,1) PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    ReviewId nvarchar(200) NOT NULL,
    ReviewUrl nvarchar(1500) NULL,
    ProfileName nvarchar(300) NULL,
    ProfileUrl nvarchar(1500) NULL,
    ProfileImageUrl nvarchar(1500) NULL,
    ReviewText nvarchar(max) NULL,
    OriginalReviewText nvarchar(max) NULL,
    OriginalLanguage nvarchar(30) NULL,
    Rating decimal(3,2) NULL,
    ReviewsCount int NULL,
    PhotosCount int NULL,
    LocalGuide bit NULL,
    TimeAgo nvarchar(100) NULL,
    ReviewTimestampUtc datetime2(0) NULL,
    OwnerAnswer nvarchar(max) NULL,
    OriginalOwnerAnswer nvarchar(max) NULL,
    OwnerTimeAgo nvarchar(100) NULL,
    OwnerTimestampUtc datetime2(0) NULL,
    SourceTaskId nvarchar(64) NULL,
    RawJson nvarchar(max) NULL,
    FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_FirstSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileName') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileName nvarchar(300) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ProfileImageUrl') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ProfileImageUrl nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewText') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalReviewText') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalReviewText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalLanguage') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalLanguage nvarchar(30) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'Rating') IS NULL
  ALTER TABLE dbo.PlaceReview ADD Rating decimal(3,2) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewsCount') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewsCount int NULL;
IF COL_LENGTH('dbo.PlaceReview', 'PhotosCount') IS NULL
  ALTER TABLE dbo.PlaceReview ADD PhotosCount int NULL;
IF COL_LENGTH('dbo.PlaceReview', 'LocalGuide') IS NULL
  ALTER TABLE dbo.PlaceReview ADD LocalGuide bit NULL;
IF COL_LENGTH('dbo.PlaceReview', 'TimeAgo') IS NULL
  ALTER TABLE dbo.PlaceReview ADD TimeAgo nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'ReviewTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD ReviewTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerAnswer') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerAnswer nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OriginalOwnerAnswer') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OriginalOwnerAnswer nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerTimeAgo') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerTimeAgo nvarchar(100) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'OwnerTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD OwnerTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'SourceTaskId') IS NULL
  ALTER TABLE dbo.PlaceReview ADD SourceTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceReview ADD RawJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceReview', 'FirstSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_FirstSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceReview', 'LastSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceReview ADD LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceReview_LastSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceReview_Place_Review' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  DROP INDEX UX_PlaceReview_Place_Review ON dbo.PlaceReview;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceReview_Place_Review_NotNull' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE UNIQUE INDEX UX_PlaceReview_Place_Review_NotNull ON dbo.PlaceReview(PlaceId, ReviewId) WHERE ReviewId IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_LastSeen' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_LastSeen ON dbo.PlaceReview(PlaceId, LastSeenUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_ReviewTimestampUtc' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_ReviewTimestampUtc ON dbo.PlaceReview(PlaceId, ReviewTimestampUtc DESC) INCLUDE (Rating, OwnerTimestampUtc);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_OwnerTimestampUtc' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_OwnerTimestampUtc ON dbo.PlaceReview(PlaceId, OwnerTimestampUtc DESC) INCLUDE (ReviewTimestampUtc);
IF OBJECT_ID('dbo.PlaceUpdate','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceUpdate(
    PlaceUpdateId bigint IDENTITY(1,1) PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    UpdateKey nvarchar(128) NOT NULL,
    PostText nvarchar(max) NULL,
    Url nvarchar(1500) NULL,
    ImagesUrlJson nvarchar(max) NULL,
    PostDateUtc datetime2(0) NULL,
    LinksJson nvarchar(max) NULL,
    SourceTaskId nvarchar(64) NULL,
    RawJson nvarchar(max) NULL,
    FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceUpdate_FirstSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceUpdate_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.PlaceUpdate', 'UpdateKey') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD UpdateKey nvarchar(128) NOT NULL CONSTRAINT DF_PlaceUpdate_UpdateKey DEFAULT '';
IF COL_LENGTH('dbo.PlaceUpdate', 'PostText') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD PostText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'Url') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD Url nvarchar(1500) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'ImagesUrlJson') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD ImagesUrlJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'PostDateUtc') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD PostDateUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'LinksJson') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD LinksJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'SourceTaskId') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD SourceTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD RawJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceUpdate', 'FirstSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceUpdate_FirstSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceUpdate', 'LastSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceUpdate ADD LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceUpdate_LastSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceUpdate_Place_UpdateKey' AND object_id=OBJECT_ID('dbo.PlaceUpdate'))
  DROP INDEX UX_PlaceUpdate_Place_UpdateKey ON dbo.PlaceUpdate;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceUpdate_Place_UpdateKey_NotBlank' AND object_id=OBJECT_ID('dbo.PlaceUpdate'))
  CREATE UNIQUE INDEX UX_PlaceUpdate_Place_UpdateKey_NotBlank ON dbo.PlaceUpdate(PlaceId, UpdateKey) WHERE UpdateKey IS NOT NULL AND UpdateKey <> N'';
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceUpdate_Place_PostDateUtc' AND object_id=OBJECT_ID('dbo.PlaceUpdate'))
  CREATE INDEX IX_PlaceUpdate_Place_PostDateUtc ON dbo.PlaceUpdate(PlaceId, PostDateUtc DESC) INCLUDE (LastSeenUtc, Url);
IF OBJECT_ID('dbo.PlaceQuestionAnswer','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceQuestionAnswer(
    PlaceQuestionAnswerId bigint IDENTITY(1,1) PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    QaKey nvarchar(128) NOT NULL,
    QuestionText nvarchar(max) NULL,
    QuestionTimestampUtc datetime2(0) NULL,
    QuestionProfileName nvarchar(300) NULL,
    AnswerText nvarchar(max) NULL,
    AnswerTimestampUtc datetime2(0) NULL,
    AnswerProfileName nvarchar(300) NULL,
    SourceTaskId nvarchar(64) NULL,
    RawJson nvarchar(max) NULL,
    FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceQuestionAnswer_FirstSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceQuestionAnswer_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'QaKey') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD QaKey nvarchar(128) NOT NULL CONSTRAINT DF_PlaceQuestionAnswer_QaKey DEFAULT '';
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'QuestionText') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD QuestionText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'QuestionTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD QuestionTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'QuestionProfileName') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD QuestionProfileName nvarchar(300) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'AnswerText') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD AnswerText nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'AnswerTimestampUtc') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD AnswerTimestampUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'AnswerProfileName') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD AnswerProfileName nvarchar(300) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'SourceTaskId') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD SourceTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'RawJson') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD RawJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'FirstSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceQuestionAnswer_FirstSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.PlaceQuestionAnswer', 'LastSeenUtc') IS NULL
  ALTER TABLE dbo.PlaceQuestionAnswer ADD LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_PlaceQuestionAnswer_LastSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceQuestionAnswer_Place_QaKey' AND object_id=OBJECT_ID('dbo.PlaceQuestionAnswer'))
  DROP INDEX UX_PlaceQuestionAnswer_Place_QaKey ON dbo.PlaceQuestionAnswer;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceQuestionAnswer_Place_QaKey_NotBlank' AND object_id=OBJECT_ID('dbo.PlaceQuestionAnswer'))
  CREATE UNIQUE INDEX UX_PlaceQuestionAnswer_Place_QaKey_NotBlank ON dbo.PlaceQuestionAnswer(PlaceId, QaKey) WHERE QaKey IS NOT NULL AND QaKey <> N'';
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceQuestionAnswer_Place_QuestionTimestampUtc' AND object_id=OBJECT_ID('dbo.PlaceQuestionAnswer'))
  CREATE INDEX IX_PlaceQuestionAnswer_Place_QuestionTimestampUtc ON dbo.PlaceQuestionAnswer(PlaceId, QuestionTimestampUtc DESC) INCLUDE (AnswerTimestampUtc, LastSeenUtc);
;WITH parsed AS (
  SELECT
    u.PlaceUpdateId,
    COALESCE(
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.timestamp') AS datetimeoffset(0)) AS datetime2(0)),
      CASE
        WHEN TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) BETWEEN 1000000000 AND 9999999999
          THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp'))), CAST('1970-01-01T00:00:00' AS datetime2(0)))
        WHEN TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) BETWEEN 1000000000000 AND 9999999999999
          THEN DATEADD(second, TRY_CONVERT(int, TRY_CONVERT(bigint, JSON_VALUE(u.RawJson, '$.timestamp')) / 1000), CAST('1970-01-01T00:00:00' AS datetime2(0)))
        ELSE NULL
      END,
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.post_date') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.date_posted') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.posted_at') AS datetimeoffset(0)) AS datetime2(0)),
      CAST(TRY_CAST(JSON_VALUE(u.RawJson, '$.date') AS datetimeoffset(0)) AS datetime2(0)),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.timestamp'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.post_date'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.date_posted'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.posted_at'), 112),
      TRY_CONVERT(datetime2(0), JSON_VALUE(u.RawJson, '$.date'), 112)
    ) AS ParsedPostDateUtc
  FROM dbo.PlaceUpdate u
  WHERE u.RawJson IS NOT NULL
)
UPDATE u
SET u.PostDateUtc = p.ParsedPostDateUtc
FROM dbo.PlaceUpdate u
JOIN parsed p ON p.PlaceUpdateId = u.PlaceUpdateId
WHERE p.ParsedPostDateUtc IS NOT NULL
  AND (
    u.PostDateUtc IS NULL
    OR u.PostDateUtc < '2005-01-01'
    OR u.PostDateUtc > DATEADD(day, 1, SYSUTCDATETIME())
  );
IF OBJECT_ID('dbo.PlaceReviewVelocityStats','U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceReviewVelocityStats(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    AsOfUtc datetime2(0) NOT NULL,
    ReviewsLast90 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast90 DEFAULT(0),
    ReviewsLast180 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast180 DEFAULT(0),
    ReviewsLast270 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast270 DEFAULT(0),
    ReviewsLast365 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_ReviewsLast365 DEFAULT(0),
    AvgPerMonth12m decimal(6,2) NULL,
    Prev90 int NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_Prev90 DEFAULT(0),
    Trend90Pct decimal(7,2) NULL,
    DaysSinceLastReview int NULL,
    LastReviewTimestampUtc datetime2(0) NULL,
    LongestGapDays12m int NULL,
    RespondedPct12m decimal(7,2) NULL,
    AvgOwnerResponseHours12m decimal(8,2) NULL,
    MomentumScore int NULL,
    StatusLabel varchar(20) NOT NULL CONSTRAINT DF_PlaceReviewVelocityStats_StatusLabel DEFAULT('NoReviews')
  );
END;
EXEC('CREATE OR ALTER PROCEDURE dbo.usp_RecomputePlaceReviewVelocityStats @PlaceId nvarchar(128) AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @AsOfUtc datetime2(0) = SYSUTCDATETIME();
  DECLARE @WindowStart12m datetime2(0) = DATEADD(day,-365,@AsOfUtc);
  DECLARE @ReviewsLast90 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-90,@AsOfUtc));
  DECLARE @ReviewsLast180 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-180,@AsOfUtc));
  DECLARE @ReviewsLast270 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-270,@AsOfUtc));
  DECLARE @ReviewsLast365 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m);
  DECLARE @Prev90 int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= DATEADD(day,-180,@AsOfUtc) AND ReviewTimestampUtc < DATEADD(day,-90,@AsOfUtc));
  DECLARE @LastReviewTimestampUtc datetime2(0) = (SELECT MAX(ReviewTimestampUtc) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId);
  DECLARE @DaysSinceLastReview int = CASE WHEN @LastReviewTimestampUtc IS NULL THEN NULL ELSE DATEDIFF(day,@LastReviewTimestampUtc,@AsOfUtc) END;
  DECLARE @Trend90Pct decimal(7,2) = CASE WHEN @Prev90 = 0 AND @ReviewsLast90 = 0 THEN 0 WHEN @Prev90 = 0 AND @ReviewsLast90 > 0 THEN 100 ELSE CAST(((@ReviewsLast90 - @Prev90) * 100.0) / NULLIF(@Prev90,0) AS decimal(7,2)) END;

  DECLARE @Total12m int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m);
  DECLARE @Replied12m int = (SELECT COUNT(1) FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m AND OwnerTimestampUtc IS NOT NULL AND OwnerTimestampUtc >= ReviewTimestampUtc);
  DECLARE @RespondedPct12m decimal(7,2) = CASE WHEN @Total12m = 0 THEN NULL ELSE CAST((@Replied12m * 100.0) / NULLIF(@Total12m,0) AS decimal(7,2)) END;
  DECLARE @AvgOwnerResponseHours12m decimal(8,2) = (
    SELECT CAST(AVG(DATEDIFF(minute, ReviewTimestampUtc, OwnerTimestampUtc) / 60.0) AS decimal(8,2))
    FROM dbo.PlaceReview
    WHERE PlaceId=@PlaceId
      AND ReviewTimestampUtc >= @WindowStart12m
      AND OwnerTimestampUtc IS NOT NULL
      AND OwnerTimestampUtc >= ReviewTimestampUtc
  );

  DECLARE @LongestGapDays12m int = NULL;
  ;WITH review_points AS (
    SELECT CAST(@WindowStart12m AS datetime2(0)) AS PointUtc
    UNION ALL
    SELECT ReviewTimestampUtc FROM dbo.PlaceReview WHERE PlaceId=@PlaceId AND ReviewTimestampUtc >= @WindowStart12m
    UNION ALL
    SELECT @AsOfUtc
  ), gap_points AS (
    SELECT PointUtc, LEAD(PointUtc) OVER (ORDER BY PointUtc) AS NextPointUtc FROM review_points
  )
  SELECT @LongestGapDays12m = MAX(CASE WHEN NextPointUtc IS NULL THEN 0 ELSE DATEDIFF(day, PointUtc, NextPointUtc) END)
  FROM gap_points;

  DECLARE @RecencyScore decimal(9,2) = CASE
    WHEN @DaysSinceLastReview IS NULL THEN 0
    WHEN @DaysSinceLastReview <= 7 THEN 100
    WHEN @DaysSinceLastReview <= 30 THEN 100 - ((@DaysSinceLastReview - 7) * (40.0 / 23.0))
    WHEN @DaysSinceLastReview <= 60 THEN 60 - ((@DaysSinceLastReview - 30) * (30.0 / 30.0))
    ELSE 0 END;

  DECLARE @MeanMonthly decimal(9,4), @StdMonthly decimal(9,4), @ConsistencyScore decimal(9,2);
  ;WITH months AS (
    SELECT 0 AS n, DATEFROMPARTS(YEAR(DATEADD(month,-11,@AsOfUtc)), MONTH(DATEADD(month,-11,@AsOfUtc)), 1) AS MonthStart
    UNION ALL
    SELECT n + 1, DATEADD(month,1,MonthStart) FROM months WHERE n < 11
  ), bucket AS (
    SELECT m.MonthStart, COUNT(r.PlaceReviewId) AS Cnt
    FROM months m
    LEFT JOIN dbo.PlaceReview r ON r.PlaceId=@PlaceId AND r.ReviewTimestampUtc >= m.MonthStart AND r.ReviewTimestampUtc < DATEADD(month,1,m.MonthStart)
    GROUP BY m.MonthStart
  )
  SELECT @MeanMonthly = AVG(CAST(Cnt AS decimal(9,4))), @StdMonthly = STDEV(CAST(Cnt AS decimal(9,4))) FROM bucket OPTION (MAXRECURSION 20);

  SET @ConsistencyScore = CASE WHEN ISNULL(@MeanMonthly,0) = 0 THEN 0 ELSE CASE WHEN (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) < 0 THEN 0 WHEN (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) > 100 THEN 100 ELSE (100 - ((ISNULL(@StdMonthly,0)/NULLIF(@MeanMonthly,0))*100.0)) END END;

  DECLARE @GrowthScore decimal(9,2);
  DECLARE @TrendClamped decimal(9,2) = CASE WHEN @Trend90Pct < -100 THEN -100 WHEN @Trend90Pct > 200 THEN 200 ELSE @Trend90Pct END;
  SET @GrowthScore = CASE
    WHEN @TrendClamped <= 0 THEN (@TrendClamped + 100) * 0.5
    WHEN @TrendClamped <= 100 THEN 50 + (@TrendClamped * 0.25)
    ELSE 75 + ((@TrendClamped - 100) * 0.25)
  END;

  DECLARE @MomentumScore int = CAST(ROUND((0.45 * @RecencyScore) + (0.25 * @ConsistencyScore) + (0.30 * @GrowthScore),0) AS int);
  IF @MomentumScore < 0 SET @MomentumScore = 0;
  IF @MomentumScore > 100 SET @MomentumScore = 100;

  DECLARE @StatusLabel varchar(20) = CASE
    WHEN @LastReviewTimestampUtc IS NULL THEN ''NoReviews''
    WHEN @DaysSinceLastReview > 60 THEN ''Stalled''
    WHEN @Trend90Pct <= -30 THEN ''Slowing''
    WHEN @Trend90Pct >= 30 AND @DaysSinceLastReview <= 30 THEN ''Accelerating''
    ELSE ''Healthy'' END;

  MERGE dbo.PlaceReviewVelocityStats AS target
  USING (SELECT @PlaceId AS PlaceId) AS source
  ON target.PlaceId = source.PlaceId
  WHEN MATCHED THEN UPDATE SET
    AsOfUtc=@AsOfUtc,
    ReviewsLast90=@ReviewsLast90,
    ReviewsLast180=@ReviewsLast180,
    ReviewsLast270=@ReviewsLast270,
    ReviewsLast365=@ReviewsLast365,
    AvgPerMonth12m=CAST(@ReviewsLast365 / 12.0 AS decimal(6,2)),
    Prev90=@Prev90,
    Trend90Pct=@Trend90Pct,
    DaysSinceLastReview=@DaysSinceLastReview,
    LastReviewTimestampUtc=@LastReviewTimestampUtc,
    LongestGapDays12m=@LongestGapDays12m,
    RespondedPct12m=@RespondedPct12m,
    AvgOwnerResponseHours12m=@AvgOwnerResponseHours12m,
    MomentumScore=@MomentumScore,
    StatusLabel=@StatusLabel
  WHEN NOT MATCHED THEN INSERT(
    PlaceId,AsOfUtc,ReviewsLast90,ReviewsLast180,ReviewsLast270,ReviewsLast365,AvgPerMonth12m,Prev90,Trend90Pct,DaysSinceLastReview,LastReviewTimestampUtc,LongestGapDays12m,RespondedPct12m,AvgOwnerResponseHours12m,MomentumScore,StatusLabel
  ) VALUES(
    @PlaceId,@AsOfUtc,@ReviewsLast90,@ReviewsLast180,@ReviewsLast270,@ReviewsLast365,CAST(@ReviewsLast365 / 12.0 AS decimal(6,2)),@Prev90,@Trend90Pct,@DaysSinceLastReview,@LastReviewTimestampUtc,@LongestGapDays12m,@RespondedPct12m,@AvgOwnerResponseHours12m,@MomentumScore,@StatusLabel
  );
END');
IF OBJECT_ID('dbo.DataForSeoReviewTask','U') IS NULL
BEGIN
  CREATE TABLE dbo.DataForSeoReviewTask(
    DataForSeoReviewTaskId bigint IDENTITY(1,1) PRIMARY KEY,
    DataForSeoTaskId nvarchar(64) NOT NULL,
    TaskType nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_TaskType DEFAULT 'reviews',
    PlaceId nvarchar(128) NOT NULL FOREIGN KEY REFERENCES dbo.Place(PlaceId),
    LocationName nvarchar(200) NULL,
    Status nvarchar(40) NOT NULL,
    TaskStatusCode int NULL,
    TaskStatusMessage nvarchar(500) NULL,
    Endpoint nvarchar(500) NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    LastCheckedUtc datetime2(0) NULL,
    ReadyAtUtc datetime2(0) NULL,
    PopulatedAtUtc datetime2(0) NULL,
    LastAttemptedPopulateUtc datetime2(0) NULL,
    LastPopulateReviewCount int NULL,
    CallbackReceivedAtUtc datetime2(0) NULL,
    CallbackTaskId nvarchar(64) NULL,
    LastError nvarchar(2000) NULL
  );
END;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskType') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskType nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_TaskType_Alt DEFAULT 'reviews';
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LocationName') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LocationName nvarchar(200) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'Status') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD Status nvarchar(40) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_Status DEFAULT 'Created';
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskStatusCode') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskStatusCode int NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskStatusMessage') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD TaskStatusMessage nvarchar(500) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'Endpoint') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD Endpoint nvarchar(500) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CreatedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_DataForSeoReviewTask_CreatedAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastCheckedUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastCheckedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'ReadyAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD ReadyAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'PopulatedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD PopulatedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastAttemptedPopulateUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastAttemptedPopulateUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastPopulateReviewCount') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastPopulateReviewCount int NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CallbackReceivedAtUtc') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CallbackReceivedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'CallbackTaskId') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD CallbackTaskId nvarchar(64) NULL;
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'LastError') IS NULL
  ALTER TABLE dbo.DataForSeoReviewTask ADD LastError nvarchar(2000) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_DataForSeoReviewTask_TaskId' AND object_id=OBJECT_ID('dbo.DataForSeoReviewTask'))
  CREATE UNIQUE INDEX UX_DataForSeoReviewTask_TaskId ON dbo.DataForSeoReviewTask(DataForSeoTaskId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DataForSeoReviewTask_Status_Created' AND object_id=OBJECT_ID('dbo.DataForSeoReviewTask'))
  CREATE INDEX IX_DataForSeoReviewTask_Status_Created ON dbo.DataForSeoReviewTask(Status, CreatedAtUtc DESC);
IF COL_LENGTH('dbo.DataForSeoReviewTask', 'Status') IS NOT NULL
   AND COL_LENGTH('dbo.DataForSeoReviewTask', 'TaskStatusMessage') IS NOT NULL
   AND COL_LENGTH('dbo.DataForSeoReviewTask', 'LastError') IS NOT NULL
BEGIN
  EXEC(N'
UPDATE dbo.DataForSeoReviewTask
SET
  Status=''NoData'',
  LastError=NULL
WHERE Status=''Error''
  AND (
    TaskStatusMessage LIKE ''%No Search Results%''
    OR LastError LIKE ''%No Search Results%''
  );');
END;
IF OBJECT_ID('dbo.AppSettings','U') IS NULL
BEGIN
  CREATE TABLE dbo.AppSettings(
    AppSettingsId int NOT NULL PRIMARY KEY,
    EnhancedGoogleDataRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_EnhancedGoogleDataRefreshHours DEFAULT(24),
    GoogleReviewsRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleReviewsRefreshHours DEFAULT(24),
    GoogleUpdatesRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleUpdatesRefreshHours DEFAULT(24),
    GoogleQuestionsAndAnswersRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleQuestionsAndAnswersRefreshHours DEFAULT(24),
    GoogleSocialProfilesRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleSocialProfilesRefreshHours DEFAULT(24),
    SearchVolumeRefreshCooldownDays int NOT NULL CONSTRAINT DF_AppSettings_SearchVolumeRefreshCooldownDays DEFAULT(30),
    MapPackClickSharePercent int NOT NULL CONSTRAINT DF_AppSettings_MapPackClickSharePercent DEFAULT(50),
    MapPackCtrPosition1Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition1Percent DEFAULT(38),
    MapPackCtrPosition2Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition2Percent DEFAULT(23),
    MapPackCtrPosition3Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition3Percent DEFAULT(16),
    MapPackCtrPosition4Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition4Percent DEFAULT(7),
    MapPackCtrPosition5Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition5Percent DEFAULT(5),
    MapPackCtrPosition6Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition6Percent DEFAULT(4),
    MapPackCtrPosition7Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition7Percent DEFAULT(3),
    MapPackCtrPosition8Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition8Percent DEFAULT(2),
    MapPackCtrPosition9Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition9Percent DEFAULT(1),
    MapPackCtrPosition10Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition10Percent DEFAULT(1),
    ZohoLeadOwnerName nvarchar(200) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadOwnerName DEFAULT(N'Richard Howes'),
    ZohoLeadOwnerId nvarchar(50) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadOwnerId DEFAULT(N'1108404000000068001'),
    ZohoLeadNextAction nvarchar(300) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadNextAction DEFAULT(N'Make first contact'),
    SiteUrl nvarchar(500) NOT NULL CONSTRAINT DF_AppSettings_SiteUrl DEFAULT(N'https://briskly-viceless-kayleen.ngrok-free.dev/'),
    MinimumPasswordLength int NOT NULL CONSTRAINT DF_AppSettings_MinimumPasswordLength DEFAULT(12),
    PasswordRequiresNumber bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresNumber DEFAULT(1),
    PasswordRequiresCapitalLetter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresCapitalLetter DEFAULT(1),
    PasswordRequiresSpecialCharacter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresSpecialCharacter DEFAULT(1),
    LoginLockoutThreshold int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutThreshold DEFAULT(5),
    LoginLockoutMinutes int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutMinutes DEFAULT(15),
    EmailCodeCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeCooldownSeconds DEFAULT(60),
    EmailCodeMaxPerHourPerEmail int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerEmail DEFAULT(10),
    EmailCodeMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerIp DEFAULT(50),
    EmailCodeExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeExpiryMinutes DEFAULT(10),
    EmailCodeMaxFailedAttemptsPerCode int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxFailedAttemptsPerCode DEFAULT(5),
    InviteExpiryHours int NOT NULL CONSTRAINT DF_AppSettings_InviteExpiryHours DEFAULT(24),
    InviteOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpExpiryMinutes DEFAULT(10),
    InviteOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpCooldownSeconds DEFAULT(60),
    InviteOtpMaxPerHourPerInvite int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerInvite DEFAULT(3),
    InviteOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerIp DEFAULT(25),
    InviteOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxAttempts DEFAULT(5),
    InviteOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpLockMinutes DEFAULT(15),
    InviteMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteMaxAttempts DEFAULT(10),
    InviteLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteLockMinutes DEFAULT(15),
    ChangePasswordOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpExpiryMinutes DEFAULT(10),
    ChangePasswordOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpCooldownSeconds DEFAULT(60),
    ChangePasswordOtpMaxPerHourPerUser int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerUser DEFAULT(3),
    ChangePasswordOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerIp DEFAULT(25),
    ChangePasswordOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxAttempts DEFAULT(5),
    ChangePasswordOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpLockMinutes DEFAULT(15),
    UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_AppSettings_UpdatedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.AppSettings', 'EnhancedGoogleDataRefreshHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD EnhancedGoogleDataRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_EnhancedGoogleDataRefreshHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'GoogleReviewsRefreshHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD GoogleReviewsRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleReviewsRefreshHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'GoogleUpdatesRefreshHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD GoogleUpdatesRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleUpdatesRefreshHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'GoogleQuestionsAndAnswersRefreshHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD GoogleQuestionsAndAnswersRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleQuestionsAndAnswersRefreshHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'GoogleSocialProfilesRefreshHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD GoogleSocialProfilesRefreshHours int NOT NULL CONSTRAINT DF_AppSettings_GoogleSocialProfilesRefreshHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'SearchVolumeRefreshCooldownDays') IS NULL
  ALTER TABLE dbo.AppSettings ADD SearchVolumeRefreshCooldownDays int NOT NULL CONSTRAINT DF_AppSettings_SearchVolumeRefreshCooldownDays_Alt DEFAULT(30);
IF COL_LENGTH('dbo.AppSettings', 'MapPackClickSharePercent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackClickSharePercent int NOT NULL CONSTRAINT DF_AppSettings_MapPackClickSharePercent_Alt DEFAULT(50);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition1Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition1Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition1Percent_Alt DEFAULT(38);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition2Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition2Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition2Percent_Alt DEFAULT(23);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition3Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition3Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition3Percent_Alt DEFAULT(16);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition4Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition4Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition4Percent_Alt DEFAULT(7);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition5Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition5Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition5Percent_Alt DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition6Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition6Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition6Percent_Alt DEFAULT(4);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition7Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition7Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition7Percent_Alt DEFAULT(3);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition8Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition8Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition8Percent_Alt DEFAULT(2);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition9Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition9Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition9Percent_Alt DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'MapPackCtrPosition10Percent') IS NULL
  ALTER TABLE dbo.AppSettings ADD MapPackCtrPosition10Percent int NOT NULL CONSTRAINT DF_AppSettings_MapPackCtrPosition10Percent_Alt DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'ZohoLeadOwnerName') IS NULL
  ALTER TABLE dbo.AppSettings ADD ZohoLeadOwnerName nvarchar(200) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadOwnerName_Alt DEFAULT(N'Richard Howes');
IF COL_LENGTH('dbo.AppSettings', 'ZohoLeadOwnerId') IS NULL
  ALTER TABLE dbo.AppSettings ADD ZohoLeadOwnerId nvarchar(50) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadOwnerId_Alt DEFAULT(N'1108404000000068001');
IF COL_LENGTH('dbo.AppSettings', 'ZohoLeadNextAction') IS NULL
  ALTER TABLE dbo.AppSettings ADD ZohoLeadNextAction nvarchar(300) NOT NULL CONSTRAINT DF_AppSettings_ZohoLeadNextAction_Alt DEFAULT(N'Make first contact');
IF COL_LENGTH('dbo.AppSettings', 'SiteUrl') IS NULL
  ALTER TABLE dbo.AppSettings ADD SiteUrl nvarchar(500) NOT NULL CONSTRAINT DF_AppSettings_SiteUrl_Alt DEFAULT(N'https://briskly-viceless-kayleen.ngrok-free.dev/');
IF COL_LENGTH('dbo.AppSettings', 'MinimumPasswordLength') IS NULL
  ALTER TABLE dbo.AppSettings ADD MinimumPasswordLength int NOT NULL CONSTRAINT DF_AppSettings_MinimumPasswordLength_Alt DEFAULT(12);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresNumber') IS NULL
  ALTER TABLE dbo.AppSettings ADD PasswordRequiresNumber bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresNumber_Alt DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresCapitalLetter') IS NULL
  ALTER TABLE dbo.AppSettings ADD PasswordRequiresCapitalLetter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresCapitalLetter_Alt DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'PasswordRequiresSpecialCharacter') IS NULL
  ALTER TABLE dbo.AppSettings ADD PasswordRequiresSpecialCharacter bit NOT NULL CONSTRAINT DF_AppSettings_PasswordRequiresSpecialCharacter_Alt DEFAULT(1);
IF COL_LENGTH('dbo.AppSettings', 'LoginLockoutThreshold') IS NULL
  ALTER TABLE dbo.AppSettings ADD LoginLockoutThreshold int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutThreshold_Alt DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'LoginLockoutMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD LoginLockoutMinutes int NOT NULL CONSTRAINT DF_AppSettings_LoginLockoutMinutes_Alt DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeCooldownSeconds') IS NULL
  ALTER TABLE dbo.AppSettings ADD EmailCodeCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeCooldownSeconds_Alt DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxPerHourPerEmail') IS NULL
  ALTER TABLE dbo.AppSettings ADD EmailCodeMaxPerHourPerEmail int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerEmail_Alt DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxPerHourPerIp') IS NULL
  ALTER TABLE dbo.AppSettings ADD EmailCodeMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxPerHourPerIp_Alt DEFAULT(50);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeExpiryMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD EmailCodeExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeExpiryMinutes_Alt DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'EmailCodeMaxFailedAttemptsPerCode') IS NULL
  ALTER TABLE dbo.AppSettings ADD EmailCodeMaxFailedAttemptsPerCode int NOT NULL CONSTRAINT DF_AppSettings_EmailCodeMaxFailedAttemptsPerCode_Alt DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'InviteExpiryHours') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteExpiryHours int NOT NULL CONSTRAINT DF_AppSettings_InviteExpiryHours_Alt DEFAULT(24);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpExpiryMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpExpiryMinutes_Alt DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpCooldownSeconds') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpCooldownSeconds_Alt DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxPerHourPerInvite') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpMaxPerHourPerInvite int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerInvite_Alt DEFAULT(3);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxPerHourPerIp') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxPerHourPerIp_Alt DEFAULT(25);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpMaxAttempts') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpMaxAttempts_Alt DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'InviteOtpLockMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteOtpLockMinutes_Alt DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'InviteMaxAttempts') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_InviteMaxAttempts_Alt DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'InviteLockMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD InviteLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_InviteLockMinutes_Alt DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpExpiryMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpExpiryMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpExpiryMinutes_Alt DEFAULT(10);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpCooldownSeconds') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpCooldownSeconds int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpCooldownSeconds_Alt DEFAULT(60);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxPerHourPerUser') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxPerHourPerUser int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerUser_Alt DEFAULT(3);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxPerHourPerIp') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxPerHourPerIp int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxPerHourPerIp_Alt DEFAULT(25);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpMaxAttempts') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpMaxAttempts int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpMaxAttempts_Alt DEFAULT(5);
IF COL_LENGTH('dbo.AppSettings', 'ChangePasswordOtpLockMinutes') IS NULL
  ALTER TABLE dbo.AppSettings ADD ChangePasswordOtpLockMinutes int NOT NULL CONSTRAINT DF_AppSettings_ChangePasswordOtpLockMinutes_Alt DEFAULT(15);
IF COL_LENGTH('dbo.AppSettings', 'UpdatedAtUtc') IS NULL
  ALTER TABLE dbo.AppSettings ADD UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_AppSettings_UpdatedAtUtc_Alt DEFAULT SYSUTCDATETIME();
EXEC(N'
MERGE dbo.AppSettings AS target
USING (SELECT CAST(1 AS int) AS AppSettingsId) AS source
ON target.AppSettingsId = source.AppSettingsId
WHEN MATCHED THEN UPDATE SET
  EnhancedGoogleDataRefreshHours = CASE WHEN target.EnhancedGoogleDataRefreshHours IS NULL OR target.EnhancedGoogleDataRefreshHours < 0 THEN 24 ELSE target.EnhancedGoogleDataRefreshHours END,
  GoogleReviewsRefreshHours = CASE WHEN target.GoogleReviewsRefreshHours IS NULL OR target.GoogleReviewsRefreshHours < 0 THEN 24 ELSE target.GoogleReviewsRefreshHours END,
  GoogleUpdatesRefreshHours = CASE WHEN target.GoogleUpdatesRefreshHours IS NULL OR target.GoogleUpdatesRefreshHours < 0 THEN 24 ELSE target.GoogleUpdatesRefreshHours END,
  GoogleQuestionsAndAnswersRefreshHours = CASE WHEN target.GoogleQuestionsAndAnswersRefreshHours IS NULL OR target.GoogleQuestionsAndAnswersRefreshHours < 0 THEN 24 ELSE target.GoogleQuestionsAndAnswersRefreshHours END,
  GoogleSocialProfilesRefreshHours = CASE WHEN target.GoogleSocialProfilesRefreshHours IS NULL OR target.GoogleSocialProfilesRefreshHours < 0 THEN 24 ELSE target.GoogleSocialProfilesRefreshHours END,
  SearchVolumeRefreshCooldownDays = CASE WHEN target.SearchVolumeRefreshCooldownDays IS NULL OR target.SearchVolumeRefreshCooldownDays < 0 THEN 30 ELSE target.SearchVolumeRefreshCooldownDays END,
  MapPackClickSharePercent = CASE WHEN target.MapPackClickSharePercent IS NULL OR target.MapPackClickSharePercent < 0 OR target.MapPackClickSharePercent > 100 THEN 50 ELSE target.MapPackClickSharePercent END,
  MapPackCtrPosition1Percent = CASE WHEN target.MapPackCtrPosition1Percent IS NULL OR target.MapPackCtrPosition1Percent < 0 OR target.MapPackCtrPosition1Percent > 100 THEN 38 ELSE target.MapPackCtrPosition1Percent END,
  MapPackCtrPosition2Percent = CASE WHEN target.MapPackCtrPosition2Percent IS NULL OR target.MapPackCtrPosition2Percent < 0 OR target.MapPackCtrPosition2Percent > 100 THEN 23 ELSE target.MapPackCtrPosition2Percent END,
  MapPackCtrPosition3Percent = CASE WHEN target.MapPackCtrPosition3Percent IS NULL OR target.MapPackCtrPosition3Percent < 0 OR target.MapPackCtrPosition3Percent > 100 THEN 16 ELSE target.MapPackCtrPosition3Percent END,
  MapPackCtrPosition4Percent = CASE WHEN target.MapPackCtrPosition4Percent IS NULL OR target.MapPackCtrPosition4Percent < 0 OR target.MapPackCtrPosition4Percent > 100 THEN 7 ELSE target.MapPackCtrPosition4Percent END,
  MapPackCtrPosition5Percent = CASE WHEN target.MapPackCtrPosition5Percent IS NULL OR target.MapPackCtrPosition5Percent < 0 OR target.MapPackCtrPosition5Percent > 100 THEN 5 ELSE target.MapPackCtrPosition5Percent END,
  MapPackCtrPosition6Percent = CASE WHEN target.MapPackCtrPosition6Percent IS NULL OR target.MapPackCtrPosition6Percent < 0 OR target.MapPackCtrPosition6Percent > 100 THEN 4 ELSE target.MapPackCtrPosition6Percent END,
  MapPackCtrPosition7Percent = CASE WHEN target.MapPackCtrPosition7Percent IS NULL OR target.MapPackCtrPosition7Percent < 0 OR target.MapPackCtrPosition7Percent > 100 THEN 3 ELSE target.MapPackCtrPosition7Percent END,
  MapPackCtrPosition8Percent = CASE WHEN target.MapPackCtrPosition8Percent IS NULL OR target.MapPackCtrPosition8Percent < 0 OR target.MapPackCtrPosition8Percent > 100 THEN 2 ELSE target.MapPackCtrPosition8Percent END,
  MapPackCtrPosition9Percent = CASE WHEN target.MapPackCtrPosition9Percent IS NULL OR target.MapPackCtrPosition9Percent < 0 OR target.MapPackCtrPosition9Percent > 100 THEN 1 ELSE target.MapPackCtrPosition9Percent END,
  MapPackCtrPosition10Percent = CASE WHEN target.MapPackCtrPosition10Percent IS NULL OR target.MapPackCtrPosition10Percent < 0 OR target.MapPackCtrPosition10Percent > 100 THEN 1 ELSE target.MapPackCtrPosition10Percent END,
  ZohoLeadOwnerName = CASE WHEN target.ZohoLeadOwnerName IS NULL OR LEN(LTRIM(RTRIM(target.ZohoLeadOwnerName))) = 0 THEN N''Richard Howes'' ELSE LEFT(target.ZohoLeadOwnerName, 200) END,
  ZohoLeadOwnerId = CASE WHEN target.ZohoLeadOwnerId IS NULL OR LEN(LTRIM(RTRIM(target.ZohoLeadOwnerId))) = 0 THEN N''1108404000000068001'' ELSE LEFT(target.ZohoLeadOwnerId, 50) END,
  ZohoLeadNextAction = CASE WHEN target.ZohoLeadNextAction IS NULL OR LEN(LTRIM(RTRIM(target.ZohoLeadNextAction))) = 0 THEN N''Make first contact'' ELSE LEFT(target.ZohoLeadNextAction, 300) END,
  SiteUrl = CASE WHEN target.SiteUrl IS NULL OR LEN(LTRIM(RTRIM(target.SiteUrl))) = 0 THEN N''https://briskly-viceless-kayleen.ngrok-free.dev/'' ELSE LEFT(target.SiteUrl, 500) END,
  MinimumPasswordLength = CASE WHEN target.MinimumPasswordLength IS NULL OR target.MinimumPasswordLength < 8 THEN 12 WHEN target.MinimumPasswordLength > 128 THEN 128 ELSE target.MinimumPasswordLength END,
  PasswordRequiresNumber = ISNULL(target.PasswordRequiresNumber, 1),
  PasswordRequiresCapitalLetter = ISNULL(target.PasswordRequiresCapitalLetter, 1),
  PasswordRequiresSpecialCharacter = ISNULL(target.PasswordRequiresSpecialCharacter, 1),
  LoginLockoutThreshold = CASE WHEN target.LoginLockoutThreshold IS NULL OR target.LoginLockoutThreshold < 1 THEN 5 ELSE target.LoginLockoutThreshold END,
  LoginLockoutMinutes = CASE WHEN target.LoginLockoutMinutes IS NULL OR target.LoginLockoutMinutes < 1 THEN 15 ELSE target.LoginLockoutMinutes END,
  EmailCodeCooldownSeconds = CASE WHEN target.EmailCodeCooldownSeconds IS NULL OR target.EmailCodeCooldownSeconds < 1 THEN 60 ELSE target.EmailCodeCooldownSeconds END,
  EmailCodeMaxPerHourPerEmail = CASE WHEN target.EmailCodeMaxPerHourPerEmail IS NULL OR target.EmailCodeMaxPerHourPerEmail < 1 THEN 10 ELSE target.EmailCodeMaxPerHourPerEmail END,
  EmailCodeMaxPerHourPerIp = CASE WHEN target.EmailCodeMaxPerHourPerIp IS NULL OR target.EmailCodeMaxPerHourPerIp < 1 THEN 50 ELSE target.EmailCodeMaxPerHourPerIp END,
  EmailCodeExpiryMinutes = CASE WHEN target.EmailCodeExpiryMinutes IS NULL OR target.EmailCodeExpiryMinutes < 1 THEN 10 ELSE target.EmailCodeExpiryMinutes END,
  EmailCodeMaxFailedAttemptsPerCode = CASE WHEN target.EmailCodeMaxFailedAttemptsPerCode IS NULL OR target.EmailCodeMaxFailedAttemptsPerCode < 1 THEN 5 ELSE target.EmailCodeMaxFailedAttemptsPerCode END,
  InviteExpiryHours = CASE WHEN target.InviteExpiryHours IS NULL OR target.InviteExpiryHours < 1 THEN 24 ELSE target.InviteExpiryHours END,
  InviteOtpExpiryMinutes = CASE WHEN target.InviteOtpExpiryMinutes IS NULL OR target.InviteOtpExpiryMinutes < 1 THEN 10 ELSE target.InviteOtpExpiryMinutes END,
  InviteOtpCooldownSeconds = CASE WHEN target.InviteOtpCooldownSeconds IS NULL OR target.InviteOtpCooldownSeconds < 1 THEN 60 ELSE target.InviteOtpCooldownSeconds END,
  InviteOtpMaxPerHourPerInvite = CASE WHEN target.InviteOtpMaxPerHourPerInvite IS NULL OR target.InviteOtpMaxPerHourPerInvite < 1 THEN 3 ELSE target.InviteOtpMaxPerHourPerInvite END,
  InviteOtpMaxPerHourPerIp = CASE WHEN target.InviteOtpMaxPerHourPerIp IS NULL OR target.InviteOtpMaxPerHourPerIp < 1 THEN 25 ELSE target.InviteOtpMaxPerHourPerIp END,
  InviteOtpMaxAttempts = CASE WHEN target.InviteOtpMaxAttempts IS NULL OR target.InviteOtpMaxAttempts < 1 THEN 5 ELSE target.InviteOtpMaxAttempts END,
  InviteOtpLockMinutes = CASE WHEN target.InviteOtpLockMinutes IS NULL OR target.InviteOtpLockMinutes < 1 THEN 15 ELSE target.InviteOtpLockMinutes END,
  InviteMaxAttempts = CASE WHEN target.InviteMaxAttempts IS NULL OR target.InviteMaxAttempts < 1 THEN 10 ELSE target.InviteMaxAttempts END,
  InviteLockMinutes = CASE WHEN target.InviteLockMinutes IS NULL OR target.InviteLockMinutes < 1 THEN 15 ELSE target.InviteLockMinutes END,
  ChangePasswordOtpExpiryMinutes = CASE WHEN target.ChangePasswordOtpExpiryMinutes IS NULL OR target.ChangePasswordOtpExpiryMinutes < 1 THEN 10 ELSE target.ChangePasswordOtpExpiryMinutes END,
  ChangePasswordOtpCooldownSeconds = CASE WHEN target.ChangePasswordOtpCooldownSeconds IS NULL OR target.ChangePasswordOtpCooldownSeconds < 1 THEN 60 ELSE target.ChangePasswordOtpCooldownSeconds END,
  ChangePasswordOtpMaxPerHourPerUser = CASE WHEN target.ChangePasswordOtpMaxPerHourPerUser IS NULL OR target.ChangePasswordOtpMaxPerHourPerUser < 1 THEN 3 ELSE target.ChangePasswordOtpMaxPerHourPerUser END,
  ChangePasswordOtpMaxPerHourPerIp = CASE WHEN target.ChangePasswordOtpMaxPerHourPerIp IS NULL OR target.ChangePasswordOtpMaxPerHourPerIp < 1 THEN 25 ELSE target.ChangePasswordOtpMaxPerHourPerIp END,
  ChangePasswordOtpMaxAttempts = CASE WHEN target.ChangePasswordOtpMaxAttempts IS NULL OR target.ChangePasswordOtpMaxAttempts < 1 THEN 5 ELSE target.ChangePasswordOtpMaxAttempts END,
  ChangePasswordOtpLockMinutes = CASE WHEN target.ChangePasswordOtpLockMinutes IS NULL OR target.ChangePasswordOtpLockMinutes < 1 THEN 15 ELSE target.ChangePasswordOtpLockMinutes END
WHEN NOT MATCHED THEN
  INSERT(AppSettingsId, EnhancedGoogleDataRefreshHours, GoogleReviewsRefreshHours, GoogleUpdatesRefreshHours, GoogleQuestionsAndAnswersRefreshHours, GoogleSocialProfilesRefreshHours, SearchVolumeRefreshCooldownDays, MapPackClickSharePercent, MapPackCtrPosition1Percent, MapPackCtrPosition2Percent, MapPackCtrPosition3Percent, MapPackCtrPosition4Percent, MapPackCtrPosition5Percent, MapPackCtrPosition6Percent, MapPackCtrPosition7Percent, MapPackCtrPosition8Percent, MapPackCtrPosition9Percent, MapPackCtrPosition10Percent, ZohoLeadOwnerName, ZohoLeadOwnerId, ZohoLeadNextAction, SiteUrl, MinimumPasswordLength, PasswordRequiresNumber, PasswordRequiresCapitalLetter, PasswordRequiresSpecialCharacter, LoginLockoutThreshold, LoginLockoutMinutes, EmailCodeCooldownSeconds, EmailCodeMaxPerHourPerEmail, EmailCodeMaxPerHourPerIp, EmailCodeExpiryMinutes, EmailCodeMaxFailedAttemptsPerCode, InviteExpiryHours, InviteOtpExpiryMinutes, InviteOtpCooldownSeconds, InviteOtpMaxPerHourPerInvite, InviteOtpMaxPerHourPerIp, InviteOtpMaxAttempts, InviteOtpLockMinutes, InviteMaxAttempts, InviteLockMinutes, ChangePasswordOtpExpiryMinutes, ChangePasswordOtpCooldownSeconds, ChangePasswordOtpMaxPerHourPerUser, ChangePasswordOtpMaxPerHourPerIp, ChangePasswordOtpMaxAttempts, ChangePasswordOtpLockMinutes, UpdatedAtUtc)
  VALUES(1, 24, 24, 24, 24, 24, 30, 50, 38, 23, 16, 7, 5, 4, 3, 2, 1, 1, N''Richard Howes'', N''1108404000000068001'', N''Make first contact'', N''https://briskly-viceless-kayleen.ngrok-free.dev/'', 12, 1, 1, 1, 5, 15, 60, 10, 50, 10, 5, 24, 10, 60, 3, 25, 5, 15, 10, 15, 10, 60, 3, 25, 5, 15, SYSUTCDATETIME());
');
IF OBJECT_ID('dbo.GoogleBusinessProfileCategory','U') IS NULL
BEGIN
  CREATE TABLE dbo.GoogleBusinessProfileCategory(
    CategoryId nvarchar(255) NOT NULL PRIMARY KEY,
    DisplayName nvarchar(300) NOT NULL,
    RegionCode nvarchar(10) NOT NULL,
    LanguageCode nvarchar(20) NOT NULL,
    Status nvarchar(20) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_Status DEFAULT('Active'),
    FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_FirstSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_LastSeenUtc DEFAULT SYSUTCDATETIME(),
    LastSyncedUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_LastSyncedUtc DEFAULT SYSUTCDATETIME(),
    LastSeenCycleId uniqueidentifier NULL
  );
END;
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'DisplayName') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD DisplayName nvarchar(300) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_DisplayName DEFAULT N'';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'RegionCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD RegionCode nvarchar(10) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_RegionCode DEFAULT N'GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LanguageCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD LanguageCode nvarchar(20) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_LanguageCode DEFAULT N'en-GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'Status') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD Status nvarchar(20) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_Status_Alt DEFAULT N'Active';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'FirstSeenUtc') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD FirstSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_FirstSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LastSeenUtc') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_LastSeenUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LastSyncedUtc') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD LastSyncedUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategory_LastSyncedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GoogleBusinessProfileCategory', 'LastSeenCycleId') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategory ADD LastSeenCycleId uniqueidentifier NULL;
IF EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_GoogleBusinessProfileCategory_Status'
    AND parent_object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
)
  ALTER TABLE dbo.GoogleBusinessProfileCategory DROP CONSTRAINT CK_GoogleBusinessProfileCategory_Status;
UPDATE dbo.GoogleBusinessProfileCategory
SET Status = N'Inactive'
WHERE Status IS NULL
  OR Status NOT IN (N'Active', N'Inactive');
ALTER TABLE dbo.GoogleBusinessProfileCategory
  WITH CHECK ADD CONSTRAINT CK_GoogleBusinessProfileCategory_Status CHECK (Status IN ('Active','Inactive'));
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GoogleBusinessProfileCategory_Status_DisplayName' AND object_id=OBJECT_ID('dbo.GoogleBusinessProfileCategory'))
  CREATE INDEX IX_GoogleBusinessProfileCategory_Status_DisplayName ON dbo.GoogleBusinessProfileCategory(Status, DisplayName, CategoryId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GoogleBusinessProfileCategory_Region_Language_Status' AND object_id=OBJECT_ID('dbo.GoogleBusinessProfileCategory'))
  CREATE INDEX IX_GoogleBusinessProfileCategory_Region_Language_Status ON dbo.GoogleBusinessProfileCategory(RegionCode, LanguageCode, Status, CategoryId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GoogleBusinessProfileCategory_Region_Language_Cycle' AND object_id=OBJECT_ID('dbo.GoogleBusinessProfileCategory'))
  CREATE INDEX IX_GoogleBusinessProfileCategory_Region_Language_Cycle ON dbo.GoogleBusinessProfileCategory(RegionCode, LanguageCode, LastSeenCycleId, CategoryId);
IF OBJECT_ID('dbo.GoogleBusinessProfileCategorySyncRun','U') IS NULL
BEGIN
  CREATE TABLE dbo.GoogleBusinessProfileCategorySyncRun(
    GoogleBusinessProfileCategorySyncRunId bigint IDENTITY(1,1) PRIMARY KEY,
    RegionCode nvarchar(10) NOT NULL,
    LanguageCode nvarchar(20) NOT NULL,
    RanAtUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_RanAtUtc DEFAULT SYSUTCDATETIME(),
    AddedCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_AddedCount DEFAULT(0),
    UpdatedCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_UpdatedCount DEFAULT(0),
    MarkedInactiveCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_MarkedInactiveCount DEFAULT(0)
  );
END;
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'RegionCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD RegionCode nvarchar(10) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_RegionCode DEFAULT N'GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'LanguageCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD LanguageCode nvarchar(20) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_LanguageCode DEFAULT N'en-GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'RanAtUtc') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD RanAtUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_RanAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'AddedCount') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD AddedCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_AddedCount_Alt DEFAULT(0);
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'UpdatedCount') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD UpdatedCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_UpdatedCount_Alt DEFAULT(0);
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncRun', 'MarkedInactiveCount') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncRun ADD MarkedInactiveCount int NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncRun_MarkedInactiveCount_Alt DEFAULT(0);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GoogleBusinessProfileCategorySyncRun_Region_Language_RanAt' AND object_id=OBJECT_ID('dbo.GoogleBusinessProfileCategorySyncRun'))
  CREATE INDEX IX_GoogleBusinessProfileCategorySyncRun_Region_Language_RanAt ON dbo.GoogleBusinessProfileCategorySyncRun(RegionCode, LanguageCode, RanAtUtc DESC);
IF OBJECT_ID('dbo.GoogleBusinessProfileCategorySyncCursor','U') IS NULL
BEGIN
  CREATE TABLE dbo.GoogleBusinessProfileCategorySyncCursor(
    RegionCode nvarchar(10) NOT NULL,
    LanguageCode nvarchar(20) NOT NULL,
    CycleId uniqueidentifier NOT NULL,
    NextPageToken nvarchar(2048) NULL,
    UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_UpdatedUtc DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_GoogleBusinessProfileCategorySyncCursor PRIMARY KEY(RegionCode, LanguageCode)
  );
END;
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncCursor', 'RegionCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncCursor ADD RegionCode nvarchar(10) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_RegionCode DEFAULT N'GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncCursor', 'LanguageCode') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncCursor ADD LanguageCode nvarchar(20) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_LanguageCode DEFAULT N'en-GB';
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncCursor', 'CycleId') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncCursor ADD CycleId uniqueidentifier NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_CycleId DEFAULT NEWID();
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncCursor', 'NextPageToken') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncCursor ADD NextPageToken nvarchar(2048) NULL;
IF COL_LENGTH('dbo.GoogleBusinessProfileCategorySyncCursor', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.GoogleBusinessProfileCategorySyncCursor ADD UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GoogleBusinessProfileCategorySyncCursor_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF OBJECT_ID('dbo.GbCounty','U') IS NULL
BEGIN
  CREATE TABLE dbo.GbCounty(
    CountyId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Name nvarchar(200) NOT NULL,
    Slug nvarchar(200) NULL,
    IsActive bit NOT NULL CONSTRAINT DF_GbCounty_IsActive DEFAULT(1),
    SortOrder int NULL,
    CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbCounty_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbCounty_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.GbCounty', 'Name') IS NULL
  ALTER TABLE dbo.GbCounty ADD Name nvarchar(200) NOT NULL CONSTRAINT DF_GbCounty_Name DEFAULT N'';
IF COL_LENGTH('dbo.GbCounty', 'Slug') IS NULL
  ALTER TABLE dbo.GbCounty ADD Slug nvarchar(200) NULL;
IF COL_LENGTH('dbo.GbCounty', 'IsActive') IS NULL
  ALTER TABLE dbo.GbCounty ADD IsActive bit NOT NULL CONSTRAINT DF_GbCounty_IsActive_Alt DEFAULT(1);
IF COL_LENGTH('dbo.GbCounty', 'SortOrder') IS NULL
  ALTER TABLE dbo.GbCounty ADD SortOrder int NULL;
IF COL_LENGTH('dbo.GbCounty', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.GbCounty ADD CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbCounty_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GbCounty', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.GbCounty ADD UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbCounty_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GbCounty_IsActive_Name' AND object_id=OBJECT_ID('dbo.GbCounty'))
  CREATE INDEX IX_GbCounty_IsActive_Name ON dbo.GbCounty(IsActive, Name);
IF OBJECT_ID('dbo.GbTown','U') IS NULL
BEGIN
  CREATE TABLE dbo.GbTown(
    TownId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CountyId bigint NOT NULL FOREIGN KEY REFERENCES dbo.GbCounty(CountyId),
    Name nvarchar(200) NOT NULL,
    Slug nvarchar(200) NULL,
    Latitude decimal(9,6) NULL,
    Longitude decimal(9,6) NULL,
    ExternalId nvarchar(128) NULL,
    IsActive bit NOT NULL CONSTRAINT DF_GbTown_IsActive DEFAULT(1),
    SortOrder int NULL,
    CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbTown_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbTown_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.GbTown', 'CountyId') IS NULL
  ALTER TABLE dbo.GbTown ADD CountyId bigint NOT NULL CONSTRAINT DF_GbTown_CountyId DEFAULT(0);
IF COL_LENGTH('dbo.GbTown', 'Name') IS NULL
  ALTER TABLE dbo.GbTown ADD Name nvarchar(200) NOT NULL CONSTRAINT DF_GbTown_Name DEFAULT N'';
IF COL_LENGTH('dbo.GbTown', 'Slug') IS NULL
  ALTER TABLE dbo.GbTown ADD Slug nvarchar(200) NULL;
IF COL_LENGTH('dbo.GbTown', 'Latitude') IS NULL
  ALTER TABLE dbo.GbTown ADD Latitude decimal(9,6) NULL;
IF COL_LENGTH('dbo.GbTown', 'Longitude') IS NULL
  ALTER TABLE dbo.GbTown ADD Longitude decimal(9,6) NULL;
IF COL_LENGTH('dbo.GbTown', 'ExternalId') IS NULL
  ALTER TABLE dbo.GbTown ADD ExternalId nvarchar(128) NULL;
IF COL_LENGTH('dbo.GbTown', 'IsActive') IS NULL
  ALTER TABLE dbo.GbTown ADD IsActive bit NOT NULL CONSTRAINT DF_GbTown_IsActive_Alt DEFAULT(1);
IF COL_LENGTH('dbo.GbTown', 'SortOrder') IS NULL
  ALTER TABLE dbo.GbTown ADD SortOrder int NULL;
IF COL_LENGTH('dbo.GbTown', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.GbTown ADD CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbTown_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.GbTown', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.GbTown ADD UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_GbTown_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.GbTown')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.GbCounty')
)
  ALTER TABLE dbo.GbTown WITH CHECK ADD CONSTRAINT FK_GbTown_GbCounty FOREIGN KEY (CountyId) REFERENCES dbo.GbCounty(CountyId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_GbTown_County_Name' AND object_id=OBJECT_ID('dbo.GbTown'))
  CREATE UNIQUE INDEX UX_GbTown_County_Name ON dbo.GbTown(CountyId, Name);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GbTown_IsActive_County_Name' AND object_id=OBJECT_ID('dbo.GbTown'))
  CREATE INDEX IX_GbTown_IsActive_County_Name ON dbo.GbTown(IsActive, CountyId, Name);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_GbTown_CountyId' AND object_id=OBJECT_ID('dbo.GbTown'))
  CREATE INDEX IX_GbTown_CountyId ON dbo.GbTown(CountyId);
IF OBJECT_ID('dbo.CategoryLocationKeyword','U') IS NULL
BEGIN
  CREATE TABLE dbo.CategoryLocationKeyword(
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CategoryId nvarchar(255) NOT NULL,
    LocationId bigint NOT NULL,
    Keyword nvarchar(255) NOT NULL,
    KeywordType int NOT NULL CONSTRAINT DF_CategoryLocationKeyword_KeywordType DEFAULT(3),
    CanonicalKeywordId int NULL,
    AvgSearchVolume int NULL,
    Cpc decimal(10,2) NULL,
    Competition nvarchar(50) NULL,
    CompetitionIndex int NULL,
    LowTopOfPageBid decimal(10,2) NULL,
    HighTopOfPageBid decimal(10,2) NULL,
    Fingerprint nvarchar(128) NULL,
    NoData bit NOT NULL CONSTRAINT DF_CategoryLocationKeyword_NoData DEFAULT(0),
    NoDataReason nvarchar(50) NULL,
    LastAttemptedUtc datetime2(0) NULL,
    LastSucceededUtc datetime2(0) NULL,
    LastStatusCode int NULL,
    LastStatusMessage nvarchar(255) NULL,
    CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'CategoryId') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD CategoryId nvarchar(255) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_CategoryId DEFAULT N'';
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LocationId') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LocationId bigint NOT NULL CONSTRAINT DF_CategoryLocationKeyword_LocationId DEFAULT(0);
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'Keyword') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD Keyword nvarchar(255) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_Keyword DEFAULT N'';
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'KeywordType') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD KeywordType int NOT NULL CONSTRAINT DF_CategoryLocationKeyword_KeywordType_Alt DEFAULT(3);
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'CanonicalKeywordId') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD CanonicalKeywordId int NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'AvgSearchVolume') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD AvgSearchVolume int NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'Cpc') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD Cpc decimal(10,2) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'Competition') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD Competition nvarchar(50) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'CompetitionIndex') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD CompetitionIndex int NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LowTopOfPageBid') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LowTopOfPageBid decimal(10,2) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'HighTopOfPageBid') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD HighTopOfPageBid decimal(10,2) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'Fingerprint') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD Fingerprint nvarchar(128) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'NoData') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD NoData bit NOT NULL CONSTRAINT DF_CategoryLocationKeyword_NoData_Alt DEFAULT(0);
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'NoDataReason') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD NoDataReason nvarchar(50) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LastAttemptedUtc') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LastAttemptedUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LastSucceededUtc') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LastSucceededUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LastStatusCode') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LastStatusCode int NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'LastStatusMessage') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD LastStatusMessage nvarchar(255) NULL;
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD CreatedUtc datetime2(0) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.CategoryLocationKeyword', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.CategoryLocationKeyword ADD UpdatedUtc datetime2(0) NOT NULL CONSTRAINT DF_CategoryLocationKeyword_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_CategoryLocationKeyword_KeywordType'
    AND parent_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
)
  ALTER TABLE dbo.CategoryLocationKeyword DROP CONSTRAINT CK_CategoryLocationKeyword_KeywordType;
UPDATE dbo.CategoryLocationKeyword
SET KeywordType = 3
WHERE KeywordType NOT IN (1,2,3,4);
ALTER TABLE dbo.CategoryLocationKeyword
  WITH CHECK ADD CONSTRAINT CK_CategoryLocationKeyword_KeywordType CHECK (KeywordType IN (1,2,3,4));
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
    AND pc.name = 'CategoryId'
    AND rc.name = 'CategoryId'
)
  ALTER TABLE dbo.CategoryLocationKeyword WITH CHECK ADD CONSTRAINT FK_CategoryLocationKeyword_GoogleBusinessProfileCategory FOREIGN KEY (CategoryId) REFERENCES dbo.GoogleBusinessProfileCategory(CategoryId);
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.GbTown')
    AND pc.name = 'LocationId'
    AND rc.name = 'TownId'
)
  ALTER TABLE dbo.CategoryLocationKeyword WITH CHECK ADD CONSTRAINT FK_CategoryLocationKeyword_GbTown FOREIGN KEY (LocationId) REFERENCES dbo.GbTown(TownId);
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
    AND pc.name = 'CanonicalKeywordId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.CategoryLocationKeyword WITH CHECK ADD CONSTRAINT FK_CategoryLocationKeyword_CanonicalKeyword FOREIGN KEY (CanonicalKeywordId) REFERENCES dbo.CategoryLocationKeyword(Id);
IF EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_CategoryLocationKeyword_SynonymCanonical'
    AND parent_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
)
  ALTER TABLE dbo.CategoryLocationKeyword DROP CONSTRAINT CK_CategoryLocationKeyword_SynonymCanonical;
UPDATE k
SET
  KeywordType = CASE WHEN k.KeywordType = 2 THEN 3 ELSE k.KeywordType END,
  CanonicalKeywordId = NULL
FROM dbo.CategoryLocationKeyword k
LEFT JOIN dbo.CategoryLocationKeyword c
  ON c.Id = k.CanonicalKeywordId
 AND c.CategoryId = k.CategoryId
 AND c.LocationId = k.LocationId
WHERE
  (k.KeywordType = 2 AND (k.CanonicalKeywordId IS NULL OR k.CanonicalKeywordId = k.Id OR c.Id IS NULL))
  OR
  (k.KeywordType <> 2 AND k.CanonicalKeywordId IS NOT NULL);
ALTER TABLE dbo.CategoryLocationKeyword
  WITH CHECK ADD CONSTRAINT CK_CategoryLocationKeyword_SynonymCanonical CHECK (
    (KeywordType = 2 AND CanonicalKeywordId IS NOT NULL AND CanonicalKeywordId <> Id)
    OR
    (KeywordType <> 2 AND CanonicalKeywordId IS NULL)
  );
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_CategoryLocationKeyword_Category_Location_Keyword' AND object_id=OBJECT_ID('dbo.CategoryLocationKeyword'))
  CREATE UNIQUE INDEX UX_CategoryLocationKeyword_Category_Location_Keyword ON dbo.CategoryLocationKeyword(CategoryId, LocationId, Keyword);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_CategoryLocationKeyword_Category_Location_MainTerm' AND object_id=OBJECT_ID('dbo.CategoryLocationKeyword'))
  CREATE UNIQUE INDEX UX_CategoryLocationKeyword_Category_Location_MainTerm ON dbo.CategoryLocationKeyword(CategoryId, LocationId) WHERE KeywordType = 1;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_CategoryLocationKeyword_Location_Category' AND object_id=OBJECT_ID('dbo.CategoryLocationKeyword'))
  CREATE INDEX IX_CategoryLocationKeyword_Location_Category ON dbo.CategoryLocationKeyword(LocationId, CategoryId, KeywordType, UpdatedUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_CategoryLocationKeyword_SynonymCanonical' AND object_id=OBJECT_ID('dbo.CategoryLocationKeyword'))
  CREATE INDEX IX_CategoryLocationKeyword_SynonymCanonical ON dbo.CategoryLocationKeyword(CategoryId, LocationId, CanonicalKeywordId) WHERE KeywordType = 2;
IF OBJECT_ID('dbo.CategoryLocationSearchVolume','U') IS NULL
BEGIN
  CREATE TABLE dbo.CategoryLocationSearchVolume(
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CategoryLocationKeywordId int NOT NULL,
    [Year] int NOT NULL,
    [Month] int NOT NULL,
    SearchVolume int NOT NULL
  );
END;
IF COL_LENGTH('dbo.CategoryLocationSearchVolume', 'CategoryLocationKeywordId') IS NULL
  ALTER TABLE dbo.CategoryLocationSearchVolume ADD CategoryLocationKeywordId int NOT NULL CONSTRAINT DF_CategoryLocationSearchVolume_KeywordId DEFAULT(0);
IF COL_LENGTH('dbo.CategoryLocationSearchVolume', 'Year') IS NULL
  ALTER TABLE dbo.CategoryLocationSearchVolume ADD [Year] int NOT NULL CONSTRAINT DF_CategoryLocationSearchVolume_Year DEFAULT(2000);
IF COL_LENGTH('dbo.CategoryLocationSearchVolume', 'Month') IS NULL
  ALTER TABLE dbo.CategoryLocationSearchVolume ADD [Month] int NOT NULL CONSTRAINT DF_CategoryLocationSearchVolume_Month DEFAULT(1);
IF COL_LENGTH('dbo.CategoryLocationSearchVolume', 'SearchVolume') IS NULL
  ALTER TABLE dbo.CategoryLocationSearchVolume ADD SearchVolume int NOT NULL CONSTRAINT DF_CategoryLocationSearchVolume_SearchVolume DEFAULT(0);
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.CategoryLocationSearchVolume')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.CategoryLocationKeyword')
    AND pc.name = 'CategoryLocationKeywordId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.CategoryLocationSearchVolume WITH CHECK ADD CONSTRAINT FK_CategoryLocationSearchVolume_Keyword FOREIGN KEY (CategoryLocationKeywordId) REFERENCES dbo.CategoryLocationKeyword(Id) ON DELETE CASCADE;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_CategoryLocationSearchVolume_Keyword_Year_Month' AND object_id=OBJECT_ID('dbo.CategoryLocationSearchVolume'))
  CREATE UNIQUE INDEX UX_CategoryLocationSearchVolume_Keyword_Year_Month ON dbo.CategoryLocationSearchVolume(CategoryLocationKeywordId, [Year], [Month]);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_CategoryLocationSearchVolume_Keyword' AND object_id=OBJECT_ID('dbo.CategoryLocationSearchVolume'))
  CREATE INDEX IX_CategoryLocationSearchVolume_Keyword ON dbo.CategoryLocationSearchVolume(CategoryLocationKeywordId, [Year] DESC, [Month] DESC);

IF NOT EXISTS (SELECT 1 FROM dbo.GbCounty WHERE Name = N'Somerset')
  INSERT INTO dbo.GbCounty(Name, Slug, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
  VALUES(N'Somerset', N'somerset', 1, 10, SYSUTCDATETIME(), SYSUTCDATETIME());
IF NOT EXISTS (SELECT 1 FROM dbo.GbCounty WHERE Name = N'Dorset')
  INSERT INTO dbo.GbCounty(Name, Slug, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
  VALUES(N'Dorset', N'dorset', 1, 20, SYSUTCDATETIME(), SYSUTCDATETIME());
IF NOT EXISTS (SELECT 1 FROM dbo.GbCounty WHERE Name = N'Devon')
  INSERT INTO dbo.GbCounty(Name, Slug, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
  VALUES(N'Devon', N'devon', 1, 30, SYSUTCDATETIME(), SYSUTCDATETIME());
IF NOT EXISTS (SELECT 1 FROM dbo.GbCounty WHERE Name = N'Wiltshire')
  INSERT INTO dbo.GbCounty(Name, Slug, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
  VALUES(N'Wiltshire', N'wiltshire', 1, 40, SYSUTCDATETIME(), SYSUTCDATETIME());

DECLARE @SeedSomersetCountyId bigint = (SELECT TOP 1 CountyId FROM dbo.GbCounty WHERE Name = N'Somerset' ORDER BY CountyId);
DECLARE @SeedDorsetCountyId bigint = (SELECT TOP 1 CountyId FROM dbo.GbCounty WHERE Name = N'Dorset' ORDER BY CountyId);
IF @SeedSomersetCountyId IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dbo.GbTown WHERE CountyId = @SeedSomersetCountyId AND Name = N'Yeovil')
    INSERT INTO dbo.GbTown(CountyId, Name, Slug, Latitude, Longitude, ExternalId, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
    VALUES(@SeedSomersetCountyId, N'Yeovil', N'yeovil', NULL, NULL, NULL, 1, 10, SYSUTCDATETIME(), SYSUTCDATETIME());
  IF NOT EXISTS (SELECT 1 FROM dbo.GbTown WHERE CountyId = @SeedSomersetCountyId AND Name = N'Taunton')
    INSERT INTO dbo.GbTown(CountyId, Name, Slug, Latitude, Longitude, ExternalId, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
    VALUES(@SeedSomersetCountyId, N'Taunton', N'taunton', NULL, NULL, NULL, 1, 20, SYSUTCDATETIME(), SYSUTCDATETIME());
END;
IF @SeedDorsetCountyId IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dbo.GbTown WHERE CountyId = @SeedDorsetCountyId AND Name = N'Dorchester')
    INSERT INTO dbo.GbTown(CountyId, Name, Slug, Latitude, Longitude, ExternalId, IsActive, SortOrder, CreatedUtc, UpdatedUtc)
    VALUES(@SeedDorsetCountyId, N'Dorchester', N'dorchester', NULL, NULL, NULL, 1, 10, SYSUTCDATETIME(), SYSUTCDATETIME());
END;
IF COL_LENGTH('dbo.SearchRun', 'CategoryId') IS NULL
  ALTER TABLE dbo.SearchRun ADD CategoryId nvarchar(255) NULL;
IF COL_LENGTH('dbo.SearchRun', 'TownId') IS NULL
  ALTER TABLE dbo.SearchRun ADD TownId bigint NULL;

IF COL_LENGTH('dbo.SearchRun', 'SeedKeyword') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE sr
  SET sr.CategoryId = c.CategoryId
  FROM dbo.SearchRun sr
  JOIN dbo.GoogleBusinessProfileCategory c
    ON LOWER(LTRIM(RTRIM(c.CategoryId))) = LOWER(LTRIM(RTRIM(sr.SeedKeyword)))
  WHERE sr.CategoryId IS NULL;');
END;

IF COL_LENGTH('dbo.SearchRun', 'LocationName') IS NOT NULL
BEGIN
  EXEC(N'
  ;WITH normalized_runs AS (
    SELECT
      sr.SearchRunId,
      LOWER(LTRIM(RTRIM(sr.LocationName))) AS LocationKey
    FROM dbo.SearchRun sr
    WHERE sr.TownId IS NULL
      AND sr.LocationName IS NOT NULL
      AND LTRIM(RTRIM(sr.LocationName)) <> N''''
  ),
  matches AS (
    SELECT
      nr.SearchRunId,
      t.TownId,
      COUNT(1) OVER (PARTITION BY nr.SearchRunId) AS MatchCount,
      ROW_NUMBER() OVER (PARTITION BY nr.SearchRunId ORDER BY t.TownId) AS rn
    FROM normalized_runs nr
    JOIN dbo.GbTown t ON 1 = 1
    JOIN dbo.GbCounty c ON c.CountyId = t.CountyId
    WHERE nr.LocationKey = LOWER(LTRIM(RTRIM(t.Name)))
      OR (
        t.Slug IS NOT NULL
        AND LTRIM(RTRIM(t.Slug)) <> N''''
        AND nr.LocationKey = LOWER(LTRIM(RTRIM(t.Slug)))
      )
      OR nr.LocationKey = LOWER(LTRIM(RTRIM(CONCAT(t.Name, N'', '', c.Name))))
      OR nr.LocationKey = LOWER(LTRIM(RTRIM(CONCAT(t.Name, N'' '', c.Name))))
  )
  UPDATE sr
  SET sr.TownId = m.TownId
  FROM dbo.SearchRun sr
  JOIN matches m ON m.SearchRunId = sr.SearchRunId
  WHERE sr.TownId IS NULL
    AND m.MatchCount = 1
    AND m.rn = 1;');
END;

IF COL_LENGTH('dbo.SearchRun', 'CenterLat') IS NOT NULL
   OR COL_LENGTH('dbo.SearchRun', 'CenterLng') IS NOT NULL
BEGIN
  EXEC(N'
  UPDATE t
  SET
    Latitude = COALESCE(t.Latitude, sr.CenterLat),
    Longitude = COALESCE(t.Longitude, sr.CenterLng),
    UpdatedUtc = CASE
      WHEN (t.Latitude IS NULL AND sr.CenterLat IS NOT NULL)
        OR (t.Longitude IS NULL AND sr.CenterLng IS NOT NULL)
      THEN SYSUTCDATETIME()
      ELSE t.UpdatedUtc
    END
  FROM dbo.GbTown t
  JOIN dbo.SearchRun sr ON sr.TownId = t.TownId
  WHERE (t.Latitude IS NULL AND sr.CenterLat IS NOT NULL)
     OR (t.Longitude IS NULL AND sr.CenterLng IS NOT NULL);');
END;

IF COL_LENGTH('dbo.SearchRun', 'CategoryId') IS NOT NULL
   AND COL_LENGTH('dbo.SearchRun', 'TownId') IS NOT NULL
BEGIN
  EXEC(N'
  ;WITH invalid_runs AS (
    SELECT SearchRunId
    FROM dbo.SearchRun
    WHERE CategoryId IS NULL
       OR TownId IS NULL
  )
  DELETE ps
  FROM dbo.PlaceSnapshot ps
  JOIN invalid_runs ir ON ir.SearchRunId = ps.SearchRunId;

  DELETE sr
  FROM dbo.SearchRun sr
  WHERE sr.CategoryId IS NULL
     OR sr.TownId IS NULL;');
END;

IF COL_LENGTH('dbo.SearchRun', 'CategoryId') IS NOT NULL
  EXEC(N'ALTER TABLE dbo.SearchRun ALTER COLUMN CategoryId nvarchar(255) NOT NULL;');
IF COL_LENGTH('dbo.SearchRun', 'TownId') IS NOT NULL
  EXEC(N'ALTER TABLE dbo.SearchRun ALTER COLUMN TownId bigint NOT NULL;');

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.SearchRun')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.GoogleBusinessProfileCategory')
    AND pc.name = 'CategoryId'
    AND rc.name = 'CategoryId'
)
  AND COL_LENGTH('dbo.SearchRun', 'CategoryId') IS NOT NULL
  EXEC(N'ALTER TABLE dbo.SearchRun WITH CHECK ADD CONSTRAINT FK_SearchRun_GoogleBusinessProfileCategory FOREIGN KEY (CategoryId) REFERENCES dbo.GoogleBusinessProfileCategory(CategoryId);');

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.SearchRun')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.GbTown')
    AND pc.name = 'TownId'
    AND rc.name = 'TownId'
)
  AND COL_LENGTH('dbo.SearchRun', 'TownId') IS NOT NULL
  EXEC(N'ALTER TABLE dbo.SearchRun WITH CHECK ADD CONSTRAINT FK_SearchRun_GbTown FOREIGN KEY (TownId) REFERENCES dbo.GbTown(TownId);');

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SearchRun_CategoryId' AND object_id=OBJECT_ID('dbo.SearchRun'))
   AND COL_LENGTH('dbo.SearchRun', 'CategoryId') IS NOT NULL
  EXEC(N'CREATE INDEX IX_SearchRun_CategoryId ON dbo.SearchRun(CategoryId);');
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SearchRun_TownId' AND object_id=OBJECT_ID('dbo.SearchRun'))
   AND COL_LENGTH('dbo.SearchRun', 'TownId') IS NOT NULL
  EXEC(N'CREATE INDEX IX_SearchRun_TownId ON dbo.SearchRun(TownId);');
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_SearchRun_Status_LastUpdatedUtc' AND object_id=OBJECT_ID('dbo.SearchRun'))
   AND COL_LENGTH('dbo.SearchRun', 'Status') IS NOT NULL
   AND COL_LENGTH('dbo.SearchRun', 'LastUpdatedUtc') IS NOT NULL
  EXEC(N'CREATE INDEX IX_SearchRun_Status_LastUpdatedUtc ON dbo.SearchRun([Status], LastUpdatedUtc DESC);');

IF COL_LENGTH('dbo.SearchRun', 'SeedKeyword') IS NOT NULL
  ALTER TABLE dbo.SearchRun DROP COLUMN SeedKeyword;
IF COL_LENGTH('dbo.SearchRun', 'LocationName') IS NOT NULL
  ALTER TABLE dbo.SearchRun DROP COLUMN LocationName;
IF COL_LENGTH('dbo.SearchRun', 'CenterLat') IS NOT NULL
  ALTER TABLE dbo.SearchRun DROP COLUMN CenterLat;
IF COL_LENGTH('dbo.SearchRun', 'CenterLng') IS NOT NULL
  ALTER TABLE dbo.SearchRun DROP COLUMN CenterLng;

IF OBJECT_ID('dbo.ZohoOAuthToken','U') IS NULL
BEGIN
  CREATE TABLE dbo.ZohoOAuthToken(
    TokenKey nvarchar(100) NOT NULL PRIMARY KEY,
    ProtectedRefreshToken nvarchar(max) NULL,
    AccessToken nvarchar(max) NULL,
    AccessTokenExpiresAtUtc datetime2(0) NULL,
    UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_ZohoOAuthToken_UpdatedAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.ZohoOAuthToken', 'ProtectedRefreshToken') IS NULL
  ALTER TABLE dbo.ZohoOAuthToken ADD ProtectedRefreshToken nvarchar(max) NULL;
IF COL_LENGTH('dbo.ZohoOAuthToken', 'AccessToken') IS NULL
  ALTER TABLE dbo.ZohoOAuthToken ADD AccessToken nvarchar(max) NULL;
IF COL_LENGTH('dbo.ZohoOAuthToken', 'AccessTokenExpiresAtUtc') IS NULL
  ALTER TABLE dbo.ZohoOAuthToken ADD AccessTokenExpiresAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.ZohoOAuthToken', 'UpdatedAtUtc') IS NULL
  ALTER TABLE dbo.ZohoOAuthToken ADD UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_ZohoOAuthToken_UpdatedAtUtc_Alt DEFAULT SYSUTCDATETIME();

IF OBJECT_ID('dbo.[User]','U') IS NULL
BEGIN
  CREATE TABLE dbo.[User](
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    FirstName varchar(100) NOT NULL,
    LastName varchar(100) NOT NULL,
    EmailAddress varchar(320) NOT NULL,
    EmailAddressNormalized varchar(320) NOT NULL,
    PasswordHash varbinary(256) NULL,
    Salt varbinary(32) NULL,
    PasswordHashVersion tinyint NOT NULL CONSTRAINT DF_User_PasswordHashVersion DEFAULT(1),
    IsActive bit NOT NULL CONSTRAINT DF_User_IsActive DEFAULT(1),
    IsAdmin bit NOT NULL CONSTRAINT DF_User_IsAdmin DEFAULT(0),
    UseGravatar bit NOT NULL CONSTRAINT DF_User_UseGravatar DEFAULT(0),
    IsDarkMode bit NOT NULL CONSTRAINT DF_User_IsDarkMode DEFAULT(0),
    SessionVersion int NOT NULL CONSTRAINT DF_User_SessionVersion DEFAULT(0),
    InviteStatus tinyint NOT NULL CONSTRAINT DF_User_InviteStatus DEFAULT(1),
    DateCreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_User_DateCreatedAtUtc DEFAULT SYSUTCDATETIME(),
    DatePasswordLastSetUtc datetime2(0) NULL,
    LastLoginAtUtc datetime2(0) NULL,
    FailedPasswordAttempts int NOT NULL CONSTRAINT DF_User_FailedPasswordAttempts DEFAULT(0),
    LockedoutUntilUtc datetime2(0) NULL
  );
END;
IF COL_LENGTH('dbo.[User]', 'PasswordHash') IS NULL
  ALTER TABLE dbo.[User] ADD PasswordHash varbinary(256) NULL;
IF COL_LENGTH('dbo.[User]', 'Salt') IS NULL
  ALTER TABLE dbo.[User] ADD Salt varbinary(32) NULL;
IF COL_LENGTH('dbo.[User]', 'PasswordHashVersion') IS NULL
  ALTER TABLE dbo.[User] ADD PasswordHashVersion tinyint NOT NULL CONSTRAINT DF_User_PasswordHashVersion_Alt DEFAULT(1);
IF COL_LENGTH('dbo.[User]', 'IsActive') IS NULL
  ALTER TABLE dbo.[User] ADD IsActive bit NOT NULL CONSTRAINT DF_User_IsActive_Alt DEFAULT(1);
IF COL_LENGTH('dbo.[User]', 'IsAdmin') IS NULL
  ALTER TABLE dbo.[User] ADD IsAdmin bit NOT NULL CONSTRAINT DF_User_IsAdmin_Alt DEFAULT(0);
IF COL_LENGTH('dbo.[User]', 'UseGravatar') IS NULL
  ALTER TABLE dbo.[User] ADD UseGravatar bit NOT NULL CONSTRAINT DF_User_UseGravatar_Alt DEFAULT(0);
IF COL_LENGTH('dbo.[User]', 'IsDarkMode') IS NULL
  ALTER TABLE dbo.[User] ADD IsDarkMode bit NOT NULL CONSTRAINT DF_User_IsDarkMode_Alt DEFAULT(0);
IF COL_LENGTH('dbo.[User]', 'SessionVersion') IS NULL
  ALTER TABLE dbo.[User] ADD SessionVersion int NOT NULL CONSTRAINT DF_User_SessionVersion_Alt DEFAULT(0);
IF COL_LENGTH('dbo.[User]', 'InviteStatus') IS NULL
  ALTER TABLE dbo.[User] ADD InviteStatus tinyint NOT NULL CONSTRAINT DF_User_InviteStatus_Alt DEFAULT(1);
IF COL_LENGTH('dbo.[User]', 'DateCreatedAtUtc') IS NULL
  ALTER TABLE dbo.[User] ADD DateCreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_User_DateCreatedAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.[User]', 'DatePasswordLastSetUtc') IS NULL
  ALTER TABLE dbo.[User] ADD DatePasswordLastSetUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.[User]', 'LastLoginAtUtc') IS NULL
  ALTER TABLE dbo.[User] ADD LastLoginAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.[User]', 'FailedPasswordAttempts') IS NULL
  ALTER TABLE dbo.[User] ADD FailedPasswordAttempts int NOT NULL CONSTRAINT DF_User_FailedPasswordAttempts_Alt DEFAULT(0);
IF COL_LENGTH('dbo.[User]', 'LockedoutUntilUtc') IS NULL
  ALTER TABLE dbo.[User] ADD LockedoutUntilUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.[User]', 'InviteStatus') IS NOT NULL
BEGIN
  EXEC(N'
UPDATE dbo.[User]
SET InviteStatus = CASE WHEN IsActive = 1 THEN 1 ELSE 2 END
WHERE InviteStatus IS NULL
   OR InviteStatus NOT IN (0, 1, 2);');
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_User_EmailAddressNormalized' AND object_id = OBJECT_ID('dbo.[User]'))
  CREATE UNIQUE INDEX UX_User_EmailAddressNormalized ON dbo.[User](EmailAddressNormalized);

EXEC(N'
IF NOT EXISTS (SELECT 1 FROM dbo.[User] WHERE EmailAddressNormalized = ''tim.howes@kontrolit.net'')
BEGIN
  INSERT INTO dbo.[User](FirstName, LastName, EmailAddress, EmailAddressNormalized, IsActive, IsAdmin, InviteStatus)
  VALUES(''Tim'', ''Howes'', ''tim.howes@kontrolit.net'', ''tim.howes@kontrolit.net'', 1, 1, 1);
END
ELSE
BEGIN
  UPDATE dbo.[User]
  SET IsAdmin = 1,
      IsActive = 1,
      InviteStatus = 1
  WHERE EmailAddressNormalized = ''tim.howes@kontrolit.net'';
END;');

IF OBJECT_ID('dbo.UserInvite','U') IS NULL
BEGIN
  CREATE TABLE dbo.UserInvite(
    UserInviteId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    UserId int NOT NULL,
    EmailNormalized varchar(320) NOT NULL,
    TokenHash varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    UsedAtUtc datetime2(0) NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_UserInvite_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    CreatedByUserId int NULL,
    ResentAtUtc datetime2(0) NULL,
    Status tinyint NOT NULL CONSTRAINT DF_UserInvite_Status DEFAULT(1),
    AttemptCount int NOT NULL CONSTRAINT DF_UserInvite_AttemptCount DEFAULT(0),
    LastAttemptAtUtc datetime2(0) NULL,
    LockedUntilUtc datetime2(0) NULL,
    OtpVerifiedAtUtc datetime2(0) NULL,
    LastOtpSentAtUtc datetime2(0) NULL
  );
END;
IF COL_LENGTH('dbo.UserInvite', 'UserId') IS NULL
  ALTER TABLE dbo.UserInvite ADD UserId int NOT NULL CONSTRAINT DF_UserInvite_UserId_Alt DEFAULT(0);
IF COL_LENGTH('dbo.UserInvite', 'EmailNormalized') IS NULL
  ALTER TABLE dbo.UserInvite ADD EmailNormalized varchar(320) NOT NULL CONSTRAINT DF_UserInvite_EmailNormalized_Alt DEFAULT('');
IF COL_LENGTH('dbo.UserInvite', 'TokenHash') IS NULL
  ALTER TABLE dbo.UserInvite ADD TokenHash varbinary(32) NOT NULL CONSTRAINT DF_UserInvite_TokenHash_Alt DEFAULT(0x00);
IF COL_LENGTH('dbo.UserInvite', 'ExpiresAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD ExpiresAtUtc datetime2(0) NOT NULL CONSTRAINT DF_UserInvite_ExpiresAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.UserInvite', 'UsedAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD UsedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.UserInvite', 'CreatedAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_UserInvite_CreatedAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.UserInvite', 'CreatedByUserId') IS NULL
  ALTER TABLE dbo.UserInvite ADD CreatedByUserId int NULL;
IF COL_LENGTH('dbo.UserInvite', 'ResentAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD ResentAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.UserInvite', 'Status') IS NULL
  ALTER TABLE dbo.UserInvite ADD Status tinyint NOT NULL CONSTRAINT DF_UserInvite_Status_Alt DEFAULT(1);
IF COL_LENGTH('dbo.UserInvite', 'AttemptCount') IS NULL
  ALTER TABLE dbo.UserInvite ADD AttemptCount int NOT NULL CONSTRAINT DF_UserInvite_AttemptCount_Alt DEFAULT(0);
IF COL_LENGTH('dbo.UserInvite', 'LastAttemptAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD LastAttemptAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.UserInvite', 'LockedUntilUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD LockedUntilUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.UserInvite', 'OtpVerifiedAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD OtpVerifiedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.UserInvite', 'LastOtpSentAtUtc') IS NULL
  ALTER TABLE dbo.UserInvite ADD LastOtpSentAtUtc datetime2(0) NULL;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.UserInvite')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.[User]')
    AND pc.name = 'UserId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.UserInvite WITH CHECK ADD CONSTRAINT FK_UserInvite_User FOREIGN KEY (UserId) REFERENCES dbo.[User](Id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_UserInvite_TokenHash' AND object_id = OBJECT_ID('dbo.UserInvite'))
  CREATE UNIQUE INDEX UX_UserInvite_TokenHash ON dbo.UserInvite(TokenHash);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserInvite_UserId_Status_ExpiresAtUtc' AND object_id = OBJECT_ID('dbo.UserInvite'))
  CREATE INDEX IX_UserInvite_UserId_Status_ExpiresAtUtc ON dbo.UserInvite(UserId, Status, ExpiresAtUtc DESC);

IF OBJECT_ID('dbo.InviteOtp','U') IS NULL
BEGIN
  CREATE TABLE dbo.InviteOtp(
    InviteOtpId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    UserInviteId bigint NOT NULL,
    CodeHash varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    SentAtUtc datetime2(0) NOT NULL CONSTRAINT DF_InviteOtp_SentAtUtc DEFAULT SYSUTCDATETIME(),
    AttemptCount int NOT NULL CONSTRAINT DF_InviteOtp_AttemptCount DEFAULT(0),
    LockedUntilUtc datetime2(0) NULL,
    UsedAtUtc datetime2(0) NULL,
    RequestedFromIp varchar(45) NULL
  );
END;
IF COL_LENGTH('dbo.InviteOtp', 'UserInviteId') IS NULL
  ALTER TABLE dbo.InviteOtp ADD UserInviteId bigint NOT NULL CONSTRAINT DF_InviteOtp_UserInviteId_Alt DEFAULT(0);
IF COL_LENGTH('dbo.InviteOtp', 'CodeHash') IS NULL
  ALTER TABLE dbo.InviteOtp ADD CodeHash varbinary(32) NOT NULL CONSTRAINT DF_InviteOtp_CodeHash_Alt DEFAULT(0x00);
IF COL_LENGTH('dbo.InviteOtp', 'ExpiresAtUtc') IS NULL
  ALTER TABLE dbo.InviteOtp ADD ExpiresAtUtc datetime2(0) NOT NULL CONSTRAINT DF_InviteOtp_ExpiresAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.InviteOtp', 'SentAtUtc') IS NULL
  ALTER TABLE dbo.InviteOtp ADD SentAtUtc datetime2(0) NOT NULL CONSTRAINT DF_InviteOtp_SentAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.InviteOtp', 'AttemptCount') IS NULL
  ALTER TABLE dbo.InviteOtp ADD AttemptCount int NOT NULL CONSTRAINT DF_InviteOtp_AttemptCount_Alt DEFAULT(0);
IF COL_LENGTH('dbo.InviteOtp', 'LockedUntilUtc') IS NULL
  ALTER TABLE dbo.InviteOtp ADD LockedUntilUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.InviteOtp', 'UsedAtUtc') IS NULL
  ALTER TABLE dbo.InviteOtp ADD UsedAtUtc datetime2(0) NULL;
IF COL_LENGTH('dbo.InviteOtp', 'RequestedFromIp') IS NULL
  ALTER TABLE dbo.InviteOtp ADD RequestedFromIp varchar(45) NULL;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.InviteOtp')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.UserInvite')
    AND pc.name = 'UserInviteId'
    AND rc.name = 'UserInviteId'
)
  ALTER TABLE dbo.InviteOtp WITH CHECK ADD CONSTRAINT FK_InviteOtp_UserInvite FOREIGN KEY (UserInviteId) REFERENCES dbo.UserInvite(UserInviteId) ON DELETE CASCADE;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_InviteOtp_UserInviteId_SentAtUtc' AND object_id = OBJECT_ID('dbo.InviteOtp'))
  CREATE INDEX IX_InviteOtp_UserInviteId_SentAtUtc ON dbo.InviteOtp(UserInviteId, SentAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_InviteOtp_RequestedFromIp_SentAtUtc' AND object_id = OBJECT_ID('dbo.InviteOtp'))
  CREATE INDEX IX_InviteOtp_RequestedFromIp_SentAtUtc ON dbo.InviteOtp(RequestedFromIp, SentAtUtc DESC) WHERE RequestedFromIp IS NOT NULL;

IF OBJECT_ID('dbo.UserOtp','U') IS NULL
BEGIN
  CREATE TABLE dbo.UserOtp(
    UserOtpId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    UserId int NOT NULL,
    Purpose nvarchar(30) NOT NULL,
    CodeHash varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(3) NOT NULL,
    UsedAtUtc datetime2(3) NULL,
    SentAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserOtp_SentAtUtc DEFAULT SYSUTCDATETIME(),
    AttemptCount int NOT NULL CONSTRAINT DF_UserOtp_AttemptCount DEFAULT(0),
    LockedUntilUtc datetime2(3) NULL,
    CorrelationId nvarchar(64) NULL,
    RequestedFromIp varchar(45) NULL
  );
END;
IF COL_LENGTH('dbo.UserOtp', 'UserId') IS NULL
  ALTER TABLE dbo.UserOtp ADD UserId int NOT NULL CONSTRAINT DF_UserOtp_UserId_Alt DEFAULT(0);
IF COL_LENGTH('dbo.UserOtp', 'Purpose') IS NULL
  ALTER TABLE dbo.UserOtp ADD Purpose nvarchar(30) NOT NULL CONSTRAINT DF_UserOtp_Purpose_Alt DEFAULT N'';
IF COL_LENGTH('dbo.UserOtp', 'CodeHash') IS NULL
  ALTER TABLE dbo.UserOtp ADD CodeHash varbinary(32) NOT NULL CONSTRAINT DF_UserOtp_CodeHash_Alt DEFAULT(0x00);
IF COL_LENGTH('dbo.UserOtp', 'ExpiresAtUtc') IS NULL
  ALTER TABLE dbo.UserOtp ADD ExpiresAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserOtp_ExpiresAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.UserOtp', 'UsedAtUtc') IS NULL
  ALTER TABLE dbo.UserOtp ADD UsedAtUtc datetime2(3) NULL;
IF COL_LENGTH('dbo.UserOtp', 'SentAtUtc') IS NULL
  ALTER TABLE dbo.UserOtp ADD SentAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserOtp_SentAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.UserOtp', 'AttemptCount') IS NULL
  ALTER TABLE dbo.UserOtp ADD AttemptCount int NOT NULL CONSTRAINT DF_UserOtp_AttemptCount_Alt DEFAULT(0);
IF COL_LENGTH('dbo.UserOtp', 'LockedUntilUtc') IS NULL
  ALTER TABLE dbo.UserOtp ADD LockedUntilUtc datetime2(3) NULL;
IF COL_LENGTH('dbo.UserOtp', 'CorrelationId') IS NULL
  ALTER TABLE dbo.UserOtp ADD CorrelationId nvarchar(64) NULL;
IF COL_LENGTH('dbo.UserOtp', 'RequestedFromIp') IS NULL
  ALTER TABLE dbo.UserOtp ADD RequestedFromIp varchar(45) NULL;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.UserOtp')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.[User]')
    AND pc.name = 'UserId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.UserOtp WITH CHECK ADD CONSTRAINT FK_UserOtp_User FOREIGN KEY (UserId) REFERENCES dbo.[User](Id) ON DELETE CASCADE;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_User_Purpose_ExpiresAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_User_Purpose_ExpiresAtUtc ON dbo.UserOtp(UserId, Purpose, ExpiresAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_User_Purpose_UsedAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_User_Purpose_UsedAtUtc ON dbo.UserOtp(UserId, Purpose, UsedAtUtc);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_User_Purpose_Correlation_SentAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_User_Purpose_Correlation_SentAtUtc ON dbo.UserOtp(UserId, Purpose, CorrelationId, SentAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserOtp_RequestedFromIp_Purpose_SentAtUtc' AND object_id = OBJECT_ID('dbo.UserOtp'))
  CREATE INDEX IX_UserOtp_RequestedFromIp_Purpose_SentAtUtc ON dbo.UserOtp(RequestedFromIp, Purpose, SentAtUtc DESC) WHERE RequestedFromIp IS NOT NULL;

IF OBJECT_ID('dbo.EmailCodes','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailCodes(
    EmailCodeId int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Purpose tinyint NOT NULL,
    Email varchar(320) NOT NULL,
    EmailNormalized varchar(320) NOT NULL,
    CodeHash varbinary(64) NOT NULL,
    Salt varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_EmailCodes_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    FailedAttempts int NOT NULL CONSTRAINT DF_EmailCodes_FailedAttempts DEFAULT(0),
    IsUsed bit NOT NULL CONSTRAINT DF_EmailCodes_IsUsed DEFAULT(0),
    RequestedFromIp varchar(45) NULL,
    RequestedUserAgent varchar(256) NULL
  );
END;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EmailCodes_EmailPurposeUsedExpiry' AND object_id = OBJECT_ID('dbo.EmailCodes'))
  CREATE INDEX IX_EmailCodes_EmailPurposeUsedExpiry
    ON dbo.EmailCodes(EmailNormalized, Purpose, IsUsed, ExpiresAtUtc)
    INCLUDE (CreatedAtUtc, FailedAttempts);

IF OBJECT_ID('dbo.UserLogins','U') IS NULL
BEGIN
  CREATE TABLE dbo.UserLogins(
    UserLoginId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    AttemptedAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserLogins_AttemptedAtUtc DEFAULT SYSUTCDATETIME(),
    EmailEntered nvarchar(320) NULL,
    EmailNormalized nvarchar(320) NULL,
    UserId int NULL,
    IpAddress varchar(45) NOT NULL,
    Succeeded bit NOT NULL,
    FailureReason nvarchar(50) NULL,
    AuthStage nvarchar(20) NULL,
    UserAgent nvarchar(512) NULL,
    CorrelationId nvarchar(64) NULL
  );
END;
IF COL_LENGTH('dbo.UserLogins', 'AttemptedAtUtc') IS NULL
  ALTER TABLE dbo.UserLogins ADD AttemptedAtUtc datetime2(3) NOT NULL CONSTRAINT DF_UserLogins_AttemptedAtUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.UserLogins', 'EmailEntered') IS NULL
  ALTER TABLE dbo.UserLogins ADD EmailEntered nvarchar(320) NULL;
IF COL_LENGTH('dbo.UserLogins', 'EmailNormalized') IS NULL
  ALTER TABLE dbo.UserLogins ADD EmailNormalized nvarchar(320) NULL;
IF COL_LENGTH('dbo.UserLogins', 'UserId') IS NULL
  ALTER TABLE dbo.UserLogins ADD UserId int NULL;
IF COL_LENGTH('dbo.UserLogins', 'IpAddress') IS NULL
  ALTER TABLE dbo.UserLogins ADD IpAddress varchar(45) NOT NULL CONSTRAINT DF_UserLogins_IpAddress_Alt DEFAULT('unknown');
IF COL_LENGTH('dbo.UserLogins', 'Succeeded') IS NULL
  ALTER TABLE dbo.UserLogins ADD Succeeded bit NOT NULL CONSTRAINT DF_UserLogins_Succeeded_Alt DEFAULT(0);
IF COL_LENGTH('dbo.UserLogins', 'FailureReason') IS NULL
  ALTER TABLE dbo.UserLogins ADD FailureReason nvarchar(50) NULL;
IF COL_LENGTH('dbo.UserLogins', 'AuthStage') IS NULL
  ALTER TABLE dbo.UserLogins ADD AuthStage nvarchar(20) NULL;
IF COL_LENGTH('dbo.UserLogins', 'UserAgent') IS NULL
  ALTER TABLE dbo.UserLogins ADD UserAgent nvarchar(512) NULL;
IF COL_LENGTH('dbo.UserLogins', 'CorrelationId') IS NULL
  ALTER TABLE dbo.UserLogins ADD CorrelationId nvarchar(64) NULL;

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.UserLogins')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.[User]')
    AND pc.name = 'UserId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.UserLogins WITH CHECK ADD CONSTRAINT FK_UserLogins_User FOREIGN KEY (UserId) REFERENCES dbo.[User](Id) ON DELETE SET NULL;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserLogins_AttemptedAtUtc' AND object_id = OBJECT_ID('dbo.UserLogins'))
  CREATE INDEX IX_UserLogins_AttemptedAtUtc ON dbo.UserLogins(AttemptedAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserLogins_EmailNormalized_AttemptedAtUtc' AND object_id = OBJECT_ID('dbo.UserLogins'))
  CREATE INDEX IX_UserLogins_EmailNormalized_AttemptedAtUtc ON dbo.UserLogins(EmailNormalized, AttemptedAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserLogins_IpAddress_AttemptedAtUtc' AND object_id = OBJECT_ID('dbo.UserLogins'))
  CREATE INDEX IX_UserLogins_IpAddress_AttemptedAtUtc ON dbo.UserLogins(IpAddress, AttemptedAtUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UserLogins_Succeeded_AttemptedAtUtc' AND object_id = OBJECT_ID('dbo.UserLogins'))
  CREATE INDEX IX_UserLogins_Succeeded_AttemptedAtUtc ON dbo.UserLogins(Succeeded, AttemptedAtUtc DESC);

IF OBJECT_ID('dbo.EmailTemplate','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailTemplate(
    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    EmailTemplateId AS (CONVERT(int, [Id])) PERSISTED,
    [Key] nvarchar(100) NOT NULL,
    [Name] nvarchar(200) NOT NULL,
    FromName nvarchar(200) NULL,
    FromEmail nvarchar(320) NOT NULL,
    SubjectTemplate nvarchar(255) NOT NULL,
    ViewPath nvarchar(260) NOT NULL CONSTRAINT DF_EmailTemplate_ViewPath DEFAULT(N'PasswordReset.cshtml'),
    BodyHtmlTemplate nvarchar(max) NOT NULL,
    IsSensitive bit NOT NULL CONSTRAINT DF_EmailTemplate_IsSensitive DEFAULT(0),
    IsEnabled bit NOT NULL CONSTRAINT DF_EmailTemplate_IsEnabled DEFAULT(1),
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.EmailTemplate', 'Key') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD [Key] nvarchar(100) NOT NULL CONSTRAINT DF_EmailTemplate_Key_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailTemplate', 'Name') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD [Name] nvarchar(200) NOT NULL CONSTRAINT DF_EmailTemplate_Name_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailTemplate', 'FromName') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD FromName nvarchar(200) NULL;
IF COL_LENGTH('dbo.EmailTemplate', 'FromEmail') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD FromEmail nvarchar(320) NOT NULL CONSTRAINT DF_EmailTemplate_FromEmail_Alt DEFAULT(N'noreply@example.local');
IF COL_LENGTH('dbo.EmailTemplate', 'SubjectTemplate') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD SubjectTemplate nvarchar(255) NOT NULL CONSTRAINT DF_EmailTemplate_SubjectTemplate_Alt DEFAULT(N'');
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('dbo.EmailTemplate') AND name='SubjectTemplate' AND max_length = -1)
BEGIN
  UPDATE dbo.EmailTemplate
  SET SubjectTemplate = LEFT(ISNULL(SubjectTemplate, N''), 255)
  WHERE LEN(ISNULL(SubjectTemplate, N'')) > 255;
  ALTER TABLE dbo.EmailTemplate ALTER COLUMN SubjectTemplate nvarchar(255) NOT NULL;
END;
IF COL_LENGTH('dbo.EmailTemplate', 'EmailTemplateId') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD EmailTemplateId AS (CONVERT(int, [Id])) PERSISTED;
IF COL_LENGTH('dbo.EmailTemplate', 'ViewPath') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD ViewPath nvarchar(260) NOT NULL CONSTRAINT DF_EmailTemplate_ViewPath_Alt DEFAULT(N'PasswordReset.cshtml');
IF COL_LENGTH('dbo.EmailTemplate', 'BodyHtmlTemplate') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD BodyHtmlTemplate nvarchar(max) NOT NULL CONSTRAINT DF_EmailTemplate_BodyHtmlTemplate_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailTemplate', 'IsSensitive') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD IsSensitive bit NOT NULL CONSTRAINT DF_EmailTemplate_IsSensitive_Alt DEFAULT(0);
IF COL_LENGTH('dbo.EmailTemplate', 'IsEnabled') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD IsEnabled bit NOT NULL CONSTRAINT DF_EmailTemplate_IsEnabled_Alt DEFAULT(1);
IF COL_LENGTH('dbo.EmailTemplate', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.EmailTemplate', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.EmailTemplate ADD UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailTemplate_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_EmailTemplate_Key' AND object_id=OBJECT_ID('dbo.EmailTemplate'))
  CREATE UNIQUE INDEX UX_EmailTemplate_Key ON dbo.EmailTemplate([Key]);
IF COL_LENGTH('dbo.EmailTemplate', 'EmailTemplateId') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_EmailTemplate_EmailTemplateId' AND object_id=OBJECT_ID('dbo.EmailTemplate'))
  EXEC(N'CREATE UNIQUE INDEX UX_EmailTemplate_EmailTemplateId ON dbo.EmailTemplate(EmailTemplateId);');
IF COL_LENGTH('dbo.EmailTemplate', 'ViewPath') IS NOT NULL
  EXEC(N'
UPDATE dbo.EmailTemplate
SET ViewPath = CASE
                 WHEN [Key] = N''PasswordReset'' THEN N''PasswordReset.cshtml''
                 WHEN LEN(LTRIM(RTRIM(ISNULL(ViewPath, N'''')))) = 0 THEN CONCAT([Key], N''.cshtml'')
                 ELSE ViewPath
               END
WHERE ViewPath IS NULL
   OR LEN(LTRIM(RTRIM(ViewPath))) = 0;');

IF OBJECT_ID('dbo.EmailSettings','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailSettings(
    EmailSettingsId int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    FromEmail nvarchar(320) NOT NULL,
    FromName nvarchar(200) NOT NULL,
    GlobalSignatureHtml nvarchar(max) NOT NULL,
    WrapperViewPath nvarchar(260) NOT NULL,
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailSettings_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.EmailSettings', 'FromEmail') IS NULL
  ALTER TABLE dbo.EmailSettings ADD FromEmail nvarchar(320) NOT NULL CONSTRAINT DF_EmailSettings_FromEmail_Alt DEFAULT(N'noreply@example.local');
IF COL_LENGTH('dbo.EmailSettings', 'FromName') IS NULL
  ALTER TABLE dbo.EmailSettings ADD FromName nvarchar(200) NOT NULL CONSTRAINT DF_EmailSettings_FromName_Alt DEFAULT(N'Local SEO');
IF COL_LENGTH('dbo.EmailSettings', 'GlobalSignatureHtml') IS NULL
  ALTER TABLE dbo.EmailSettings ADD GlobalSignatureHtml nvarchar(max) NOT NULL CONSTRAINT DF_EmailSettings_GlobalSignatureHtml_Alt DEFAULT(N'<p>Regards,<br/>Local SEO Team</p>');
IF COL_LENGTH('dbo.EmailSettings', 'WrapperViewPath') IS NULL
  ALTER TABLE dbo.EmailSettings ADD WrapperViewPath nvarchar(260) NOT NULL CONSTRAINT DF_EmailSettings_WrapperViewPath_Alt DEFAULT(N'_EmailWrapper.cshtml');
IF COL_LENGTH('dbo.EmailSettings', 'UpdatedUtc') IS NULL
  ALTER TABLE dbo.EmailSettings ADD UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailSettings_UpdatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF NOT EXISTS (SELECT 1 FROM dbo.EmailSettings)
BEGIN
  INSERT INTO dbo.EmailSettings(FromEmail, FromName, GlobalSignatureHtml, WrapperViewPath, UpdatedUtc)
  VALUES(N'noreply@kontrolit.net', N'Local SEO Tool', N'<p>Kind regards,<br/>Local SEO Team</p>', N'_EmailWrapper.cshtml', SYSUTCDATETIME());
END;

IF OBJECT_ID('dbo.EmailLog','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailLog(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailLog_CreatedUtc DEFAULT SYSUTCDATETIME(),
    TemplateKey nvarchar(100) NOT NULL,
    ToEmail nvarchar(320) NOT NULL,
    ToEmailHash binary(32) NOT NULL,
    FromName nvarchar(200) NULL,
    FromEmail nvarchar(320) NOT NULL,
    SubjectRendered nvarchar(max) NOT NULL,
    BodyHtmlRendered nvarchar(max) NOT NULL,
    IsSensitive bit NOT NULL CONSTRAINT DF_EmailLog_IsSensitive DEFAULT(0),
    RedactionApplied bit NOT NULL CONSTRAINT DF_EmailLog_RedactionApplied DEFAULT(0),
    [Status] nvarchar(20) NOT NULL,
    [Error] nvarchar(max) NULL,
    CorrelationId nvarchar(64) NULL,
    SendGridMessageId nvarchar(200) NULL,
    LastProviderEvent nvarchar(50) NULL,
    LastProviderEventUtc datetime2(3) NULL
  );
END;
IF COL_LENGTH('dbo.EmailLog', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.EmailLog ADD CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailLog_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.EmailLog', 'TemplateKey') IS NULL
  ALTER TABLE dbo.EmailLog ADD TemplateKey nvarchar(100) NOT NULL CONSTRAINT DF_EmailLog_TemplateKey_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailLog', 'ToEmail') IS NULL
  ALTER TABLE dbo.EmailLog ADD ToEmail nvarchar(320) NOT NULL CONSTRAINT DF_EmailLog_ToEmail_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailLog', 'ToEmailHash') IS NULL
  ALTER TABLE dbo.EmailLog ADD ToEmailHash binary(32) NOT NULL CONSTRAINT DF_EmailLog_ToEmailHash_Alt DEFAULT(0x0000000000000000000000000000000000000000000000000000000000000000);
IF COL_LENGTH('dbo.EmailLog', 'FromName') IS NULL
  ALTER TABLE dbo.EmailLog ADD FromName nvarchar(200) NULL;
IF COL_LENGTH('dbo.EmailLog', 'FromEmail') IS NULL
  ALTER TABLE dbo.EmailLog ADD FromEmail nvarchar(320) NOT NULL CONSTRAINT DF_EmailLog_FromEmail_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailLog', 'SubjectRendered') IS NULL
  ALTER TABLE dbo.EmailLog ADD SubjectRendered nvarchar(max) NOT NULL CONSTRAINT DF_EmailLog_SubjectRendered_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailLog', 'BodyHtmlRendered') IS NULL
  ALTER TABLE dbo.EmailLog ADD BodyHtmlRendered nvarchar(max) NOT NULL CONSTRAINT DF_EmailLog_BodyHtmlRendered_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailLog', 'IsSensitive') IS NULL
  ALTER TABLE dbo.EmailLog ADD IsSensitive bit NOT NULL CONSTRAINT DF_EmailLog_IsSensitive_Alt DEFAULT(0);
IF COL_LENGTH('dbo.EmailLog', 'RedactionApplied') IS NULL
  ALTER TABLE dbo.EmailLog ADD RedactionApplied bit NOT NULL CONSTRAINT DF_EmailLog_RedactionApplied_Alt DEFAULT(0);
IF COL_LENGTH('dbo.EmailLog', 'Status') IS NULL
  ALTER TABLE dbo.EmailLog ADD [Status] nvarchar(20) NOT NULL CONSTRAINT DF_EmailLog_Status_Alt DEFAULT(N'Queued');
IF COL_LENGTH('dbo.EmailLog', 'Error') IS NULL
  ALTER TABLE dbo.EmailLog ADD [Error] nvarchar(max) NULL;
IF COL_LENGTH('dbo.EmailLog', 'CorrelationId') IS NULL
  ALTER TABLE dbo.EmailLog ADD CorrelationId nvarchar(64) NULL;
IF COL_LENGTH('dbo.EmailLog', 'SendGridMessageId') IS NULL
  ALTER TABLE dbo.EmailLog ADD SendGridMessageId nvarchar(200) NULL;
IF COL_LENGTH('dbo.EmailLog', 'LastProviderEvent') IS NULL
  ALTER TABLE dbo.EmailLog ADD LastProviderEvent nvarchar(50) NULL;
IF COL_LENGTH('dbo.EmailLog', 'LastProviderEventUtc') IS NULL
  ALTER TABLE dbo.EmailLog ADD LastProviderEventUtc datetime2(3) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_EmailLog_CreatedUtc' AND object_id=OBJECT_ID('dbo.EmailLog'))
  CREATE INDEX IX_EmailLog_CreatedUtc ON dbo.EmailLog(CreatedUtc DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_EmailLog_ToEmailHash' AND object_id=OBJECT_ID('dbo.EmailLog'))
  CREATE INDEX IX_EmailLog_ToEmailHash ON dbo.EmailLog(ToEmailHash);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_EmailLog_TemplateKey' AND object_id=OBJECT_ID('dbo.EmailLog'))
  CREATE INDEX IX_EmailLog_TemplateKey ON dbo.EmailLog(TemplateKey);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_EmailLog_SendGridMessageId' AND object_id=OBJECT_ID('dbo.EmailLog'))
  CREATE INDEX IX_EmailLog_SendGridMessageId ON dbo.EmailLog(SendGridMessageId) WHERE SendGridMessageId IS NOT NULL;

IF OBJECT_ID('dbo.EmailProviderEvent','U') IS NULL
BEGIN
  CREATE TABLE dbo.EmailProviderEvent(
    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    EmailLogId bigint NULL,
    Provider nvarchar(50) NOT NULL CONSTRAINT DF_EmailProviderEvent_Provider DEFAULT(N'SendGrid'),
    EventType nvarchar(50) NOT NULL,
    EventUtc datetime2(3) NOT NULL,
    ProviderMessageId nvarchar(200) NOT NULL,
    PayloadJson nvarchar(max) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailProviderEvent_CreatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.EmailProviderEvent', 'EmailLogId') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD EmailLogId bigint NULL;
IF COL_LENGTH('dbo.EmailProviderEvent', 'Provider') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD Provider nvarchar(50) NOT NULL CONSTRAINT DF_EmailProviderEvent_Provider_Alt DEFAULT(N'SendGrid');
IF COL_LENGTH('dbo.EmailProviderEvent', 'EventType') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD EventType nvarchar(50) NOT NULL CONSTRAINT DF_EmailProviderEvent_EventType_Alt DEFAULT(N'unknown');
IF COL_LENGTH('dbo.EmailProviderEvent', 'EventUtc') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD EventUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailProviderEvent_EventUtc_Alt DEFAULT SYSUTCDATETIME();
IF COL_LENGTH('dbo.EmailProviderEvent', 'ProviderMessageId') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD ProviderMessageId nvarchar(200) NOT NULL CONSTRAINT DF_EmailProviderEvent_ProviderMessageId_Alt DEFAULT(N'');
IF COL_LENGTH('dbo.EmailProviderEvent', 'PayloadJson') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD PayloadJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.EmailProviderEvent', 'CreatedUtc') IS NULL
  ALTER TABLE dbo.EmailProviderEvent ADD CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_EmailProviderEvent_CreatedUtc_Alt DEFAULT SYSUTCDATETIME();
IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_key_columns fkc
  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
  WHERE fkc.parent_object_id = OBJECT_ID('dbo.EmailProviderEvent')
    AND fkc.referenced_object_id = OBJECT_ID('dbo.EmailLog')
    AND pc.name = 'EmailLogId'
    AND rc.name = 'Id'
)
  ALTER TABLE dbo.EmailProviderEvent WITH CHECK ADD CONSTRAINT FK_EmailProviderEvent_EmailLog FOREIGN KEY (EmailLogId) REFERENCES dbo.EmailLog(Id) ON DELETE SET NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_EmailProviderEvent_Idempotency' AND object_id=OBJECT_ID('dbo.EmailProviderEvent'))
  CREATE UNIQUE INDEX UX_EmailProviderEvent_Idempotency ON dbo.EmailProviderEvent(Provider, ProviderMessageId, EventType, EventUtc);

EXEC(N'
MERGE dbo.EmailTemplate AS target
USING (VALUES
  (N''TwoFactorCode'', N''Two Factor Code'', N''Local SEO Tool'', N''noreply@kontrolit.net'', N''Your Local SEO login code'', N''TwoFactorCode.cshtml'', N''<p>Your 2FA login code is <strong>[%Code%]</strong>.</p><p>It expires in [%ExpiryMinutes%] minutes.</p>'', 1, 1),
  (N''PasswordReset'', N''Password Reset'', N''Local SEO Tool'', N''noreply@kontrolit.net'', N''Your Local SEO password reset code'', N''PasswordReset.cshtml'', N''<p>Use code <strong>[%Code%]</strong> to reset your password.</p><p><a href=""[%ResetUrl%]"">Reset password</a></p><p>This code expires in [%ExpiryMinutes%] minutes.</p>'', 1, 1),
  (N''NewUserInvite'', N''New User Invite'', N''Local SEO Tool'', N''noreply@kontrolit.net'', N''You have been invited to Local SEO'', N''NewUserInvite.cshtml'', N''<p>Hi [%RecipientName%],</p><p>You have been invited to Local SEO.</p><p><a href=""[%InviteUrl%]"">Open invite link</a></p><p>This link expires at [%ExpiresAtUtc%].</p>'', 1, 1),
  (N''InviteOtp'', N''Invite OTP'', N''Local SEO Tool'', N''noreply@kontrolit.net'', N''Your Local SEO invite verification code'', N''InviteOtp.cshtml'', N''<p>Your invite verification code is <strong>[%Code%]</strong>.</p><p>It expires at [%ExpiresAtUtc%].</p>'', 1, 1),
  (N''ChangePasswordOtp'', N''Change Password OTP'', N''Local SEO Tool'', N''noreply@kontrolit.net'', N''Your Local SEO change password verification code'', N''ChangePasswordOtp.cshtml'', N''<p>Your change password verification code is <strong>[%Code%]</strong>.</p><p>It expires at [%ExpiresAtUtc%].</p>'', 1, 1)
) AS source([Key], [Name], FromName, FromEmail, SubjectTemplate, ViewPath, BodyHtmlTemplate, IsSensitive, IsEnabled)
ON target.[Key] = source.[Key]
WHEN MATCHED AND (target.ViewPath IS NULL OR LEN(LTRIM(RTRIM(target.ViewPath))) = 0) THEN
  UPDATE SET
    target.ViewPath = source.ViewPath,
    target.UpdatedUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT([Key], [Name], FromName, FromEmail, SubjectTemplate, ViewPath, BodyHtmlTemplate, IsSensitive, IsEnabled, CreatedUtc, UpdatedUtc)
  VALUES(source.[Key], source.[Name], source.FromName, source.FromEmail, source.SubjectTemplate, source.ViewPath, source.BodyHtmlTemplate, source.IsSensitive, source.IsEnabled, SYSUTCDATETIME(), SYSUTCDATETIME());');

IF OBJECT_ID('dbo.LoginCode','U') IS NOT NULL
BEGIN
  INSERT INTO dbo.EmailCodes(
    Purpose,
    Email,
    EmailNormalized,
    CodeHash,
    Salt,
    ExpiresAtUtc,
    CreatedAtUtc,
    FailedAttempts,
    IsUsed)
  SELECT
    1,
    CAST(Email AS varchar(320)),
    LOWER(LTRIM(RTRIM(CAST(Email AS varchar(320))))),
    CodeHash,
    Salt,
    ExpiresAtUtc,
    CreatedAtUtc,
    FailedAttempts,
    IsUsed
  FROM dbo.LoginCode lc
  WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.EmailCodes ec
    WHERE ec.Purpose = 1
      AND ec.EmailNormalized = LOWER(LTRIM(RTRIM(CAST(lc.Email AS varchar(320)))))
      AND ec.CreatedAtUtc = lc.CreatedAtUtc
      AND ec.ExpiresAtUtc = lc.ExpiresAtUtc
  );
  DROP TABLE dbo.LoginCode;
END;
IF OBJECT_ID('dbo.LoginThrottle','U') IS NOT NULL
  DROP TABLE dbo.LoginThrottle;";
        await conn.ExecuteAsync(new CommandDefinition(sql, cancellationToken: ct));
        logger.LogInformation("Schema bootstrap completed.");
    }
}
