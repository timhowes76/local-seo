IF OBJECT_ID('dbo.CloudflareWorker', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.CloudflareWorker(
    CloudflareWorkerId int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name] nvarchar(200) NOT NULL,
    WorkerKey nvarchar(200) NOT NULL,
    BaseUrl nvarchar(1000) NOT NULL CONSTRAINT DF_CloudflareWorker_BaseUrl DEFAULT(N''),
    RoutePath nvarchar(500) NOT NULL,
    AuthHeaderName nvarchar(200) NULL,
    AuthToken nvarchar(1000) NULL,
    TimeoutSeconds int NOT NULL CONSTRAINT DF_CloudflareWorker_TimeoutSeconds DEFAULT(30),
    IsEnabled bit NOT NULL CONSTRAINT DF_CloudflareWorker_IsEnabled DEFAULT(1),
    DisplayOrder int NOT NULL CONSTRAINT DF_CloudflareWorker_DisplayOrder DEFAULT(0),
    Notes nvarchar(2000) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_CloudflareWorker_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_CloudflareWorker_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_CloudflareWorker_WorkerKey' AND object_id=OBJECT_ID('dbo.CloudflareWorker'))
  CREATE UNIQUE INDEX UX_CloudflareWorker_WorkerKey ON dbo.CloudflareWorker(WorkerKey);
GO

IF OBJECT_ID('dbo.PlaceWebsite', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceWebsite(
    PlaceWebsiteId int IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceId nvarchar(128) NOT NULL,
    WebsiteUrl nvarchar(1000) NOT NULL,
    NormalizedWebsiteUrl nvarchar(1000) NULL,
    HostName nvarchar(255) NULL,
    IsHttps bit NULL,
    SourceType nvarchar(50) NULL,
    [Status] nvarchar(50) NULL,
    FirstDiscoveredUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceWebsite_FirstDiscoveredUtc DEFAULT SYSUTCDATETIME(),
    LastCheckedUtc datetime2(3) NULL,
    LastSuccessfulFetchUtc datetime2(3) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceWebsite_CreatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceWebsite_UpdatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_keys
  WHERE name='FK_PlaceWebsite_Place'
    AND parent_object_id = OBJECT_ID('dbo.PlaceWebsite')
)
  ALTER TABLE dbo.PlaceWebsite WITH CHECK ADD CONSTRAINT FK_PlaceWebsite_Place FOREIGN KEY(PlaceId) REFERENCES dbo.Place(PlaceId);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceWebsite_PlaceId' AND object_id=OBJECT_ID('dbo.PlaceWebsite'))
  CREATE UNIQUE INDEX UX_PlaceWebsite_PlaceId ON dbo.PlaceWebsite(PlaceId);
GO

IF OBJECT_ID('dbo.PlaceWebsiteFetch', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceWebsiteFetch(
    PlaceWebsiteFetchId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceWebsiteId int NOT NULL,
    FetchStartedUtc datetime2(3) NOT NULL,
    FetchCompletedUtc datetime2(3) NULL,
    Success bit NOT NULL,
    RequestedUrl nvarchar(1000) NOT NULL,
    FinalUrl nvarchar(1000) NULL,
    HttpStatusCode int NULL,
    ErrorCode nvarchar(100) NULL,
    ErrorMessage nvarchar(2000) NULL,
    ContentType nvarchar(200) NULL,
    ResponseSizeBytes int NULL,
    RedirectCount int NULL,
    UsedWorker bit NOT NULL CONSTRAINT DF_PlaceWebsiteFetch_UsedWorker DEFAULT(1),
    WorkerKey nvarchar(200) NULL,
    HtmlHash char(64) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceWebsiteFetch_CreatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_keys
  WHERE name='FK_PlaceWebsiteFetch_PlaceWebsite'
    AND parent_object_id = OBJECT_ID('dbo.PlaceWebsiteFetch')
)
  ALTER TABLE dbo.PlaceWebsiteFetch WITH CHECK ADD CONSTRAINT FK_PlaceWebsiteFetch_PlaceWebsite FOREIGN KEY(PlaceWebsiteId) REFERENCES dbo.PlaceWebsite(PlaceWebsiteId);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceWebsiteFetch_PlaceWebsiteId_FetchStartedUtc' AND object_id=OBJECT_ID('dbo.PlaceWebsiteFetch'))
  CREATE INDEX IX_PlaceWebsiteFetch_PlaceWebsiteId_FetchStartedUtc ON dbo.PlaceWebsiteFetch(PlaceWebsiteId, FetchStartedUtc DESC);
GO

IF OBJECT_ID('dbo.PlaceWebsiteHomepageAudit', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.PlaceWebsiteHomepageAudit(
    PlaceWebsiteHomepageAuditId bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PlaceWebsiteFetchId bigint NOT NULL,
    TitleTag nvarchar(1000) NULL,
    TitleTagLength int NULL,
    MetaDescription nvarchar(2000) NULL,
    MetaDescriptionLength int NULL,
    CanonicalUrl nvarchar(1000) NULL,
    RobotsMeta nvarchar(500) NULL,
    HtmlLang nvarchar(50) NULL,
    H1Text nvarchar(1000) NULL,
    H1Count int NULL,
    H2Count int NULL,
    H3Count int NULL,
    H2TextsJson nvarchar(max) NULL,
    H3TextsJson nvarchar(max) NULL,
    VisibleWordCount int NULL,
    ParagraphCount int NULL,
    BulletListCount int NULL,
    ContentSectionCount int NULL,
    HasPhoneNumber bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasPhoneNumber DEFAULT(0),
    PhoneNumbersJson nvarchar(max) NULL,
    HasPostalAddress bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasPostalAddress DEFAULT(0),
    PostalAddressesJson nvarchar(max) NULL,
    HasPostcode bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasPostcode DEFAULT(0),
    PostcodesJson nvarchar(max) NULL,
    HasCityName bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasCityName DEFAULT(0),
    CityNamesJson nvarchar(max) NULL,
    HasBusinessName bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasBusinessName DEFAULT(0),
    BusinessNamesJson nvarchar(max) NULL,
    SchemaTypesJson nvarchar(max) NULL,
    HasLocalBusinessSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasLocalBusinessSchema DEFAULT(0),
    HasOrganizationSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasOrganizationSchema DEFAULT(0),
    HasProductSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasProductSchema DEFAULT(0),
    HasFaqSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasFaqSchema DEFAULT(0),
    HasBreadcrumbSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasBreadcrumbSchema DEFAULT(0),
    HasNapInSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasNapInSchema DEFAULT(0),
    HasGeoCoordinatesInSchema bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasGeoCoordinatesInSchema DEFAULT(0),
    PageScheme nvarchar(10) NULL,
    CanonicalScheme nvarchar(10) NULL,
    RedirectsToHttps bit NULL,
    HasMixedContent bit NULL,
    InternalLinkCount int NULL,
    ServicePageLinkCount int NULL,
    InternalAnchorTextsJson nvarchar(max) NULL,
    ImageCount int NULL,
    ImagesMissingAltCount int NULL,
    ImageAltTextsJson nvarchar(max) NULL,
    ImageFileNamesJson nvarchar(max) NULL,
    DetectedCms nvarchar(100) NULL,
    GeneratorMetaTag nvarchar(255) NULL,
    HasViewportMeta bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasViewportMeta DEFAULT(0),
    HasResponsiveIndicators bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasResponsiveIndicators DEFAULT(0),
    HasFavicon bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasFavicon DEFAULT(0),
    HasCookieBanner bit NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_HasCookieBanner DEFAULT(0),
    ServiceKeywordsJson nvarchar(max) NULL,
    LocationKeywordsJson nvarchar(max) NULL,
    ServiceTownCombinationsJson nvarchar(max) NULL,
    BrandNamesJson nvarchar(max) NULL,
    CreatedUtc datetime2(3) NOT NULL CONSTRAINT DF_PlaceWebsiteHomepageAudit_CreatedUtc DEFAULT SYSUTCDATETIME()
  );
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.foreign_keys
  WHERE name='FK_PlaceWebsiteHomepageAudit_PlaceWebsiteFetch'
    AND parent_object_id = OBJECT_ID('dbo.PlaceWebsiteHomepageAudit')
)
  ALTER TABLE dbo.PlaceWebsiteHomepageAudit WITH CHECK ADD CONSTRAINT FK_PlaceWebsiteHomepageAudit_PlaceWebsiteFetch FOREIGN KEY(PlaceWebsiteFetchId) REFERENCES dbo.PlaceWebsiteFetch(PlaceWebsiteFetchId);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceWebsiteHomepageAudit_PlaceWebsiteFetchId' AND object_id=OBJECT_ID('dbo.PlaceWebsiteHomepageAudit'))
  CREATE UNIQUE INDEX UX_PlaceWebsiteHomepageAudit_PlaceWebsiteFetchId ON dbo.PlaceWebsiteHomepageAudit(PlaceWebsiteFetchId);
GO

MERGE dbo.CloudflareWorker AS target
USING (VALUES
  (N'Sales Local SEO - Home Page Fetch', N'SalesLocalSeoHomePageFetch', N'', N'/sales-local-seo-homepage-fetch', N'X-Worker-Token', N'', 30, 1, 10, NULL)
) AS source([Name], WorkerKey, BaseUrl, RoutePath, AuthHeaderName, AuthToken, TimeoutSeconds, IsEnabled, DisplayOrder, Notes)
ON target.WorkerKey = source.WorkerKey
WHEN NOT MATCHED THEN
  INSERT([Name], WorkerKey, BaseUrl, RoutePath, AuthHeaderName, AuthToken, TimeoutSeconds, IsEnabled, DisplayOrder, Notes, CreatedUtc, UpdatedUtc)
  VALUES(source.[Name], source.WorkerKey, source.BaseUrl, source.RoutePath, source.AuthHeaderName, source.AuthToken, source.TimeoutSeconds, source.IsEnabled, source.DisplayOrder, source.Notes, SYSUTCDATETIME(), SYSUTCDATETIME());
GO
