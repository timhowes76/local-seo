IF OBJECT_ID('dbo.PlaceWebsiteHomepageAudit', 'U') IS NOT NULL
  DROP TABLE dbo.PlaceWebsiteHomepageAudit;
GO

IF OBJECT_ID('dbo.PlaceWebsiteFetch', 'U') IS NOT NULL
  DROP TABLE dbo.PlaceWebsiteFetch;
GO

IF OBJECT_ID('dbo.PlaceWebsite', 'U') IS NOT NULL
  DROP TABLE dbo.PlaceWebsite;
GO

IF OBJECT_ID('dbo.CloudflareWorker', 'U') IS NOT NULL
  DROP TABLE dbo.CloudflareWorker;
GO
