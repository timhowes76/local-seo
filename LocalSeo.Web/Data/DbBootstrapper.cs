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
    RanAtUtc datetime2(0) NOT NULL CONSTRAINT DF_SearchRun_RanAtUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF OBJECT_ID('dbo.Place','U') IS NULL
BEGIN
  CREATE TABLE dbo.Place(
    PlaceId nvarchar(128) NOT NULL PRIMARY KEY,
    DisplayName nvarchar(300) NULL,
    PrimaryType nvarchar(80) NULL,
    PrimaryCategory nvarchar(200) NULL,
    TypesCsv nvarchar(1500) NULL,
    FormattedAddress nvarchar(500) NULL,
    Lat decimal(9,6) NULL,
    Lng decimal(9,6) NULL,
    NationalPhoneNumber nvarchar(50) NULL,
    WebsiteUri nvarchar(500) NULL,
    Description nvarchar(2000) NULL,
    PhotoCount int NULL,
    IsServiceAreaBusiness bit NULL,
    BusinessStatus nvarchar(50) NULL,
    RegularOpeningHoursJson nvarchar(max) NULL,
    OpeningDate datetime2(0) NULL,
    SocialProfilesJson nvarchar(max) NULL,
    ServiceAreasJson nvarchar(max) NULL,
    LastSeenUtc datetime2(0) NOT NULL CONSTRAINT DF_Place_LastSeenUtc DEFAULT SYSUTCDATETIME()
  );
END;
IF COL_LENGTH('dbo.Place', 'PrimaryCategory') IS NULL
  ALTER TABLE dbo.Place ADD PrimaryCategory nvarchar(200) NULL;
IF COL_LENGTH('dbo.Place', 'NationalPhoneNumber') IS NULL
  ALTER TABLE dbo.Place ADD NationalPhoneNumber nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'WebsiteUri') IS NULL
  ALTER TABLE dbo.Place ADD WebsiteUri nvarchar(500) NULL;
IF COL_LENGTH('dbo.Place', 'Description') IS NULL
  ALTER TABLE dbo.Place ADD Description nvarchar(2000) NULL;
IF COL_LENGTH('dbo.Place', 'PhotoCount') IS NULL
  ALTER TABLE dbo.Place ADD PhotoCount int NULL;
IF COL_LENGTH('dbo.Place', 'IsServiceAreaBusiness') IS NULL
  ALTER TABLE dbo.Place ADD IsServiceAreaBusiness bit NULL;
IF COL_LENGTH('dbo.Place', 'BusinessStatus') IS NULL
  ALTER TABLE dbo.Place ADD BusinessStatus nvarchar(50) NULL;
IF COL_LENGTH('dbo.Place', 'RegularOpeningHoursJson') IS NULL
  ALTER TABLE dbo.Place ADD RegularOpeningHoursJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'OpeningDate') IS NULL
  ALTER TABLE dbo.Place ADD OpeningDate datetime2(0) NULL;
IF COL_LENGTH('dbo.Place', 'SocialProfilesJson') IS NULL
  ALTER TABLE dbo.Place ADD SocialProfilesJson nvarchar(max) NULL;
IF COL_LENGTH('dbo.Place', 'ServiceAreasJson') IS NULL
  ALTER TABLE dbo.Place ADD ServiceAreasJson nvarchar(max) NULL;
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
IF OBJECT_ID('dbo.LoginCode','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginCode(
    LoginCodeId bigint IDENTITY(1,1) PRIMARY KEY,
    Email nvarchar(320) NOT NULL,
    CodeHash varbinary(64) NOT NULL,
    Salt varbinary(32) NOT NULL,
    ExpiresAtUtc datetime2(0) NOT NULL,
    CreatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_LoginCode_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    FailedAttempts int NOT NULL CONSTRAINT DF_LoginCode_FailedAttempts DEFAULT(0),
    IsUsed bit NOT NULL CONSTRAINT DF_LoginCode_IsUsed DEFAULT(0)
  );
END;
IF OBJECT_ID('dbo.LoginThrottle','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoginThrottle(
    Email nvarchar(320) NOT NULL PRIMARY KEY,
    WindowStartUtc datetime2(0) NOT NULL,
    SendCount int NOT NULL
  );
END;";
        await conn.ExecuteAsync(new CommandDefinition(sql, cancellationToken: ct));
        logger.LogInformation("Schema bootstrap completed.");
    }
}
