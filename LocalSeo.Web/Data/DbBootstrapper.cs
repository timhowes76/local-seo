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
    SearchLocationName nvarchar(200) NULL,
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
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='UX_PlaceReview_Place_Review' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE UNIQUE INDEX UX_PlaceReview_Place_Review ON dbo.PlaceReview(PlaceId, ReviewId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PlaceReview_Place_LastSeen' AND object_id=OBJECT_ID('dbo.PlaceReview'))
  CREATE INDEX IX_PlaceReview_Place_LastSeen ON dbo.PlaceReview(PlaceId, LastSeenUtc DESC);
IF OBJECT_ID('dbo.DataForSeoReviewTask','U') IS NULL
BEGIN
  CREATE TABLE dbo.DataForSeoReviewTask(
    DataForSeoReviewTaskId bigint IDENTITY(1,1) PRIMARY KEY,
    DataForSeoTaskId nvarchar(64) NOT NULL,
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
